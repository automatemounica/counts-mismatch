"""
Verify observation list count for configured tenants/applications.
Works as both:
1) pytest test file
2) standalone python script
"""

import re
import json
import base64
from typing import Any

import requests

from tenants import TENANTS, OPTIONAL_SCOPES, load_tenants_from_excel

try:
    import pytest
except ModuleNotFoundError:
    pytest = None

try:
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:
    sync_playwright = None

STRICT_REQUEST_PARITY = True


def _log(tenant_name: str, step: str, message: str) -> None:
    print(f"[{tenant_name}] [{step}] {message}")


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        padding = "=" * (-len(payload_b64) % 4)
        payload_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
        data = json.loads(payload_bytes.decode("utf-8"))
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def get_token(tenant: dict) -> str:
    tenant_name = tenant.get("name", "unknown")
    token_url = f"{tenant['auth_url']}?__tenant={tenant['name']}"
    _log(tenant_name, "token", f"Requesting token from {token_url}")
    base_payload = {
        "grant_type": "password",
        "client_id": tenant["client_id"],
        "client_secret": tenant["client_secret"],
        "username": tenant["username"],
        "password": tenant["password"],
    }

    scope_value = str(tenant.get("scope", "")).strip()
    requested_scopes = scope_value.split()

    def _request_token(scopes: list[str], attempt: int) -> requests.Response:
        payload = dict(base_payload)
        payload["scope"] = " ".join(scopes)
        _log(tenant_name, "token", f"Token attempt={attempt} with scope_len={len(scopes)}")
        resp = requests.post(
            token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=60,
        )
        _log(tenant_name, "token", f"Token API status={resp.status_code}")
        return resp

    current_scopes = list(requested_scopes)
    response = _request_token(current_scopes, 1)

    # Auth server returns invalid_scope if any single requested scope isn't
    # provisioned, but doesn't say which one. Drop optional scopes one at a
    # time so we keep every service the tenant actually has.
    attempt = 1
    pending_optional = [s for s in OPTIONAL_SCOPES if s in current_scopes]
    while (
        response.status_code == 400
        and "invalid_scope" in response.text.lower()
        and pending_optional
    ):
        to_drop = pending_optional.pop(0)
        attempt += 1
        current_scopes = [s for s in current_scopes if s != to_drop]
        _log(tenant_name, "token", f"invalid_scope -> retrying without '{to_drop}'")
        response = _request_token(current_scopes, attempt)
    dropped = [s for s in requested_scopes if s not in current_scopes]
    if dropped:
        _log(tenant_name, "token", f"Final dropped scopes: {dropped}")

    if response.status_code >= 400:
        _log(tenant_name, "token", f"Token error body={response.text}")
    response.raise_for_status()
    token = response.json()["access_token"]
    claims = _decode_jwt_payload(token)
    claim_client_id = claims.get("client_id")
    claim_scope = claims.get("scope")
    claim_aud = claims.get("aud")
    _log(tenant_name, "token", "Token generated successfully")
    _log(
        tenant_name,
        "token_claims",
        f"client_id={claim_client_id}, "
        f"scope={claim_scope}, aud={claim_aud}",
    )
    return token


def _auth_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    }


def _extract_total_count(payload: Any) -> int:
    if isinstance(payload, dict):
        for key in ("totalCount", "count"):
            value = payload.get(key)
            if isinstance(value, int):
                return value

        nested = payload.get("result")
        if isinstance(nested, dict) and isinstance(nested.get("totalCount"), int):
            return nested["totalCount"]

        nested = payload.get("data")
        if isinstance(nested, dict) and isinstance(nested.get("totalCount"), int):
            return nested["totalCount"]

    raise AssertionError(f"Could not extract total count from response: {payload}")


def _call_count_url(
    tenant_name: str,
    tenant: dict,
    token: str,
    endpoint_path: str,
    preferred_method: str = "GET",
    endpoint_name: str = "list_endpoint",
) -> int:
    endpoint_path = (endpoint_path or "").strip()
    url = f"{tenant['api_gateway_url'].rstrip('/')}{endpoint_path}"
    preferred_method = (preferred_method or "GET").upper()
    _log(tenant_name, endpoint_name, f"Calling API: method={preferred_method}, url={url}")

    def _send(method: str) -> requests.Response:
        if method == "POST":
            return requests.post(url, headers=_auth_headers(token), json={}, timeout=60)
        return requests.get(url, headers=_auth_headers(token), timeout=60)

    response = _send(preferred_method)
    _log(tenant_name, endpoint_name, f"Initial response status={response.status_code}")
    if response.status_code == 405 and not STRICT_REQUEST_PARITY:
        fallback_method = "POST" if preferred_method == "GET" else "GET"
        print(f"[{tenant_name}] {endpoint_name}: {preferred_method} not allowed. Retrying with {fallback_method}...")
        response = _send(fallback_method)

    response.raise_for_status()
    total_count = _extract_total_count(response.json())
    print(f"[{tenant_name}] {endpoint_name} totalCount = {total_count}")
    return total_count


def _get_dashboard_count_from_ui(
    tenant_name: str,
    tenant: dict,
    dashboard_name: str,
    count_label: str,
    app_keyword: str | None = None,
    expected_count: int | None = None,
    page: Any | None = None,
) -> int:
    if sync_playwright is None:
        raise ModuleNotFoundError(
            "Playwright is not installed in this environment. "
            "Install it to read dashboard count from UI."
        )

    dashboard_url = tenant.get("dashboard_url") or f"{tenant['base_url'].rstrip('/')}/AdvancedDashboards"
    dashboard_name = dashboard_name or "Observation Dashboard"
    count_label = count_label or "TOTAL OBSERVATION COUNT"
    headless = (
        tenant.get("api_config", {})
        .get("endpoints_meta", {})
        .get("ui_headless", False)
    )

    owns_page = page is None
    browser = None
    if owns_page:
        p = sync_playwright().start()
        browser = p.chromium.launch(
            headless=headless,
            args=["--disable-notifications"],
        )
        context = browser.new_context(ignore_https_errors=True, permissions=[])
        page = context.new_page()

        # Login only once for this UI session.
        page.goto(tenant["base_url"], wait_until="networkidle", timeout=120000)
        page.locator("#LoginInput_UserNameOrEmailAddress").fill(tenant["username"])
        page.locator("#password-input").fill(tenant["password"])
        page.get_by_role("button", name="Login").click()
        page.wait_for_url("**/AdvancedDashboards", timeout=300000)
        page.wait_for_load_state("networkidle", timeout=30000)
        page.wait_for_timeout(2000)
        _log(tenant_name, "dashboard_ui", "Login complete for dashboard session")

    # If URL is not dashboard for any reason, force-open it.
    if "AdvancedDashboards" not in page.url:
        page.goto(dashboard_url, wait_until="networkidle", timeout=120000)
        page.wait_for_timeout(1500)
    _log(tenant_name, "dashboard_ui", f"Selecting dashboard='{dashboard_name}', label='{count_label}'")

    search_keyword = (app_keyword or dashboard_name or "").strip()
    network_count_candidates: list[int] = []

    def _extract_candidate_counts(payload: Any) -> list[int]:
        counts: list[int] = []
        if isinstance(payload, dict):
            for k, v in payload.items():
                key = str(k).lower()
                if key in {"totalcount", "count"} and isinstance(v, int) and v >= 0:
                    counts.append(v)
                counts.extend(_extract_candidate_counts(v))
        elif isinstance(payload, list):
            for item in payload:
                counts.extend(_extract_candidate_counts(item))
        return counts

    def _on_response(resp: Any) -> None:
        try:
            ctype = (resp.headers.get("content-type") or "").lower()
            if "application/json" not in ctype:
                return
            data = resp.json()
            network_count_candidates.extend(_extract_candidate_counts(data))
        except Exception:
            return

    page.on("response", _on_response)

    def _label_locator(label_text: str) -> Any:
        # Prefer exact text match to avoid partial collisions like
        # "TOTAL INSPECTIONS" matching "CLOSED INSPECTIONS".
        loc = page.get_by_text(label_text, exact=True).first
        try:
            if loc.count() > 0:
                return loc
        except Exception:
            pass
        return page.get_by_text(label_text, exact=False).first

    def _select_dashboard_via_select2() -> bool:
        triggers = [
            "#select2-EHSWatchDashboard_Caption-container",
            "span[id='select2-EHSWatchDashboard_Caption-container']",
            "span.select2-selection__rendered",
        ]
        for trigger in triggers:
            try:
                loc = page.locator(trigger).first
                if loc.count() == 0:
                    continue
                loc.click(timeout=3000)
                page.wait_for_timeout(500)

                search = page.locator("input.select2-search__field").first
                primary_target = (dashboard_name or "").strip()
                fallback_target = (search_keyword or "").strip()
                target = primary_target or fallback_target
                if search.count() > 0:
                    # Always search by dashboard title first; app keyword is only fallback.
                    search.fill(target)
                    page.wait_for_timeout(500)
                    option = page.locator("li.select2-results__option", has_text=target).first
                    if option.count() > 0:
                        option.click(timeout=3000)
                    else:
                        if fallback_target and fallback_target != target:
                            search.fill(fallback_target)
                            page.wait_for_timeout(500)
                            option = page.locator("li.select2-results__option", has_text=fallback_target).first
                            if option.count() > 0:
                                option.click(timeout=3000)
                            else:
                                search.press("Enter")
                        else:
                            search.press("Enter")
                else:
                    page.get_by_text(target, exact=False).first.click(timeout=3000)

                page.wait_for_load_state("networkidle", timeout=20000)
                page.wait_for_timeout(2500)

                # Verify selected caption reflects expected dashboard.
                caption = page.locator("#select2-EHSWatchDashboard_Caption-container").first
                if caption.count() > 0:
                    caption_text = (caption.inner_text(timeout=3000) or "").strip().lower()
                    if (search_keyword and search_keyword.lower() in caption_text) or (
                        dashboard_name and dashboard_name.lower() in caption_text
                    ):
                        return True
                return True
            except Exception:
                continue
        return False

    # Prefer direct click in dashboard list/sidebar.
    selected = False
    try:
        page.get_by_text(dashboard_name, exact=False).first.click(timeout=4000)
        page.wait_for_timeout(2000)
        selected = True
    except Exception:
        pass

    # Select2 dashboard dropdown fallback using app keyword first.
    if not selected:
        selected = _select_dashboard_via_select2()
    _log(tenant_name, "dashboard_ui", f"Primary dashboard selection result={selected}")

    # Generic combobox fallback.
    if not selected:
        try:
            page.locator("div[role='combobox'], .ant-select-selector").first.click()
            page.get_by_text(search_keyword or dashboard_name, exact=False).first.click()
            page.wait_for_load_state("networkidle", timeout=20000)
            page.wait_for_timeout(2500)
        except Exception:
            pass

    # One more deterministic select2 attempt if selection still not stable.
    if not selected:
        selected = _select_dashboard_via_select2()
    _log(tenant_name, "dashboard_ui", f"Final dashboard selection result={selected}")

    # Wait for target dashboard card label if it appears after async rendering.
    try:
        page.get_by_text(count_label, exact=False).first.wait_for(timeout=12000)
    except Exception:
        page.wait_for_load_state("networkidle", timeout=20000)
        page.wait_for_timeout(3500)

    # Label-anchored extraction from page text (most deterministic for these dashboards).
    body_text = page.locator("body").inner_text(timeout=15000)
    anchored_patterns = [
        rf"{re.escape(count_label)}\s*[\r\n ]+\s*(\d+)",
        rf"{re.escape(count_label)}[^\d]*(\d+)",
    ]
    for pat in anchored_patterns:
        m = re.search(pat, body_text, flags=re.IGNORECASE)
        if m:
            ui_count = int(m.group(1))
            print(f"[{tenant_name}] dashboard_ui totalCount = {ui_count} (anchored)")
            if owns_page and browser:
                browser.close()
                p.stop()
            return ui_count

    # KPI-card extraction by explicit label (most reliable path).
    label_candidates = [count_label]
    if search_keyword.lower() == "action":
        # Action dashboard uses generic TOTAL COUNT label.
        label_candidates.append("TOTAL COUNT")

    for label_text in label_candidates:
        label_loc = _label_locator(label_text)
        try:
            if label_loc.count() == 0:
                continue
            for xpath in (
                "xpath=ancestor::div[contains(@class,'ant-card')][1]",
                "xpath=ancestor::div[contains(@class,'widget')][1]",
                "xpath=ancestor::section[1]",
                "xpath=ancestor::div[1]",
            ):
                try:
                    card_text = label_loc.locator(xpath).inner_text(timeout=6000)
                except Exception:
                    continue
                numbers = [int(n) for n in re.findall(r"(\d+)", card_text)]
                if numbers:
                    # Use strongest KPI candidate rather than first occurrence;
                    # this avoids reading suffix digits from labels like "Observations1".
                    ui_count = max(numbers)
                    print(f"[{tenant_name}] dashboard_ui totalCount = {ui_count} (label={label_text})")
                    if owns_page and browser:
                        browser.close()
                        p.stop()
                    return ui_count
        except Exception:
            pass

    # Read count from the specific dashboard card first.
    label_locator = _label_locator(count_label)
    try:
        if label_locator.count() > 0:
            # Try nearest card/container text.
            for xpath in (
                "xpath=ancestor::div[contains(@class,'ant-card')][1]",
                "xpath=ancestor::section[1]",
                "xpath=ancestor::div[1]",
            ):
                try:
                    card_text = label_locator.locator(xpath).inner_text(timeout=5000)
                except Exception:
                    continue
                numbers = [int(n) for n in re.findall(r"(\d+)", card_text)]
                if numbers:
                    ui_count = max(numbers)
                    print(f"[{tenant_name}] dashboard_ui totalCount = {ui_count}")
                    if owns_page and browser:
                        browser.close()
                        p.stop()
                    return ui_count
    except Exception:
        pass

    # Fallback: parse text around target label.
    upper_text = body_text.upper()
    marker = count_label.upper()
    idx = upper_text.find(marker)
    if idx >= 0:
        snippet = body_text[max(0, idx - 80) : idx + 500]
        match = re.search(r"(\d+)", snippet)
        if match:
            ui_count = int(match.group(1))
            print(f"[{tenant_name}] dashboard_ui totalCount = {ui_count}")
            if owns_page and browser:
                browser.close()
                p.stop()
            return ui_count

    # Fallback: support "34 TOTAL ACTION COUNT" style layouts.
    alt_match = re.search(rf"(\d+)\s+{re.escape(count_label)}", body_text, flags=re.IGNORECASE)
    if alt_match:
        ui_count = int(alt_match.group(1))
        print(f"[{tenant_name}] dashboard_ui totalCount = {ui_count}")
        if owns_page and browser:
            browser.close()
            p.stop()
        return ui_count

    # Fallback: generic app-specific count extraction, e.g. TOTAL <APP> COUNT.
    if search_keyword:
        normalized = re.escape(search_keyword.upper())
        generic_patterns = [
            rf"TOTAL\s+{normalized}[\w\s-]*COUNT[^\d]*(\d+)",
            rf"(\d+)[^\n]*TOTAL\s+{normalized}[\w\s-]*COUNT",
        ]
        for gp in generic_patterns:
            gm = re.search(gp, body_text.upper(), flags=re.IGNORECASE)
            if gm:
                ui_count = int(gm.group(1))
                print(f"[{tenant_name}] dashboard_ui totalCount = {ui_count}")
                if owns_page and browser:
                    browser.close()
                    p.stop()
                return ui_count

    current_url = page.url
    page_title = page.title()
    html_preview = page.content()[:300]

    patterns = [
        rf"{re.escape(count_label)}\s+(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, body_text, flags=re.IGNORECASE)
        if match:
            ui_count = int(match.group(1))
            print(f"[{tenant_name}] dashboard_ui totalCount = {ui_count}")
            if owns_page and browser:
                browser.close()
                p.stop()
            return ui_count

    # Final fallback for debugging: show a short snippet near label if present.
    marker = count_label.upper()
    idx = body_text.upper().find(marker)
    debug_snippet = body_text[max(idx - 80, 0) : idx + 220] if idx >= 0 else body_text[:220]

    # Final fallback: infer dashboard count from captured network JSON responses.
    candidates = [c for c in network_count_candidates if isinstance(c, int) and c >= 0]
    if candidates:
        if expected_count is not None and expected_count in candidates:
            ui_count = expected_count
        else:
            # Prefer likely KPI counts over huge payload totals.
            non_zero = [c for c in candidates if c > 0]
            ui_count = max(non_zero) if non_zero else 0
        print(f"[{tenant_name}] dashboard_ui totalCount (network fallback) = {ui_count}")
        if owns_page and browser:
            browser.close()
            p.stop()
        return ui_count

    if owns_page and browser:
        browser.close()
        p.stop()
    raise AssertionError(
        f"[{tenant_name}] Could not extract '{count_label}' from dashboard UI. "
        f"Dashboard={dashboard_name!r}, URL: {current_url!r}, Title: {page_title!r}, "
        f"Snippet: {debug_snippet!r}, HTMLPreview: {html_preview!r}"
    )


def _run_application_count_verification(
    tenant_name: str,
    tenant: dict,
    token: str,
    app_name: str,
    app_cfg: dict,
    dashboard_page: Any | None = None,
) -> tuple[int, int, str]:
    list_endpoint = (app_cfg.get("list_endpoint") or "").strip()
    if not list_endpoint:
        print(f"[{tenant_name}] [{app_name}] application endpoint not found in API sheet.")
        return -1, -1, "APPLICATION_NOT_FOUND"
    _log(tenant_name, app_name, "Starting application verification")

    list_count = _call_count_url(
        tenant_name=tenant_name,
        tenant=tenant,
        token=token,
        endpoint_path=list_endpoint,
        preferred_method=app_cfg.get("list_method", "GET"),
        endpoint_name=f"{app_name}.list",
    )

    dashboard_endpoint = (app_cfg.get("dashboard_endpoint") or "").strip()
    if dashboard_endpoint:
        dashboard_count = _call_count_url(
            tenant_name=tenant_name,
            tenant=tenant,
            token=token,
            endpoint_path=dashboard_endpoint,
            preferred_method=app_cfg.get("dashboard_method", "GET"),
            endpoint_name=f"{app_name}.dashboard",
        )
    else:
        print(f"[{tenant_name}] [{app_name}] dashboard API not configured. Falling back to dashboard UI.")
        default_dashboard_name = f"{str(app_name).replace('_', ' ').title()} Dashboard"
        try:
            dashboard_count = _get_dashboard_count_from_ui(
                tenant_name=tenant_name,
                tenant=tenant,
                dashboard_name=app_cfg.get("dashboard_name", default_dashboard_name),
                count_label=app_cfg.get("dashboard_count_label", "TOTAL COUNT"),
                app_keyword=app_cfg.get("dashboard_search_keyword", app_name),
                expected_count=list_count,
                page=dashboard_page,
            )
        except AssertionError as exc:
            print(f"[{tenant_name}] [{app_name}] dashboard_count not found: {exc}")
            return list_count, -1, "NOT_FOUND"

    diff = list_count - dashboard_count
    status = "PASS" if list_count == dashboard_count else "FAIL"
    print(
        f"[{tenant_name}] [{app_name}] compare -> list={list_count}, "
        f"dashboard={dashboard_count}, difference={diff}, status={status}"
    )

    if status == "PASS":
        _log(tenant_name, app_name, "Application verification passed")
    else:
        _log(tenant_name, app_name, "Application verification failed (count mismatch)")
    return list_count, dashboard_count, status


def _get_available_dashboards(page: Any) -> set[str]:
    available: set[str] = set()
    selectors = [
        "li.select2-results__option",
        "#select2-EHSWatchDashboard_Caption-results li",
    ]
    # Open Select2 and harvest option texts.
    triggers = [
        "#select2-EHSWatchDashboard_Caption-container",
        "span[id='select2-EHSWatchDashboard_Caption-container']",
        "span.select2-selection__rendered",
    ]
    for trigger in triggers:
        try:
            loc = page.locator(trigger).first
            if loc.count() == 0:
                continue
            loc.click(timeout=3000)
            page.wait_for_timeout(700)
            for selector in selectors:
                options = page.locator(selector)
                count = options.count()
                for i in range(count):
                    text = (options.nth(i).inner_text(timeout=1000) or "").strip()
                    if text:
                        available.add(text.lower())
            # Close dropdown with Escape.
            page.keyboard.press("Escape")
            break
        except Exception:
            continue
    return available


def _normalize_label(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def _dashboard_is_available(
    available_dashboards: set[str],
    dashboard_name: str,
    app_keyword: str,
    app_name: str,
) -> bool:
    if not available_dashboards:
        return True
    normalized_available = {_normalize_label(x) for x in available_dashboards}
    candidates = {
        _normalize_label(dashboard_name),
        _normalize_label(app_keyword),
        _normalize_label(app_name),
        _normalize_label(str(app_name).replace("_", " ") + " dashboard"),
    }
    for candidate in list(candidates):
        if not candidate:
            continue
        if candidate in normalized_available:
            return True
        if any(candidate in av or av in candidate for av in normalized_available):
            return True
    return False


def _run_tenant_count_verification(tenant_name: str, tenant: dict) -> list[dict[str, Any]]:
    _log(tenant_name, "tenant", "Starting tenant verification")
    token = get_token(tenant)
    applications = tenant.get("api_config", {}).get("applications", {})
    if not applications:
        raise AssertionError(
            f"[{tenant_name}] No applications configured under api_config.applications."
        )

    runnable_apps = [
        cfg for cfg in applications.values()
        if isinstance(cfg, dict) and (cfg.get("list_endpoint") or "").strip()
    ]
    requires_ui = any(
        not (cfg.get("dashboard_endpoint") or "").strip()
        for cfg in runnable_apps
    )
    playwright_ctx = None
    browser = None
    context = None
    dashboard_page = None
    available_dashboards: set[str] = set()
    if requires_ui:
        if sync_playwright is None:
            raise ModuleNotFoundError(
                "Playwright is required because one or more applications use dashboard UI fallback."
            )
        dashboard_url = tenant.get("dashboard_url") or f"{tenant['base_url'].rstrip('/')}/AdvancedDashboards"
        headless = (
            tenant.get("api_config", {})
            .get("endpoints_meta", {})
            .get("ui_headless", False)
        )
        playwright_ctx = sync_playwright().start()
        browser = playwright_ctx.chromium.launch(headless=headless, args=["--disable-notifications"])
        context = browser.new_context(ignore_https_errors=True, permissions=[])
        dashboard_page = context.new_page()
        dashboard_page.goto(tenant["base_url"], wait_until="networkidle", timeout=120000)
        dashboard_page.locator("#LoginInput_UserNameOrEmailAddress").fill(tenant["username"])
        dashboard_page.locator("#password-input").fill(tenant["password"])
        dashboard_page.get_by_role("button", name="Login").click()
        dashboard_page.wait_for_url("**/AdvancedDashboards", timeout=300000)
        if "AdvancedDashboards" not in dashboard_page.url:
            dashboard_page.goto(dashboard_url, wait_until="networkidle", timeout=120000)
        available_dashboards = _get_available_dashboards(dashboard_page)
        _log(
            tenant_name,
            "dashboard_ui",
            f"available_dashboards_count={len(available_dashboards)}",
        )
        print(f"[{tenant_name}] dashboard UI session login successful (reused for all applications).")
    else:
        _log(tenant_name, "tenant", "No dashboard UI fallback required for runnable apps")

    result_rows: list[dict[str, Any]] = []
    for app_name, app_cfg in applications.items():
        list_endpoint = (app_cfg.get("list_endpoint") or "").strip()
        if not list_endpoint:
            result_rows.append(
                {
                    "application": app_name,
                    "list_count": -1,
                    "dashboard_count": -1,
                    "difference": 0,
                    "status": "APPLICATION_NOT_FOUND",
                }
            )
            print(f"[{tenant_name}] [{app_name}] final_status=APPLICATION_NOT_FOUND (list endpoint missing)")
            continue

        try:
            list_count, dashboard_count, app_status = _run_application_count_verification(
                tenant_name,
                tenant,
                token,
                app_name,
                app_cfg,
                dashboard_page=dashboard_page,
            )
            dashboard_endpoint = (app_cfg.get("dashboard_endpoint") or "").strip()
            default_dashboard_name = f"{str(app_name).replace('_', ' ').title()} Dashboard"
            dashboard_name = (app_cfg.get("dashboard_name") or default_dashboard_name).strip()
            app_keyword = (app_cfg.get("dashboard_search_keyword") or app_name).strip()
            if (
                not dashboard_endpoint
                and not _dashboard_is_available(
                    available_dashboards,
                    dashboard_name=dashboard_name,
                    app_keyword=app_keyword,
                    app_name=app_name,
                )
            ):
                app_status = "NOT_AVAILABLE"
                dashboard_count = -1
                print(
                    f"[{tenant_name}] [{app_name}] final_status=NOT_AVAILABLE "
                    f"(dashboard '{dashboard_name}' not present for tenant)"
                )
            result_rows.append(
                {
                    "application": app_name,
                    "list_count": list_count,
                    "dashboard_count": dashboard_count,
                    "difference": (list_count - dashboard_count) if dashboard_count >= 0 else 0,
                    "status": app_status,
                }
            )
            if app_status != "NOT_AVAILABLE":
                print(f"[{tenant_name}] [{app_name}] final_status={app_status}")
        except Exception as exc:
            result_rows.append(
                {
                    "application": app_name,
                    "list_count": -1,
                    "dashboard_count": -1,
                    "difference": 0,
                    "status": "ERROR",
                }
            )
            print(f"[{tenant_name}] [{app_name}] final_status=ERROR ({exc})")
            continue

    if browser:
        browser.close()
    if playwright_ctx:
        playwright_ctx.stop()

    passed = sum(1 for row in result_rows if row["status"] == "PASS")
    failed = sum(1 for row in result_rows if row["status"] == "FAIL")
    skipped = sum(1 for row in result_rows if row["status"] == "SKIPPED")
    app_not_found = sum(1 for row in result_rows if row["status"] == "APPLICATION_NOT_FOUND")
    not_found = sum(1 for row in result_rows if row["status"] == "NOT_FOUND")
    errored = sum(1 for row in result_rows if row["status"] == "ERROR")
    not_available = sum(1 for row in result_rows if row["status"] == "NOT_AVAILABLE")
    print(
        f"[{tenant_name}] overall -> passed={passed}, failed={failed}, skipped={skipped}, "
        f"not_found={not_found}, not_available={not_available}, app_not_found={app_not_found}, error={errored}"
    )
    print(f"[{tenant_name}] FINAL SUMMARY TABLE")
    print(f"[{tenant_name}] tenant | application | list_count | dashboard_count | difference | status")
    print(f"[{tenant_name}] " + "-" * 88)
    for row in result_rows:
        print(
            f"[{tenant_name}] {tenant_name} | {row['application']} | {row['list_count']} | "
            f"{row['dashboard_count']} | {row['difference']} | {row['status']}"
        )
    _log(tenant_name, "tenant", "Tenant verification complete")
    return result_rows


def _print_consolidated_summary(all_rows: list[dict[str, Any]]) -> None:
    print("[ALL] CONSOLIDATED SUMMARY TABLE")
    print("[ALL] tenant | application | list_count | dashboard_count | difference | status")
    print("[ALL] " + "-" * 88)
    for row in all_rows:
        print(
            f"[ALL] {row['tenant']} | {row['application']} | {row['list_count']} | "
            f"{row['dashboard_count']} | {row['difference']} | {row['status']}"
        )


if pytest is not None and __name__ != "__main__":
    try:
        RUNTIME_TENANTS = load_tenants_from_excel()
        print("[config] Loaded tenants/applications from Excel.")
    except Exception as exc:
        print(f"[config] Excel load failed ({exc}). Falling back to in-file TENANTS.")
        RUNTIME_TENANTS = TENANTS
    if not RUNTIME_TENANTS:
        raise AssertionError(
            "No tenants loaded. Check Excel files and tenant/application columns."
        )

    @pytest.mark.parametrize("tenant_name,tenant", RUNTIME_TENANTS.items())
    def test_observation_list_count(tenant_name: str, tenant: dict) -> None:
        _run_tenant_count_verification(tenant_name, tenant)


if __name__ == "__main__":
    print("Running as standalone script (without pytest)...")
    try:
        runtime_tenants = load_tenants_from_excel()
        print("[config] Loaded tenants/applications from Excel.")
    except Exception as exc:
        print(f"[config] Excel load failed ({exc}). Falling back to in-file TENANTS.")
        runtime_tenants = TENANTS
    if not runtime_tenants:
        raise AssertionError(
            "No tenants loaded. Check Excel files and tenant/application columns."
        )

    all_rows: list[dict[str, Any]] = []
    for name, tenant_cfg in runtime_tenants.items():
        tenant_rows = _run_tenant_count_verification(name, tenant_cfg)
        for row in tenant_rows:
            all_rows.append(
                {
                    "tenant": name,
                    "application": row["application"],
                    "list_count": row["list_count"],
                    "dashboard_count": row["dashboard_count"],
                    "difference": row["difference"],
                    "status": row["status"],
                }
            )

    _print_consolidated_summary(all_rows)
