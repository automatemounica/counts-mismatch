from __future__ import annotations
import copy
import os
import re
from typing import Any

# Intentionally empty: tenants are sourced from Excel files.
TENANTS: dict[str, dict[str, Any]] = {}

DEFAULT_EXCEL_API_PATH = r"C:\Users\mojagu.CORP\Downloads\formatted_api_endpoints.xlsx"
DEFAULT_EXCEL_CREDENTIALS_PATH = r"C:\Users\mojagu.CORP\Downloads\Demo site credentials.xlsx"
DEFAULT_EXCEL_TEMPLATE_PATH = r"C:\Users\mojagu.CORP\Downloads\Template.xlsx"
DEFAULT_TENANT_MODULES_JSON = r"C:\Users\mojagu.CORP\Downloads\tenants_with_modules_updated.json"

# Per-instance credentials Excel files — add new instances here.
INSTANCE_CREDENTIALS_PATHS: dict[str, str] = {
    "demo": r"C:\Users\mojagu.CORP\Downloads\Demo site credentials.xlsx",
    "dev":  r"C:\Users\mojagu.CORP\Downloads\DEV site credentials.xlsx",
}

# Maps module names from the JSON (lowercased, stripped) -> app key used in API/SQL config.
MODULE_NAME_TO_APP_KEY: dict[str, str] = {
    "action tracker":                   "actionmanagements",
    "actions":                          "actionmanagements",
    "audit management":                 "auditsmanagements",
    "audit managements":                "auditsmanagements",
    "customer complaints":              "customercomplaints",
    "file management":                  "filemanagement",
    "file managment":                   "filemanagement",
    "filemanagement":                   "filemanagement",
    "hse monthly statistics unified":   "hsemonthlystatistics",
    "hse monthly statistics oneics":    "hsemonthlystatistics",
    "hse monthly statistics":           "hsemonthlystatistics",
    "hse monthly stats unified":        "hsemonthlystatistics",
    "hse monthly stat":                 "hsemonthlystatistics",
    "incident management":              "incidentmanagement",
    "inspection":                       "inspectionmanagements",
    "inspections":                      "inspectionmanagements",
    "management of change":             "managementofchanges",
    "management of changes":            "managementofchanges",
    "meeting management":               "meetingmanagements",
    "meeting managements":              "meetingmanagements",
    "non conformance":                  "nonconformances",
    "moc non conformance":              "mocnonconformances",
    "observation":                      "observations",
    "observations":                     "observations",
    "obsverations":                     "observations",
    "risk management":                  "riskmanagements",
    "risk managment":                   "riskmanagements",
    "risk assessments":                 "riskmanagements",
    "survey management":                "surveymanagements",
    "survery management":               "surveymanagements",
    "training management":              "trainingmanagements",
    "training managements":             "trainingmanagements",
    "8d report":                        "eightdreports",
    "hse plan":                         "hseplandetails",
    "hse plans":                        "hseplandetails",
    "permit to work":                   "permittoworks",
    "vehicle inspections":              "vehicle_inspections",
    "ofi":                              "ofis",
    "sor":                              "sor",
}
DEFAULT_AUTH_URL = "https://authserver.demoehswatch.com/connect/token"
DEFAULT_API_GATEWAY_URL = "https://webgateway.demoehswatch.com"
DEV_AUTH_URL = "https://authserver.dev-ehswatch.com/connect/token"
DEV_API_GATEWAY_URL = "https://webgateway.dev-ehswatch.com"
DEFAULT_CLIENT_ID = "EHSWatch_MobileApp"
DEFAULT_CLIENT_SECRET = "Exceego@890"
DEFAULT_SCOPE = (
    "IdentityService SaasService ObservationsService AdministrationService EmployeeService IncidentService InspectionService UserTaskService AttachmentService ActionService RMService TMService ReportService FileManagement HSEPlansService Forms CustomerService PTWService NCRService"
)
# Scopes that may not be provisioned for every tenant. If the auth server
# rejects the full DEFAULT_SCOPE with invalid_scope, these are dropped one
# at a time in this order until the request is accepted. Order matters:
# scopes most likely to be unprovisioned go FIRST so they get dropped first;
# scopes that are commonly needed (e.g. HSEPlansService) go LAST so they
# stay in the token whenever possible.
OPTIONAL_SCOPES = [
    "PTWService",
    "HSEPlansService",
    "ReportService",
    "FileManagement",
    "TMService",
    "Forms",
    "CustomerService",
    "NCRService",
    "RMService",
]
DEFAULT_TOKEN_CONFIG = {
    "auth_url": DEFAULT_AUTH_URL,
    "client_id": DEFAULT_CLIENT_ID,
    "client_secret": DEFAULT_CLIENT_SECRET,
    "scope": DEFAULT_SCOPE,
}


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _key(value: Any) -> str:
    text = _norm(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def _slug(value: Any) -> str:
    text = _key(value)
    return "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in text).strip("_")


def _pick(row: dict[str, Any], aliases: list[str]) -> str:
    for alias in aliases:
        if alias in row and _norm(row[alias]):
            return _norm(row[alias])
    return ""


def _pick_fuzzy(row: dict[str, Any], aliases: list[str], token_groups: list[list[str]]) -> str:
    exact = _pick(row, aliases)
    if exact:
        return exact
    for key, value in row.items():
        normalized_key = _key(key)
        if not _norm(value):
            continue
        for tokens in token_groups:
            if all(token in normalized_key for token in tokens):
                return _norm(value)
    return ""


def _normalize_tenant_name(value: Any) -> str:
    text = _norm(value).lower()
    text = text.replace("https://", "").replace("http://", "")
    text = text.split("/")[0].strip()
    if text.endswith(".demoehswatch.com"):
        text = text.replace(".demoehswatch.com", "")
    return text


def _is_valid_tenant_key(name: str) -> bool:
    if not name:
        return False
    if name.isdigit():
        return False
    return bool(re.fullmatch(r"[a-z0-9][a-z0-9_-]{1,40}", name))


def _guess_tenant_from_row_values(row: dict[str, Any]) -> str:
    for raw in row.values():
        text = _normalize_tenant_name(raw)
        if not text:
            continue
        if "demoehswatch.com" in _norm(raw).lower():
            return text
        if re.fullmatch(r"[a-z0-9][a-z0-9_-]{1,40}", text):
            # Avoid obvious key/value labels
            if text not in {"username", "password", "tenant", "site"}:
                return text
    return ""


def _derive_app_name(app_name: str, dashboard_name: str, list_endpoint: str) -> str:
    if app_name:
        return _slug(app_name)
    if dashboard_name:
        return _slug(dashboard_name.replace("Dashboard", "").strip())
    path = _norm(list_endpoint).lower()
    if not path:
        return ""
    clean_path = path.split("?")[0].strip("/")
    if not clean_path:
        return ""
    segments = [seg for seg in clean_path.split("/") if seg]
    # Pick the most meaningful segment from the tail, skipping generic tokens.
    generic_tokens = {"api", "service", "managements", "management", "details", "detail", "list"}
    for seg in reversed(segments):
        candidate = _slug(seg)
        if not candidate or candidate in generic_tokens:
            continue
        if candidate.endswith("_list"):
            candidate = candidate[:-5]
        if candidate.endswith("s"):
            return candidate
        return f"{candidate}s"
    return ""


def _normalize_label(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _norm(value).lower()).strip()


def _read_excel_rows(path: str) -> list[dict[str, Any]]:
    try:
        import pandas as pd  # type: ignore
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "Excel-driven mode requires pandas. Install with: pip install pandas openpyxl"
        ) from exc

    rows: list[dict[str, Any]] = []
    workbook = pd.ExcelFile(path)
    print(f"[config] Reading Excel: {path}")
    print(f"[config] Sheets found: {workbook.sheet_names}")
    for sheet in workbook.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
        if df.empty:
            print(f"[config] Sheet '{sheet}' is empty; skipped.")
            continue
        print(f"[config] Sheet '{sheet}' rows={len(df)}")
        normalized_columns = {_key(c): c for c in df.columns.tolist()}
        for _, series in df.iterrows():
            row: dict[str, Any] = {}
            for nk, original in normalized_columns.items():
                val = series.get(original)
                if val is None:
                    continue
                # NaN check without importing math/pandas
                if str(val).lower() == "nan":
                    continue
                row[nk] = val
            if row:
                rows.append(row)
    return rows


def load_tenants_from_excel(
    api_excel_path: str = DEFAULT_EXCEL_API_PATH,
    credentials_excel_path: str = DEFAULT_EXCEL_CREDENTIALS_PATH,
    template_excel_path: str = DEFAULT_EXCEL_TEMPLATE_PATH,
) -> dict[str, Any]:
    """
    Build runtime tenant configuration from Excel sheets.
    Tenants/applications are sourced from Excel; no hardcoded tenant entries.
    """
    tenants = copy.deepcopy(TENANTS)
    print("[config] Starting tenant load from Excel files...")
    print("[config] Using shared token defaults: auth_url, client_id, client_secret, scope")

    # 1) Pull tenant-level overrides from credentials workbook (preferred).
    try:
        credentials_rows = _read_excel_rows(credentials_excel_path)
    except Exception:
        credentials_rows = []

    try:
        template_rows = _read_excel_rows(template_excel_path)
    except Exception:
        template_rows = []

    def _extract_tenant_from_row(row: dict[str, Any]) -> str:
        tenant = _normalize_tenant_name(_pick_fuzzy(
            row,
            ["tenant", "tenant_name", "tenantname", "name", "client_name", "client"],
            [["tenant"], ["site"], ["subdomain"], ["client", "name"], ["client"]],
        ))
        if not tenant:
            tenant = _guess_tenant_from_row_values(row)
        return tenant if _is_valid_tenant_key(tenant) else ""

    template_tenants = {_extract_tenant_from_row(r) for r in template_rows}
    credentials_tenants = {_extract_tenant_from_row(r) for r in credentials_rows}
    template_tenants.discard("")
    credentials_tenants.discard("")
    validated_tenants = template_tenants.intersection(credentials_tenants)
    missing_in_credentials = sorted(template_tenants - credentials_tenants)
    if missing_in_credentials:
        print(
            "[config] Tenants present in template but missing in credentials (skipped): "
            + ", ".join(missing_in_credentials)
        )
    print(
        f"[config] Tenant validation -> template={len(template_tenants)}, "
        f"credentials={len(credentials_tenants)}, valid={len(validated_tenants)}"
    )

    # Build explicit credentials override map (authoritative for auth fields).
    credentials_override: dict[str, dict[str, str]] = {}
    for row in credentials_rows:
        tenant_name = _normalize_tenant_name(_pick_fuzzy(
            row,
            ["tenant", "tenant_name", "tenantname", "name", "client_name", "client"],
            [["tenant"], ["site"], ["subdomain"], ["client", "name"], ["client"]],
        ))
        if not tenant_name:
            tenant_name = _guess_tenant_from_row_values(row)
        if not _is_valid_tenant_key(tenant_name):
            continue
        auth_bits: dict[str, str] = {}
        for key_name, aliases, tokens in (
            ("username", ["username", "user_name", "user"], [["user", "name"], ["login", "id"]]),
            ("password", ["password", "pass"], [["password"], ["pwd"]]),
            ("scope", ["scope"], [["scope"]]),
            ("client_id", ["client_id"], [["client", "id"]]),
            ("client_secret", ["client_secret"], [["client", "secret"], ["secret"]]),
        ):
            value = _pick_fuzzy(row, aliases, tokens)
            if value:
                auth_bits[key_name] = value
        if auth_bits:
            credentials_override.setdefault(tenant_name, {}).update(auth_bits)

    # Merge both sources based on file modified time (latest file wins).
    source_rows: list[tuple[str, list[dict[str, Any]]]] = [
        (credentials_excel_path, credentials_rows),
        (template_excel_path, template_rows),
    ]
    ordered_sources = sorted(
        source_rows,
        key=lambda item: os.path.getmtime(item[0]) if os.path.exists(item[0]) else 0,
    )
    print(
        "[config] Tenant source priority (oldest->latest): "
        + " -> ".join(path for path, _ in ordered_sources)
    )
    all_tenant_rows: list[dict[str, Any]] = []
    for _, rows in ordered_sources:
        all_tenant_rows.extend(rows)
    print(
        f"[config] Tenant rows parsed: template={len(template_rows)}, "
        f"credentials={len(credentials_rows)}, merged={len(all_tenant_rows)}"
    )

    for row in all_tenant_rows:
        tenant_name = _normalize_tenant_name(_pick_fuzzy(
            row,
            ["tenant", "tenant_name", "tenantname", "name", "client_name", "client"],
            [["tenant"], ["site"], ["subdomain"], ["client", "name"], ["client"]],
        ))
        if not tenant_name:
            tenant_name = _guess_tenant_from_row_values(row)
        if not _is_valid_tenant_key(tenant_name):
            if tenant_name:
                print(f"[config] Tenant row skipped: invalid tenant name '{tenant_name}'")
            continue
        if tenant_name not in validated_tenants:
            continue
        if tenant_name not in tenants:
            tenants[tenant_name] = {"name": tenant_name, "api_config": {"applications": {}}}
        tenant = tenants[tenant_name]
        for target_key, aliases in (
            ("base_url", ["base_url", "tenant_url", "url"]),
            ("api_gateway_url", ["api_gateway_url", "gateway_url", "api_base_url"]),
            ("auth_url", ["auth_url", "token_url"]),
            ("client_id", ["client_id"]),
            ("client_secret", ["client_secret"]),
            ("username", ["username", "user_name", "user"]),
            ("password", ["password", "pass"]),
            ("scope", ["scope"]),
        ):
            fuzzy_tokens = {
                "base_url": [["base", "url"], ["tenant", "url"]],
                "api_gateway_url": [["api", "gateway"], ["gateway", "url"]],
                "auth_url": [["auth", "url"], ["token", "url"]],
                "client_id": [["client", "id"]],
                "client_secret": [["client", "secret"], ["secret"]],
                "username": [["user", "name"], ["login", "id"]],
                "password": [["password"], ["pwd"]],
                "scope": [["scope"]],
            }.get(target_key, [])
            value = _pick_fuzzy(row, aliases, fuzzy_tokens)
            if value:
                tenant[target_key] = value

        # Template rows provide tenant+application dashboard metadata.
        row_app_name = _pick_fuzzy(
            row,
            ["application", "application_name", "app_name", "module"],
            [["application"], ["module"], ["feature"]],
        ).lower()
        row_dashboard_name = _pick_fuzzy(
            row,
            ["dashboard_name", "dashboard", "dashboard_title"],
            [["dashboard"]],
        )
        row_dashboard_label = _pick_fuzzy(
            row,
            ["dashboard_count_label", "label_name", "count_label", "kpi_label"],
            [["label"], ["count", "label"], ["kpi", "label"]],
        )
        if row_app_name:
            app_key = _slug(row_app_name)
            apps = tenant.setdefault("api_config", {}).setdefault("applications", {})
            app_cfg = apps.setdefault(app_key, {})
            if row_dashboard_name:
                app_cfg["dashboard_name"] = row_dashboard_name
            if row_dashboard_label:
                app_cfg["dashboard_count_label"] = row_dashboard_label
            row_dashboard_keyword = _pick_fuzzy(
                row,
                ["dashboard_search_keyword", "dashboard_keyword", "search_keyword"],
                [["search", "keyword"], ["dashboard", "keyword"]],
            )
            if row_dashboard_keyword:
                app_cfg["dashboard_search_keyword"] = row_dashboard_keyword
        print(f"[config] Template tenant mapped: {tenant_name}")

    # 2) Pull application/list/dashboard mapping from API workbook.
    api_rows = _read_excel_rows(api_excel_path)
    print(f"[config] API rows parsed: {len(api_rows)}")
    global_apps: dict[str, dict[str, Any]] = {}
    for row in api_rows:
        tenant_name = _normalize_tenant_name(_pick_fuzzy(
            row,
            ["tenant", "tenant_name", "tenantname", "name", "client_name", "client"],
            [["tenant"], ["site"], ["subdomain"], ["client", "name"], ["client"]],
        ))
        dashboard_name = _pick_fuzzy(
            row,
            ["dashboard_name", "dashboard", "dashboard_title"],
            [["dashboard"]],
        )
        app_name_raw = _pick_fuzzy(
            row,
            ["application", "application_name", "app_name", "module"],
            [["application"], ["module"], ["feature"]],
        ).lower()
        list_endpoint = _pick_fuzzy(
            row,
            ["list_endpoint", "endpoint", "api_endpoint", "list_api", "api_path"],
            [["api", "endpoint"], ["list", "api"], ["endpoint"]],
        )
        app_name = _derive_app_name(app_name_raw, dashboard_name, list_endpoint)
        if not app_name:
            print(f"[config] Row skipped: tenant={tenant_name or '<blank>'}, reason=missing application/dashboard mapping")
            continue
        if app_name.startswith("list_endpoint") or app_name.startswith("list_method"):
            print(f"[config] Row skipped: tenant={tenant_name or '<blank>'}, app={app_name}, reason=metadata row")
            continue
        if not _norm(list_endpoint).startswith("/"):
            print(f"[config] Row skipped: tenant={tenant_name or '<blank>'}, app={app_name}, reason=invalid endpoint")
            continue
        list_method = _pick_fuzzy(
            row,
            ["list_method", "method", "http_method"],
            [["http", "method"], ["method"]],
        ) or "GET"
        dashboard_label = _pick_fuzzy(
            row,
            ["dashboard_count_label", "label_name", "count_label", "kpi_label"],
            [["label"], ["count", "label"], ["kpi", "label"]],
        )
        dashboard_keyword = _pick_fuzzy(
            row,
            ["dashboard_search_keyword", "dashboard_keyword", "search_keyword"],
            [["search", "keyword"], ["dashboard", "keyword"]],
        )

        target_apps_container: dict[str, dict[str, Any]]
        if tenant_name:
            if tenant_name not in tenants:
                # Respect Step-1 validation: ignore API rows for non-validated tenants.
                continue
            tenant = tenants[tenant_name]
            target_apps_container = tenant.setdefault("api_config", {}).setdefault("applications", {})
        else:
            target_apps_container = global_apps

        app_cfg = target_apps_container.setdefault(app_name, {})
        if list_endpoint:
            app_cfg["list_endpoint"] = list_endpoint
        app_cfg["list_method"] = list_method.upper()
        if dashboard_name:
            app_cfg["dashboard_name"] = dashboard_name
        if dashboard_label:
            app_cfg["dashboard_count_label"] = dashboard_label
        if dashboard_keyword:
            app_cfg["dashboard_search_keyword"] = dashboard_keyword
        print(
            f"[config] Application mapped: tenant={tenant_name}, app={app_name}, "
            f"list_endpoint={bool(app_cfg.get('list_endpoint'))}, dashboard_name={app_cfg.get('dashboard_name', '')}"
        )

    # IMPORTANT: only keep tenant applications coming from Template/Credentials.
    # API sheet should enrich endpoint/method only for matching application names.
    if global_apps:
        print(f"[config] API catalog size (global apps): {len(global_apps)}")
    for tenant_name, tenant in tenants.items():
        apps = tenant.setdefault("api_config", {}).setdefault("applications", {})
        for app_name, app_cfg in apps.items():
            # Priority:
            # 1) tenant-specific app row in API sheet, if available in same tenant
            # 2) global app row in API sheet
            tenant_api_cfg = (
                tenants.get(tenant_name, {})
                .get("api_config", {})
                .get("applications", {})
                .get(app_name, {})
            )
            source_cfg = tenant_api_cfg if tenant_api_cfg.get("list_endpoint") else global_apps.get(app_name, {})
            if source_cfg.get("list_endpoint") and not app_cfg.get("list_endpoint"):
                app_cfg["list_endpoint"] = source_cfg["list_endpoint"]
            if source_cfg.get("list_method") and not app_cfg.get("list_method"):
                app_cfg["list_method"] = source_cfg["list_method"]

    # 3) Normalize defaults and keep only runnable tenants.
    required_for_token = ["username", "password"]
    sanitized: dict[str, Any] = {}
    for tenant_name, tenant in tenants.items():
        if tenant_name in credentials_override:
            tenant.update(credentials_override[tenant_name])
        tenant.setdefault("name", tenant_name)
        tenant.setdefault("auth_url", DEFAULT_AUTH_URL)
        tenant.setdefault("api_gateway_url", DEFAULT_API_GATEWAY_URL)
        tenant.setdefault("client_id", DEFAULT_TOKEN_CONFIG["client_id"])
        tenant.setdefault("client_secret", DEFAULT_TOKEN_CONFIG["client_secret"])
        tenant.setdefault("scope", DEFAULT_TOKEN_CONFIG["scope"])
        if not tenant.get("base_url"):
            tenant["base_url"] = f"https://{tenant_name}.demoehswatch.com"

        missing = [k for k in required_for_token if not _norm(tenant.get(k))]
        applications = tenant.get("api_config", {}).get("applications", {})
        if missing:
            print(f"[config] Tenant skipped (missing token fields): tenant={tenant_name}, missing={missing}")
            continue
        if not isinstance(applications, dict) or not applications:
            print(f"[config] Tenant skipped (no applications mapped): tenant={tenant_name}")
            continue
        sanitized[tenant_name] = tenant

    tenants = sanitized
    print(f"[config] Total tenants loaded: {len(tenants)}")
    return tenants


def _load_tenant_modules_json(path: str = DEFAULT_TENANT_MODULES_JSON) -> dict[str, set[str]]:
    """Load JSON and return {tenant_key -> set of app_keys} using MODULE_NAME_TO_APP_KEY."""
    import json
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        print(f"[config] Could not load tenant modules JSON ({exc}); all apps will run for all tenants.")
        return {}
    result: dict[str, set[str]] = {}
    for tenant_raw, modules in data.items():
        tenant_key = _normalize_tenant_name(tenant_raw)
        if not _is_valid_tenant_key(tenant_key):
            continue
        app_keys: set[str] = set()
        for entry in modules:
            module_name = _norm(entry.get("Modules", "")).lower().strip()
            app_key = MODULE_NAME_TO_APP_KEY.get(module_name)
            if app_key:
                app_keys.add(app_key)
            else:
                print(f"[config] No app key mapping for module '{module_name}' (tenant={tenant_key}); skipped.")
        if app_keys:
            result[tenant_key] = app_keys
    print(f"[config] Tenant modules JSON loaded: {len(result)} tenant(s)")
    return result


def load_tenants_for_sql_mode(
    api_excel_path: str = DEFAULT_EXCEL_API_PATH,
    credentials_excel_path: str = DEFAULT_EXCEL_CREDENTIALS_PATH,
    domain: str = "demoehswatch.com",
) -> dict[str, Any]:
    """
    Build tenant configuration for SQL-based count verification.
    Reads ONLY:
      - credentials Excel  -> tenant list + auth fields
      - API endpoints Excel -> applications + list_endpoint + list_method
    Template.xlsx is intentionally not used here; the SQL queries replace
    the dashboard UI step that previously needed Template metadata.
    Every credential tenant is paired with every application from the API
    catalog, so apps a given tenant doesn't have provisioned will fail at
    the API/SQL call (visible as ERROR/SQL_ERROR in the issues report).
    """
    print("[config] Starting tenant load for SQL mode (credentials + api_endpoints only)...")
    tenants: dict[str, dict[str, Any]] = copy.deepcopy(TENANTS)

    # 1) Build tenants from credentials workbook.
    credentials_rows = _read_excel_rows(credentials_excel_path)
    for row in credentials_rows:
        tenant_name = _normalize_tenant_name(_pick_fuzzy(
            row,
            ["tenant", "tenant_name", "tenantname", "name", "client_name", "client"],
            [["tenant"], ["site"], ["subdomain"], ["client", "name"], ["client"]],
        ))
        if not tenant_name:
            tenant_name = _guess_tenant_from_row_values(row)
        if not _is_valid_tenant_key(tenant_name):
            if tenant_name:
                print(f"[config] Credentials row skipped: invalid tenant name '{tenant_name}'")
            continue
        if tenant_name not in tenants:
            tenants[tenant_name] = {"name": tenant_name, "api_config": {"applications": {}}}
        tenant = tenants[tenant_name]
        for target_key, aliases in (
            ("base_url", ["base_url", "tenant_url", "url"]),
            ("api_gateway_url", ["api_gateway_url", "gateway_url", "api_base_url"]),
            ("auth_url", ["auth_url", "token_url"]),
            ("client_id", ["client_id"]),
            ("client_secret", ["client_secret"]),
            ("username", ["username", "user_name", "user"]),
            ("password", ["password", "pass"]),
            ("scope", ["scope"]),
        ):
            fuzzy_tokens = {
                "base_url": [["base", "url"], ["tenant", "url"]],
                "api_gateway_url": [["api", "gateway"], ["gateway", "url"]],
                "auth_url": [["auth", "url"], ["token", "url"]],
                "client_id": [["client", "id"]],
                "client_secret": [["client", "secret"], ["secret"]],
                "username": [["user", "name"], ["login", "id"]],
                "password": [["password"], ["pwd"]],
                "scope": [["scope"]],
            }.get(target_key, [])
            value = _pick_fuzzy(row, aliases, fuzzy_tokens)
            if value:
                tenant[target_key] = value
    print(f"[config] Tenants from credentials: {len(tenants)}")

    # 2) Build the global application catalog from API workbook.
    api_rows = _read_excel_rows(api_excel_path)
    global_apps: dict[str, dict[str, Any]] = {}
    for row in api_rows:
        app_name_raw = _pick_fuzzy(
            row,
            ["application", "application_name", "app_name", "module"],
            [["application"], ["module"], ["feature"]],
        ).lower()
        list_endpoint = _pick_fuzzy(
            row,
            ["list_endpoint", "endpoint", "api_endpoint", "list_api", "api_path"],
            [["api", "endpoint"], ["list", "api"], ["endpoint"]],
        )
        app_name = _derive_app_name(app_name_raw, "", list_endpoint)
        if not app_name:
            continue
        if not _norm(list_endpoint).startswith("/"):
            continue
        list_method = _pick_fuzzy(
            row,
            ["list_method", "method", "http_method"],
            [["http", "method"], ["method"]],
        ) or "GET"
        global_apps[app_name] = {
            "list_endpoint": _norm(list_endpoint),
            "list_method": list_method.upper(),
        }
    print(f"[config] Applications loaded from API endpoints sheet: {len(global_apps)}")

    # 3) Attach apps per tenant filtered by the modules JSON.
    # Only apps that are actually provisioned for a tenant (per JSON) are assigned.
    # If a tenant has no JSON entry, fall back to the full global catalog.
    tenant_modules = _load_tenant_modules_json()
    for tenant_name, tenant in tenants.items():
        allowed = tenant_modules.get(tenant_name)
        if allowed:
            filtered = {k: v for k, v in global_apps.items() if k in allowed}
            # Include apps from JSON that have no Excel entry (may have TENANT_SPECIFIC_ENDPOINTS override)
            for app_key in allowed:
                if app_key not in filtered:
                    filtered[app_key] = {}
            tenant["api_config"]["applications"] = filtered
            print(f"[config] {tenant_name}: {len(filtered)} app(s) from JSON module list")
        else:
            tenant["api_config"]["applications"] = copy.deepcopy(global_apps)
            print(f"[config] {tenant_name}: {len(global_apps)} app(s) (no JSON entry, using full catalog)")

    # 4) Apply auth defaults and drop tenants missing required fields.
    auth_url     = f"https://authserver.{domain}/connect/token"
    gateway_url  = f"https://webgateway.{domain}"
    required_for_token = ["username", "password"]
    sanitized: dict[str, Any] = {}
    for tenant_name, tenant in tenants.items():
        tenant.setdefault("name", tenant_name)
        tenant.setdefault("auth_url", auth_url)
        tenant.setdefault("api_gateway_url", gateway_url)
        tenant.setdefault("client_id", DEFAULT_TOKEN_CONFIG["client_id"])
        tenant.setdefault("client_secret", DEFAULT_TOKEN_CONFIG["client_secret"])
        tenant.setdefault("scope", DEFAULT_TOKEN_CONFIG["scope"])
        if not tenant.get("base_url"):
            tenant["base_url"] = f"https://{tenant_name}.{domain}"
        missing = [k for k in required_for_token if not _norm(tenant.get(k))]
        if missing:
            print(f"[config] Tenant skipped (missing token fields): tenant={tenant_name}, missing={missing}")
            continue
        if not tenant["api_config"]["applications"]:
            print(f"[config] Tenant skipped (no applications in API catalog): tenant={tenant_name}")
            continue
        sanitized[tenant_name] = tenant
    print(f"[config] Total tenants loaded for SQL mode: {len(sanitized)}")
    return sanitized


# OPTIONAL - only for testing
if __name__ == "__main__":
    loaded = load_tenants_from_excel()
    print("✅ TENANTS loaded successfully")
    print("Total tenants:", len(loaded))
    print("Tenant names:", list(loaded.keys()))