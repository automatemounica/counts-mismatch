"""
SQL-based count verification.
Same flow as test_process.py — reads tenants from Excel, generates a token,
calls each application's list endpoint, and compares the API count against
a SQL Server query (one query per application, hardcoded below).
"""

from typing import Any

try:
    import pyodbc
except ModuleNotFoundError:
    pyodbc = None

from tenants import TENANTS, load_tenants_for_sql_mode
from test_process import _call_count_url, _log, get_token

# ============================================================================
# SQL Server connection.
# ============================================================================
DB_DRIVER = "{ODBC Driver 17 for SQL Server}"
DB_TRUSTED_AUTH = False  # True = Windows auth (ignores user / password)

DB_CONFIGS: dict[str, dict] = {
    "demo": {
        "server":   "devehswatchind.cxusmedxl659.ap-south-1.rds.amazonaws.com",
        "name":     "EHSWatchV3Q01_ReportService",
        "user":     "devuser",
        "password": "Ehswatch-Exceego",
    },
    "dev": {
        "server":   "devehswatchind.cxusmedxl659.ap-south-1.rds.amazonaws.com",
        "name":     "EHSWatchV3D01_ReportService",
        "user":     "devuser",
        "password": "Ehswatch-Exceego",
    },
}

_active_db_config: dict = DB_CONFIGS["demo"]


def set_active_instance(instance: str) -> None:
    global _active_db_config
    cfg = DB_CONFIGS.get(instance)
    if cfg is None:
        raise ValueError(f"No DB config for instance '{instance}'. Available: {list(DB_CONFIGS)}")
    _active_db_config = cfg

# Per-tenant schema prefix. Each tenant's data lives in a schema named
# 's<tenant>' (e.g. tenant 'albaraka' -> schema 'salbaraka').
# {schema} in the SQL templates below is replaced with this value.
TENANT_SCHEMA_PREFIX = "s"


# ============================================================================
# Per-application SQL.
#   {schema} -> resolved to TENANT_SCHEMA_PREFIX + tenant_name (e.g. 'salbaraka')
#   {tenant} -> the raw tenant key (e.g. 'albaraka'), if you ever need it
# Each query MUST return a row with a 'count' or 'totalCount' column.
# Tenant keys are validated by tenants.py (regex [a-z0-9][a-z0-9_-]{1,40}),
# so the format-string substitution is safe from SQL injection.
# Adjust the table names below to match your actual schema.
# ============================================================================
APP_SQL_QUERIES: dict[str, str] = {
    # Observations
    "observations": (
        "SELECT COUNT(*) AS count FROM {schema}.Observations WHERE IsDeleted = 0"
    ),
    # Incident Management (old key + new key)
    "incident_management": (
        "SELECT COUNT(*) AS count FROM {schema}.IncidentManagement WHERE IsDeleted = 0"
    ),
    "incidentmanagement": (
        "SELECT COUNT(*) AS count FROM {schema}.IncidentManagement WHERE IsDeleted = 0"
    ),
    # Action Tracker / Action Managements (old key + new key)
    "action_tracker": (
        "SELECT COUNT(*) AS count FROM {schema}.ActionManagements WHERE IsDeleted = 0"
    ),
    "actionmanagements": (
        "SELECT COUNT(*) AS count FROM {schema}.ActionManagements WHERE IsDeleted = 0"
    ),
    # Audits Management (old key + new key)
    "audits_management": (
        "SELECT COUNT(*) AS count FROM {schema}.AuditsManagements WHERE IsDeleted = 0"
    ),
    "auditsmanagements": (
        "SELECT COUNT(*) AS count FROM {schema}.AuditsManagements WHERE IsDeleted = 0"
    ),
    # Inspection Management (old key + new key)
    "inspection_management": (
        "SELECT COUNT(*) AS count FROM {schema}.InspectionManagements WHERE IsDeleted = 0"
    ),
    "inspectionmanagements": (
        "SELECT COUNT(*) AS count FROM {schema}.InspectionManagements WHERE IsDeleted = 0"
    ),
    # HSE Plan Details (old key + new key)
    "hse_plan_details": (
        "SELECT COUNT(*) AS count FROM {schema}.HSEPlanDetails WHERE IsDeleted = 0"
    ),
    "hseplandetails": (
        "SELECT COUNT(*) AS count FROM {schema}.HSEPlanDetails WHERE IsDeleted = 0"
    ),
    # Non Conformance (old key + new key)
    "non_conformance": (
        "SELECT COUNT(*) AS count FROM {schema}.NonConformanceDetails WHERE IsDeleted = 0"
    ),
    "nonconformances": (
        "SELECT COUNT(*) AS count FROM {schema}.NonConformanceDetails WHERE IsDeleted = 0"
    ),
    # MOC Non Conformance (old key + new key)
    "moc_non_conformance": (
        "SELECT COUNT(*) AS count FROM {schema}.MocNonConformanceDetails WHERE IsDeleted = 0"
    ),
    "mocnonconformances": (
        "SELECT COUNT(*) AS count FROM {schema}.MocNonConformanceDetails WHERE IsDeleted = 0"
    ),
    # Vehicle Inspections
    "vehicle_inspections": (
        "SELECT COUNT(*) AS count FROM {schema}.VehicleInspectionDetails WHERE IsDeleted = 0"
    ),
    # 8D Reports
    "eightdreports": (
        "SELECT COUNT(*) AS count FROM {schema}.EightDReports WHERE IsDeleted = 0"
    ),
    # Customer Complaints
    "customercomplaints": (
        "SELECT COUNT(*) AS count FROM {schema}.CustomerComplaintDetails WHERE IsDeleted = 0"
    ),
    # Meeting Managements
    "meetingmanagements": (
        "SELECT COUNT(*) AS count FROM {schema}.MeetingManagements WHERE IsDeleted = 0"
    ),
    # Risk Managements
    "riskmanagements": (
        "SELECT COUNT(*) AS count FROM {schema}.RiskAssesments WHERE IsDeleted = 0"
    ),
    "risk_management": (
        "SELECT COUNT(*) AS count FROM {schema}.RiskAssesments WHERE IsDeleted = 0"
    ),
    # Management of Change
    "managementofchanges": (
        "SELECT COUNT(*) AS count FROM {schema}.Mocs WHERE IsDeleted = 0"
    ),
    # Permit To Work
    "permittoworks": (
        "SELECT COUNT(*) AS count FROM {schema}.PermitToWork WHERE IsDeleted = 0"
    ),
    # Training Management
    "trainingmanagements": (
        "SELECT COUNT(*) AS count FROM {schema}.TrainingMatrices WHERE IsDeleted = 0"
    ),
    # HSE Monthly Statistics — unified variant (default for most tenants)
    "hsemonthlystatistics": (
        "SELECT COUNT(*) AS count FROM {schema}.HSEMonthlyStatisticsUnified WHERE IsDeleted = 0"
    ),
    # Survey Management
    "surveymanagements": (
        "SELECT COUNT(*) AS count FROM {schema}.Forms WHERE IsDeleted = 0"
    ),
    # OFI (Opportunity for Improvement)
    "ofis": (
        "SELECT COUNT(*) AS count FROM {schema}.Ofis WHERE IsDeleted = 0"
    ),
    # Legal Register
    "legal_register": (
        "SELECT COUNT(*) AS count FROM {schema}.LegalRegisterDetail WHERE IsDeleted = 0"
    ),
    # Communications
    "communications": (
        "SELECT COUNT(*) AS count FROM {schema}.CommunicationsManagements WHERE IsDeleted = 0"
    ),
    # Emergency Response Drills
    "emergency_response_drills": (
        "SELECT COUNT(*) AS count FROM {schema}.EmergencyResponseDrills WHERE IsDeleted = 0"
    ),
}

# Per-tenant SQL overrides — checked first before APP_SQL_QUERIES.
# Use this for apps where the table name differs between tenants.
TENANT_SPECIFIC_SQL_QUERIES: dict[str, dict[str, str]] = {
    # oneic has its own HSE Monthly Statistics table
    "oneic": {
        "hsemonthlystatistics": (
            "SELECT COUNT(*) AS count FROM {schema}.HSEMonthlyStatisticsONEIC WHERE IsDeleted = 0"
        ),
    },
    # oapil/omancables: NonconformanceDetails uses lowercase 'c' in table name
    "oapil": {
        "nonconformances": (
            "SELECT COUNT(*) AS count FROM {schema}.NonconformanceDetails WHERE IsDeleted = 0"
        ),
    },
    "omancables": {
        "nonconformances": (
            "SELECT COUNT(*) AS count FROM {schema}.NonconformanceDetails WHERE IsDeleted = 0"
        ),
    },
    "qia": {
        "nonconformances": (
            "SELECT COUNT(*) AS count FROM {schema}.NonconformanceDetails WHERE IsDeleted = 0"
        ),
    },
    "barik": {
        "hsemonthlystatistics": (
            "SELECT COUNT(*) AS count FROM {schema}.HSEMonthlyStatistics"
        ),
        "auditsmanagements": (
            "SELECT COUNT(*) AS count FROM {schema}.AuditsManagements"
        ),
        "mocnonconformances": (
            "SELECT COUNT(*) AS count FROM {schema}.MocNonconformanceDetails"
        ),
    },
    "sos": {
        "trainingmanagements": (
            "SELECT COUNT(*) AS count FROM {schema}.TrainingMatrices WHERE IsDeleted = 0"
        ),
    },
    "base": {
        "hsemonthlystatistics": (
            "SELECT COUNT(*) AS count FROM {schema}.HSEMonthlyStatistics WHERE IsDeleted = 0"
        ),
    },
}


# Per-tenant endpoint overrides — used when a tenant needs a different endpoint
# or HTTP method than what the global Excel catalog provides.
TENANT_SPECIFIC_ENDPOINTS: dict[str, dict[str, dict[str, str]]] = {
    "oneic": {
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statistics-oNEICS/hse-monthly-Statistics_list_post",
            "list_method": "POST",
        },
    },
    "alsumri": {
        "mocnonconformances": {
            "list_endpoint": "/api/nCR-service/moc-nonconformance-details/moc-nonconformance-details_list_post",
            "list_method": "POST",
        },
        "trainingmanagements": {
            "list_endpoint": "/api/tM-service/training-matrices/training-matrix_list?filterText=&extraProperties=%5B%5D&viewArchievedData=&showDataUptoTwoMonths=false&skipCount=0&maxResultCount=100",
            "list_method": "GET",
        },
    },
    "barik": {
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statisticss/hse-Statistics_list_post",
            "list_method": "POST",
        },
        "mocnonconformances": {
            "list_endpoint": "/api/nCR-service/moc-nonconformance-details/moc-nonconformance-details_list_post",
            "list_method": "POST",
        },
        "trainingmanagements": {
            "list_endpoint": "/api/tM-service/training-matrices/training-matrix_list?filterText=&extraProperties=%5B%5D&viewArchievedData=&showDataUptoTwoMonths=false&skipCount=0&maxResultCount=100",
            "list_method": "GET",
        },
    },
    "sos": {
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statistics-uNified/hse-monthly-Statistics_list_post",
            "list_method": "POST",
        },
        "managementofchanges": {
            "list_endpoint": "/api/nCR-service/mocs/moc-list-post",
            "list_method": "POST",
        },
        "surveymanagements": {
            "list_endpoint": "/api/form/forms/survey_list_post",
            "list_method": "POST",
        },
        "mocnonconformances": {
            "list_endpoint": "/api/nCR-service/moc-nonconformance-details/moc-nonconformance-details_list_post",
            "list_method": "POST",
        },
        "trainingmanagements": {
            "list_endpoint": "/api/tM-service/training-matrices/training-matrix_list?filterText=&extraProperties=%5B%5D&viewArchievedData=&showDataUptoTwoMonths=false&skipCount=0&maxResultCount=100",
            "list_method": "GET",
        },
    },
    "oapil": {
        "eightdreports": {
            "list_endpoint": "/api/nCR-service/eight-dReports/eightDReport_service_list-post",
            "list_method": "POST",
        },
        "nonconformances": {
            "list_endpoint": "/api/nCR-service/nonconformance-details/ncr_service_list_post",
            "list_method": "POST",
        },
    },
    "ndsc": {
        "riskmanagements": {
            "list_endpoint": "/api/rM-service/risk-assesments/risk-assesment_list_post",
            "list_method": "POST",
        },
        "ofis": {
            "list_endpoint": "/api/action-service/ofis/ofi_list",
            "list_method": "GET",
        },
    },
    "synergies": {
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statistics-uNified/hse-monthly-Statistics_list_post",
            "list_method": "POST",  
        },
    },
    "albaraka": {
        "managementofchanges": {
            "list_endpoint": "/api/nCR-service/mocs/moc-list-post",
            "list_method": "POST",
        },
        "trainingmanagements": {
            "list_endpoint": "/api/tM-service/training-matrices/training-matrix_list?filterText=&extraProperties=%5B%5D&viewArchievedData=&showDataUptoTwoMonths=false&skipCount=0&maxResultCount=100",
            "list_method": "GET",
        },
    },
    "omancables": {
        "eightdreports": {
            "list_endpoint": "/api/nCR-service/eight-dReports/eightDReport_service_list-post",
            "list_method": "POST",
        },
        "nonconformances": {
            "list_endpoint": "/api/nCR-service/nonconformance-details/ncr_service_list_post",
            "list_method": "POST",
        },
    },
    "qia": {
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statistics-uNified/hse-monthly-Statistics_list_post",
            "list_method": "POST",
        },
        "nonconformances": {
            "list_endpoint": "/api/nCR-service/nonconformance-details/ncr_service_list_post",
            "list_method": "POST",
        },
    },
    "ajbanpv3": {
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statistics-uNified/hse-monthly-Statistics_list_post",
            "list_method": "POST",
        },
    },
    "powerchina": {
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statistics-uNified/hse-monthly-Statistics_list_post",
            "list_method": "POST",
        },
    },
    "base": {
        "trainingmanagements": {
            "list_endpoint": "/api/tM-service/training-matrices/training-matrix_list?filterText=&extraProperties=%5B%5D&viewArchievedData=&showDataUptoTwoMonths=false&skipCount=0&maxResultCount=100",
            "list_method": "GET",
        },
        "observations": {
            "list_endpoint": "/api/observations-service/observations/observation_list-post",
            "list_method": "POST",
        },
        "actionmanagements": {
            "list_endpoint": "/api/action-service/action-managements/actions_list-post",
            "list_method": "POST",
        },
        "auditsmanagements": {
            "list_endpoint": "/api/inspection-service/audits-managements/audit_service_list_post",
            "list_method": "POST",
        },
        "customercomplaints": {
            "list_endpoint": "/api/customer-service/customer-complaint-details/customerComplaint-detail_list_post",
            "list_method": "POST",
        },
        "hseplandetails": {
            "list_endpoint": "/api/hSEPlans-service/h-sEPlan-details/hSEPlan-detail_list?filterText=&recordNo=&organizationUnitId=&hSEPlanStartDateMin=&hSEPlanStartDateMax=&hSEPlanEndDateMin=&hSEPlanEndDateMax=&createdBy=&dateCreatedMin=&dateCreatedMax=&viewArchievedData=&skipCount=0&maxResultCount=15",
            "list_method": "GET",
        },
        "eightdreports": {
            "list_endpoint": "/api/nCR-service/eight-dReports/eightDReport_service_list-post",
            "list_method": "POST",
        },
        "hsemonthlystatistics": {
            "list_endpoint": "/api/incident-service/h-sEMonthly-statisticss/hse-Statistics_list_post",
            "list_method": "POST",
        },
        "nonconformances": {
            "list_endpoint": "/api/nCR-service/nonconformance-details/ncr_service_list_post",
            "list_method": "POST",
        },
    },
}


# Force POST for endpoints that require it regardless of what the Excel says.
APP_METHOD_OVERRIDES: dict[str, str] = {
    "nonconformances":      "POST",
    "non_conformance":      "POST",
    "mocnonconformances":   "POST",
    "moc_non_conformance":  "POST",
    "riskmanagements":      "POST",
    "risk_management":      "POST",
    "eightdreports":        "POST",
    "8d_reports":           "POST",
}


def _build_connection_string() -> str:
    cfg = _active_db_config
    if DB_TRUSTED_AUTH:
        return (
            f"DRIVER={DB_DRIVER};SERVER={cfg['server']};DATABASE={cfg['name']};"
            f"Trusted_Connection=yes;"
        )
    return (
        f"DRIVER={DB_DRIVER};SERVER={cfg['server']};DATABASE={cfg['name']};"
        f"UID={cfg['user']};PWD={cfg['password']};"
    )


def _connect_db():
    if pyodbc is None:
        raise ModuleNotFoundError(
            "pyodbc is required for SQL Server queries. Install: pip install pyodbc"
        )
    return pyodbc.connect(_build_connection_string(), timeout=30)


def _extract_count(row: Any, columns: list[str]) -> int:
    for idx, col in enumerate(columns):
        if str(col).lower() in {"count", "totalcount"}:
            value = row[idx]
            return 0 if value is None else int(value)
    if columns and row[0] is not None:
        return int(row[0])
    raise AssertionError(
        f"SQL query did not return a 'count' or 'totalCount' column; got columns={columns}"
    )


def _get_sql_count(tenant_name: str, app_name: str) -> int:
    sql_template = (
        TENANT_SPECIFIC_SQL_QUERIES.get(tenant_name, {}).get(app_name)
        or APP_SQL_QUERIES.get(app_name)
    )
    if not sql_template:
        raise AssertionError(f"No SQL query configured for application '{app_name}'")
    schema = f"{TENANT_SCHEMA_PREFIX}{tenant_name}"
    sql = sql_template.format(tenant=tenant_name, schema=schema)
    _log(tenant_name, f"{app_name}.sql", f"Executing: {sql}")
    with _connect_db() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        if row is None:
            raise AssertionError(f"SQL query returned no rows for '{app_name}'")
        columns = [d[0] for d in cursor.description]
        count = _extract_count(row, columns)
    _log(tenant_name, f"{app_name}.sql", f"sql count = {count}")
    return count


def _run_application_sql_verification(
    tenant_name: str,
    tenant: dict,
    token: str,
    app_name: str,
    app_cfg: dict,
) -> tuple[int, int, str]:
    tenant_ep_override = TENANT_SPECIFIC_ENDPOINTS.get(tenant_name, {}).get(app_name, {})
    list_endpoint = (
        tenant_ep_override.get("list_endpoint")
        or (app_cfg.get("list_endpoint") or "").strip()
    )
    if not list_endpoint:
        print(f"[{tenant_name}] [{app_name}] application endpoint not found in API sheet.")
        return -1, -1, "APPLICATION_NOT_FOUND"
    if app_name not in APP_SQL_QUERIES and not TENANT_SPECIFIC_SQL_QUERIES.get(tenant_name, {}).get(app_name):
        print(f"[{tenant_name}] [{app_name}] no SQL query configured; skipping comparison.")
        return -1, -1, "SQL_NOT_CONFIGURED"
    _log(tenant_name, app_name, "Starting application verification (SQL mode)")

    list_count = _call_count_url(
        tenant_name=tenant_name,
        tenant=tenant,
        token=token,
        endpoint_path=list_endpoint,
        preferred_method=(
            APP_METHOD_OVERRIDES.get(app_name)
            or tenant_ep_override.get("list_method")
            or app_cfg.get("list_method", "GET")
        ),
        endpoint_name=f"{app_name}.list",
    )

    try:
        sql_count = _get_sql_count(tenant_name, app_name)
    except Exception as exc:
        print(f"[{tenant_name}] [{app_name}] sql_count error: {exc}")
        return list_count, -1, "FAIL"

    diff = list_count - sql_count
    status = "PASS" if list_count == sql_count else "FAIL"
    print(
        f"[{tenant_name}] [{app_name}] compare -> list={list_count}, "
        f"sql={sql_count}, difference={diff}, status={status}"
    )
    if status == "PASS":
        _log(tenant_name, app_name, "Application verification passed")
    else:
        _log(tenant_name, app_name, "Application verification failed (count mismatch)")
    return list_count, sql_count, status


def _run_tenant_sql_verification(tenant_name: str, tenant: dict) -> list[dict[str, Any]]:
    _log(tenant_name, "tenant", "Starting tenant verification (SQL mode)")
    token = get_token(tenant)
    applications = tenant.get("api_config", {}).get("applications", {})
    if not applications:
        raise AssertionError(
            f"[{tenant_name}] No applications configured under api_config.applications."
        )

    result_rows: list[dict[str, Any]] = []
    for app_name, app_cfg in applications.items():
        list_endpoint = (app_cfg.get("list_endpoint") or "").strip()
        has_tenant_override = bool(
            TENANT_SPECIFIC_ENDPOINTS.get(tenant_name, {}).get(app_name, {}).get("list_endpoint")
        )
        if not list_endpoint and not has_tenant_override:
            result_rows.append(
                {
                    "application": app_name,
                    "list_count": -1,
                    "sql_count": -1,
                    "difference": 0,
                    "status": "APPLICATION_NOT_FOUND",
                }
            )
            print(f"[{tenant_name}] [{app_name}] final_status=APPLICATION_NOT_FOUND")
            continue

        try:
            list_count, sql_count, app_status = _run_application_sql_verification(
                tenant_name, tenant, token, app_name, app_cfg,
            )
            result_rows.append(
                {
                    "application": app_name,
                    "list_count": list_count,
                    "sql_count": sql_count,
                    "difference": (list_count - sql_count) if sql_count >= 0 else 0,
                    "status": app_status,
                }
            )
            print(f"[{tenant_name}] [{app_name}] final_status={app_status}")
        except Exception as exc:
            error_msg = str(exc)
            result_rows.append(
                {
                    "application": app_name,
                    "list_count": -1,
                    "sql_count": -1,
                    "difference": 0,
                    "status": "FAIL",
                    "error_detail": error_msg,
                }
            )
            print(f"[{tenant_name}] [{app_name}] final_status=FAIL ({error_msg})")
            continue

    passed = sum(1 for r in result_rows if r["status"] == "PASS")
    failed = sum(1 for r in result_rows if r["status"] == "FAIL")
    sql_errors = 0  # SQL_ERROR is now reported as FAIL
    not_configured = sum(1 for r in result_rows if r["status"] == "SQL_NOT_CONFIGURED")
    not_found = sum(1 for r in result_rows if r["status"] == "APPLICATION_NOT_FOUND")
    print(
        f"[{tenant_name}] overall -> passed={passed}, failed={failed}, "
        f"sql_error={sql_errors}, sql_not_configured={not_configured}, "
        f"app_not_found={not_found}"
    )
    print(f"[{tenant_name}] FINAL SUMMARY TABLE")
    print(f"[{tenant_name}] tenant | application | list_count | sql_count | difference | status")
    print(f"[{tenant_name}] " + "-" * 88)
    for row in result_rows:
        print(
            f"[{tenant_name}] {tenant_name} | {row['application']} | {row['list_count']} | "
            f"{row['sql_count']} | {row['difference']} | {row['status']}"
        )
    _log(tenant_name, "tenant", "Tenant verification complete")
    return result_rows


def _print_consolidated_summary(all_rows: list[dict[str, Any]]) -> None:
    print("[ALL] CONSOLIDATED SUMMARY TABLE")
    print("[ALL] tenant | application | list_count | sql_count | difference | status")
    print("[ALL] " + "-" * 88)
    for row in all_rows:
        print(
            f"[ALL] {row['tenant']} | {row['application']} | {row['list_count']} | "
            f"{row['sql_count']} | {row['difference']} | {row['status']}"
        )


def _write_summary_csv(all_rows: list[dict[str, Any]], path: str) -> None:
    import csv
    fieldnames = ["tenant", "application", "list_count", "sql_count", "difference", "status"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
    print(f"[ALL] Consolidated summary written to: {path}")


_STATUS_DESCRIPTIONS: dict[str, str] = {
    "FAIL": "API count does not match SQL count (or API/SQL error — see error_detail)",
    "SQL_NOT_CONFIGURED": "no SQL query mapped for this application",
    "APPLICATION_NOT_FOUND": "list endpoint not configured in API Excel",
}


def _print_issues_report(
    all_rows: list[dict[str, Any]],
    failed_tenants: list[tuple[str, str]],
    total_tenants: int,
) -> None:
    issues_by_tenant: dict[str, list[dict[str, Any]]] = {}
    for row in all_rows:
        if row["status"] != "PASS":
            issues_by_tenant.setdefault(row["tenant"], []).append(row)

    print()
    print("=" * 90)
    print(" ISSUES REPORT")
    print("=" * 90)

    if not issues_by_tenant and not failed_tenants:
        print(" No issues found - all tenants and applications passed.")
    else:
        # Tenant-level failures first (couldn't even run).
        for tenant_name, error_msg in failed_tenants:
            print()
            print(f" Tenant: {tenant_name}")
            print(f"   [TENANT FAILED] verification could not run for this tenant")
            print(f"                   reason: {error_msg}")

        # App-level issues for tenants that did execute.
        for tenant_name in sorted(issues_by_tenant.keys()):
            print()
            print(f" Tenant: {tenant_name}")
            for issue in issues_by_tenant[tenant_name]:
                status = issue["status"]
                app = issue["application"]
                list_c = issue["list_count"]
                sql_c = issue["sql_count"]
                diff = issue["difference"]
                description = _STATUS_DESCRIPTIONS.get(status, "")

                if status == "FAIL":
                    error_detail = issue.get("error_detail")
                    if error_detail:
                        detail = error_detail
                    else:
                        detail = f"list={list_c}, sql={sql_c}, difference={diff}"
                elif status == "SQL_NOT_CONFIGURED":
                    detail = f"list={list_c}, sql=N/A"
                elif status == "APPLICATION_NOT_FOUND":
                    detail = "no API endpoint"
                else:
                    detail = f"list={list_c}, sql={sql_c}"

                print(f"   [{status:<22}] {app:<28} {detail}")
                if description:
                    print(f"                            -> {description}")

    print()
    print("=" * 90)
    print(" EXECUTION SUMMARY")
    print("=" * 90)

    executed_tenants = total_tenants - len(failed_tenants)
    print(f" Total tenants attempted:    {total_tenants}")
    print(f" Tenants executed:           {executed_tenants}")
    print(f" Tenants failed (no run):    {len(failed_tenants)}")

    if all_rows:
        print()
        print(f" Total applications checked: {len(all_rows)}")
        for status_name in ("PASS", "FAIL", "SQL_NOT_CONFIGURED", "APPLICATION_NOT_FOUND"):
            count = sum(1 for r in all_rows if r["status"] == status_name)
            if count > 0:
                print(f"   {status_name:<25} {count}")

    print("=" * 90)
    if not issues_by_tenant and not failed_tenants:
        print(f" RESULT: All {total_tenants} tenant(s) executed successfully with no issues.")
    else:
        affected_tenants = set(issues_by_tenant.keys()) | {t for t, _ in failed_tenants}
        print(
            f" RESULT: {len(affected_tenants)} tenant(s) need attention "
            f"(see ISSUES REPORT above)."
        )
    print("=" * 90)


if __name__ == "__main__":
    import os
    from datetime import datetime

    print("Running SQL-based count verification (standalone)...")
    try:
        runtime_tenants = load_tenants_for_sql_mode()
        print("[config] Loaded tenants/applications from credentials + API endpoints Excel.")
    except Exception as exc:
        print(f"[config] Excel load failed ({exc}). Falling back to in-file TENANTS.")
        runtime_tenants = TENANTS
    if not runtime_tenants:
        raise AssertionError(
            "No tenants loaded. Check Excel files and tenant/application columns."
        )

    all_rows: list[dict[str, Any]] = []
    failed_tenants: list[tuple[str, str]] = []  # (tenant_name, error_message)
    for name, tenant_cfg in runtime_tenants.items():
        try:
            tenant_rows = _run_tenant_sql_verification(name, tenant_cfg)
        except Exception as exc:
            print(f"[{name}] [tenant] verification FAILED: {exc}")
            failed_tenants.append((name, str(exc)))
            continue
        for row in tenant_rows:
            all_rows.append(
                {
                    "tenant": name,
                    "application": row["application"],
                    "list_count": row["list_count"],
                    "sql_count": row["sql_count"],
                    "difference": row["difference"],
                    "status": row["status"],
                    "error_detail": row.get("error_detail", ""),
                }
            )

    _print_consolidated_summary(all_rows)

    output_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_path = os.path.join(output_dir, f"csv_file_summary_{timestamp}.csv")
    _write_summary_csv(all_rows, csv_path)

    _print_issues_report(all_rows, failed_tenants, total_tenants=len(runtime_tenants))
