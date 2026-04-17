#!/usr/bin/env python3
"""
Unified Monthly Hours + Summary Reporting ETL

Outputs:
1. Project Report
2. Merged
3. Time Tracking Hours
4. Summary
5. Inhouse Report
6. Present in Team List & Activity
7. Not in Team List & Activity
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Sequence

import gspread
import pandas as pd
import requests
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from pandas.tseries.offsets import MonthEnd

# -----------------------------
# Logging
# -----------------------------

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOGGER = logging.getLogger("unified_monthly_reporting")


def setup_logging(level: str, log_file: Optional[str] = None) -> None:
    LOGGER.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    LOGGER.handlers.clear()

    formatter = logging.Formatter(LOG_FORMAT)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    LOGGER.addHandler(sh)

    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        LOGGER.addHandler(fh)


# -----------------------------
# Config
# -----------------------------

GOOGLE_SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

REPORT_NUMERIC_COLUMNS = [
    "Effective Work Hour",
    "Final Working Hour",
    "Client Billing Hours",
    "Annotation Time (Minutes)",
    "QA Time (Minutes)",
    "Crosscheck Time (Minutes)",
    "Meeting Time (Minutes)",
    "Project Study (Minutes)",
    "Resource Training (Minutes) - This section is for lead",
    "Q&A Group support (Minutes)",
    "Documentation (Minutes)",
    "Demo (Minutes)",
    "Break Time (Minutes)",
    "Server Downtime (Minutes)",
    "Free time (Minutes)",
    "Total Logged Hours",
]

REPORT_REQUIRED_COLUMNS = [
    "REPORT_MONTH",
    "QAI ID",
    "Full Name",
    "Resource Type",
    "Resource Allocation",
    *REPORT_NUMERIC_COLUMNS,
]

ATTENDANCE_REQUIRED_COLUMNS = ["QAI ID", "Joining Date", "Role"]
RESIGNED_REQUIRED_COLUMNS = ["ID", "LWD"]

EXCLUDED_RESOURCE_TYPES = {"REMOTE", "CLIENT", "VENDOR"}


@dataclass(frozen=True)
class Config:
    creds_file: str

    # input sheets
    project_sheet_key: str
    project_tab: str
    internal_sheet_key: str
    internal_tab: str
    delivery_sheet_key: str
    delivery_worksheet_name: str
    resigned_worksheet_name: str

    # clickup
    clickup_api_token: str
    clickup_list_id: str

    # output sheet
    output_sheet_key: str
    output_sheet_url: str

    # output tabs
    project_report_tab: str
    merged_tab: str
    time_tracking_tab: str
    summary_tab: str
    inhouse_tab: str
    present_teamlist_tab: str
    not_present_teamlist_tab: str

    report_year_filter: Optional[int]
    log_level: str = "INFO"
    log_file: Optional[str] = None


class ConfigurationError(Exception):
    pass


class DataValidationError(Exception):
    pass


def parse_optional_int(value: str | None) -> Optional[int]:
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    return int(value)


def get_env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    val = os.getenv(name)
    if val is None or val == "":
        val = default
    if required and not val:
        raise ConfigurationError(f"Missing required env var: {name}")
    return val


def _maybe_load_dotenv() -> None:
    if os.getenv("GITHUB_ACTIONS") == "true":
        return
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()
    LOGGER.info("Loaded environment from .env (local)")


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Unified Monthly Hours + Summary Reporting ETL")

    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    parser.add_argument("--log-file", default=os.getenv("LOG_FILE"))

    args = parser.parse_args()

    return Config(
        creds_file=get_env("GOOGLE_CREDS_FILE", "hip-lightning-451508-e5-c80ef62ddcea.json"),
        project_sheet_key=get_env("PROJECT_SHEET_KEY", required=True),
        project_tab=get_env("PROJECT_TAB", "For Internal Report"),
        internal_sheet_key=get_env("INTERNAL_LOG_SHEET_KEY", required=True),
        internal_tab=get_env("INTERNAL_LOG_TAB", "Form Responses 1"),
        delivery_sheet_key=get_env("DELIVERY_SHEET_KEY", "1YgIGvaN0NA6M2k5oHSJF-m0S8A5EhC6OkTCYfjT3bjw"),
        delivery_worksheet_name=get_env("DELIVERY_WORKSHEET_NAME", "Team List & Activity"),
        resigned_worksheet_name=get_env("RESIGNED_WORKSHEET_NAME", "Resign/Terminated"),
        clickup_api_token=get_env("CLICKUP_API_TOKEN", required=True),
        clickup_list_id=get_env("CLICKUP_LIST_ID", required=True),
        output_sheet_key=get_env("OUTPUT_SHEET_KEY", "1IikdQL_2hwlOrqm0JZOdQ_DxZkCa012X_CsJmycmsi0"),
        output_sheet_url=get_env("OUTPUT_SHEET_URL", "https://docs.google.com/spreadsheets/d/1IikdQL_2hwlOrqm0JZOdQ_DxZkCa012X_CsJmycmsi0/edit"),
        project_report_tab=get_env("PROJECT_REPORT_TAB", "Project Report"),
        merged_tab=get_env("MERGED_REPORT_TAB", "Merged"),
        time_tracking_tab=get_env("TIME_TRACKING_TAB", "Time Tracking Hours"),
        summary_tab=get_env("OUTPUT_SUMMARY_WORKSHEET", "Summary"),
        inhouse_tab=get_env("OUTPUT_INHOUSE_WORKSHEET", "Inhouse Report"),
        present_teamlist_tab=get_env("OUTPUT_PRESENT_TEAMLIST_WORKSHEET", "Present in Team List & Activity"),
        not_present_teamlist_tab=get_env("OUTPUT_NOT_PRESENT_TEAMLIST_WORKSHEET", "Not in Team List & Activity"),
        report_year_filter=parse_optional_int(os.getenv("REPORT_YEAR_FILTER", "").strip()),
        log_level=args.log_level,
        log_file=args.log_file,
    )


def validate_config(config: Config) -> None:
    if not os.path.isfile(config.creds_file):
        raise ConfigurationError(f"Credentials file not found: {config.creds_file}")

    required_values = {
        "project_sheet_key": config.project_sheet_key,
        "project_tab": config.project_tab,
        "internal_sheet_key": config.internal_sheet_key,
        "internal_tab": config.internal_tab,
        "delivery_sheet_key": config.delivery_sheet_key,
        "delivery_worksheet_name": config.delivery_worksheet_name,
        "resigned_worksheet_name": config.resigned_worksheet_name,
        "clickup_api_token": config.clickup_api_token,
        "clickup_list_id": config.clickup_list_id,
        "output_sheet_key": config.output_sheet_key,
        "output_sheet_url": config.output_sheet_url,
        "project_report_tab": config.project_report_tab,
        "merged_tab": config.merged_tab,
        "time_tracking_tab": config.time_tracking_tab,
        "summary_tab": config.summary_tab,
        "inhouse_tab": config.inhouse_tab,
        "present_teamlist_tab": config.present_teamlist_tab,
        "not_present_teamlist_tab": config.not_present_teamlist_tab,
    }
    missing = [k for k, v in required_values.items() if not str(v).strip()]
    if missing:
        raise ConfigurationError(f"Missing configuration values: {', '.join(missing)}")


# -----------------------------
# Helpers
# -----------------------------

def ensure_columns(df: pd.DataFrame, cols: List[str], fill_value="") -> pd.DataFrame:
    missing = [c for c in cols if c not in df.columns]
    for c in missing:
        df[c] = fill_value
    return df


def require_columns(df: pd.DataFrame, required_columns: Sequence[str], label: str) -> None:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise DataValidationError(f"Missing required columns in {label}: {', '.join(missing)}")


def accuracy_to_ratio(x) -> float:
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return 0.0
    s = s.replace("%", "")
    try:
        v = float(s)
    except Exception:
        return 0.0
    if v > 1:
        v /= 100
    if v < 0:
        v = 0
    if v > 1:
        v = 1
    return round(v, 4)


def normalize_date_str(value: object) -> str:
    if value is None:
        return ""
    value_str = str(value).strip()
    if not value_str:
        return ""
    parsed = pd.to_datetime(value_str, errors="coerce")
    if pd.isna(parsed):
        return value_str
    return parsed.strftime("%Y-%m-%d")


def parse_report_month(value: str) -> str | None:
    try:
        dt = datetime.strptime(str(value).strip(), "%Y-%m")
        return dt.strftime("%B - %Y")
    except ValueError:
        return None


def most_common_non_empty(series: pd.Series) -> str:
    cleaned = (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        .dropna()
    )
    if cleaned.empty:
        return ""
    modes = cleaned.mode()
    if modes.empty:
        return cleaned.iloc[0]
    return str(modes.iloc[0]).strip()


# -----------------------------
# Google Sheets
# -----------------------------

def build_gspread_client(creds_path: str) -> gspread.Client:
    credentials = ServiceAccountCredentials.from_json_keyfile_name(creds_path, GOOGLE_SCOPES)
    return gspread.authorize(credentials)


def fetch_sheet_df_by_key(client: gspread.Client, sheet_key: str, tab_name: str) -> pd.DataFrame:
    LOGGER.info("Fetching worksheet '%s' from sheet key %s", tab_name, sheet_key)
    ws = client.open_by_key(sheet_key).worksheet(tab_name)
    values = ws.get_all_values()

    if not values:
        LOGGER.warning("Worksheet '%s' is empty", tab_name)
        return pd.DataFrame()

    header = values[0]
    rows = values[1:] if len(values) > 1 else []
    df = pd.DataFrame(rows, columns=header)
    LOGGER.info("Loaded %s rows from '%s'", len(df), tab_name)
    return df


def ensure_worksheet_by_url(
    client: gspread.Client,
    sheet_url: str,
    worksheet_name: str,
    rows: int = 2000,
    cols: int = 40,
):
    spreadsheet = client.open_by_url(sheet_url)
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)
    return worksheet


def export_df_to_sheet(client: gspread.Client, sheet_url: str, tab_name: str, df: pd.DataFrame) -> None:
    LOGGER.info("Exporting %s rows x %s columns to '%s'", len(df), len(df.columns), tab_name)

    worksheet = ensure_worksheet_by_url(
        client,
        sheet_url,
        tab_name,
        rows=max(len(df) + 20, 2000),
        cols=max(len(df.columns) + 5, 40),
    )
    worksheet.clear()

    export_df = df.copy()
    export_df = export_df.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], "")
    for col in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[col]):
            export_df[col] = export_df[col].dt.strftime("%Y-%m-%d")

    if export_df.empty:
        if len(export_df.columns) > 0:
            worksheet.update([export_df.columns.tolist()])
        else:
            worksheet.update([[]])
    else:
        set_with_dataframe(worksheet, export_df)

    LOGGER.info("Upload complete for '%s'", tab_name)


# -----------------------------
# ClickUp
# -----------------------------

class ClickUpIndustryFetcher:
    INDUSTRY_FIELD_ID = "bb224045-37ed-44b6-8204-dc42f32a44cd"

    def __init__(self, api_token: str, list_id: str):
        self.api_token = api_token
        self.list_id = list_id
        self.headers = {
            "Authorization": self.api_token,
            "Content-Type": "application/json",
        }
        self.industry_map: Dict[str, str] = {}
        self.industry_order: List[str] = []
        self.tasks: List[Dict] = []

    def fetch_dropdown_options(self) -> None:
        url = f"https://api.clickup.com/api/v2/list/{self.list_id}/field"
        r = requests.get(url, headers=self.headers, timeout=30)
        if r.status_code != 200:
            LOGGER.error("ClickUp field fetch failed | Status=%s", r.status_code)
            return

        for f in r.json().get("fields", []):
            if f.get("id") == self.INDUSTRY_FIELD_ID:
                opts = f.get("type_config", {}).get("options", [])
                self.industry_map = {str(o["id"]): o["name"] for o in opts}
                self.industry_order = [str(o["id"]) for o in opts]
                LOGGER.info("Industry dropdown loaded | Options=%s", len(self.industry_map))
                return

        LOGGER.warning("Industry dropdown field not found in ClickUp")

    def fetch_tasks(self, limit: int = 100) -> None:
        page = 0
        while True:
            url = f"https://api.clickup.com/api/v2/list/{self.list_id}/task"
            params = {"page": page, "limit": limit, "include_closed": True, "include_archived": True}
            r = requests.get(url, headers=self.headers, params=params, timeout=30)

            if r.status_code != 200:
                LOGGER.error("Task fetch failed | Page=%s | Status=%s", page, r.status_code)
                break

            tasks = r.json().get("tasks", [])
            self.tasks.extend(tasks)

            if len(tasks) < limit:
                break
            page += 1

        LOGGER.info("ClickUp tasks fetched | Count=%s", len(self.tasks))

    def _extract_dropdown_value(self, field: Dict) -> str:
        val = field.get("value")

        if isinstance(val, dict):
            if "name" in val and val["name"]:
                return str(val["name"])
            if "id" in val and str(val["id"]) in self.industry_map:
                return self.industry_map[str(val["id"])]

        if isinstance(val, str) and val in self.industry_map:
            return self.industry_map[val]

        if isinstance(val, int):
            if 0 <= val < len(self.industry_order):
                opt_id = self.industry_order[val]
                return self.industry_map.get(opt_id, "Not Set")

        return "Not Set"

    def build_industry_dataframe(self) -> pd.DataFrame:
        records = []
        for t in self.tasks:
            industry = "Not Set"
            for f in t.get("custom_fields", []):
                if f.get("id") == self.INDUSTRY_FIELD_ID:
                    industry = self._extract_dropdown_value(f)
                    break
            records.append({"Project Batch": t.get("name", ""), "Industry Type": industry})

        df = pd.DataFrame(records)
        LOGGER.info("Industry mapping created | Rows=%s", len(df))
        return df


# -----------------------------
# First Flow: Project/Merged/Time Tracking
# -----------------------------

def build_project_report(
    project_data: pd.DataFrame,
    internal_log_data: pd.DataFrame,
    clickup_industry_df: pd.DataFrame,
) -> pd.DataFrame:
    project_cols = [
        "Project Batch", "Month", "Client Source", "START DATE", "COMPLETION DATE",
        "Tool TYPE (POLYGON, POLYLINE ETC)", "Industry Type", "Project", "DL",
        "Effective Work Hour", "Bonus", "Penalty", "Final Working Hour",
        "Accuracy", "Client Billing Hours", "Resource Type", "Type"
    ]

    df = project_data.copy()
    df = ensure_columns(df, project_cols, "")

    for col in ["Effective Work Hour", "Bonus", "Penalty", "Final Working Hour", "Accuracy", "Client Billing Hours"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Accuracy"] = df["Accuracy"].apply(accuracy_to_ratio)
    df["Resource Type"] = df["Resource Type"].astype(str).str.strip().str.upper()
    df["Project Batch"] = df["Project Batch"].astype(str).str.strip()

    df = df.drop(columns=["Industry Type"], errors="ignore")
    df = df.merge(clickup_industry_df, on="Project Batch", how="left")
    df["Industry Type"] = df["Industry Type"].fillna("Not Set")

    group_cols = [
        "Project Batch", "Month", "Client Source", "START DATE", "COMPLETION DATE",
        "Tool TYPE (POLYGON, POLYLINE ETC)", "Industry Type", "Project", "DL"
    ]

    project_pivot = (
        df.groupby(group_cols, dropna=False)
        .agg({
            "Effective Work Hour": "sum",
            "Bonus": "sum",
            "Penalty": "sum",
            "Final Working Hour": "sum",
            "Accuracy": "mean",
            "Client Billing Hours": "sum",
        })
        .reset_index()
        .rename(columns={
            "Effective Work Hour": "SUM of Effective Work Hour",
            "Bonus": "SUM of Bonus",
            "Penalty": "SUM of Penalty",
            "Final Working Hour": "SUM of Final Working Hour",
            "Accuracy": "AVERAGE of Accuracy",
            "Client Billing Hours": "SUM of Client Billing Hours",
        })
    )

    remote_summary = (
        df.groupby(["Project Batch", "Resource Type"], dropna=False)["Final Working Hour"]
        .sum()
        .reset_index()
    )

    remote_agg = (
        remote_summary[remote_summary["Resource Type"] == "REMOTE"]
        .groupby("Project Batch")["Final Working Hour"]
        .sum()
        .reset_index()
        .rename(columns={"Final Working Hour": "Remote Hours"})
    )

    inhouse_agg = (
        remote_summary[remote_summary["Resource Type"] != "REMOTE"]
        .groupby("Project Batch")["Final Working Hour"]
        .sum()
        .reset_index()
        .rename(columns={"Final Working Hour": "Inhouse Hours"})
    )

    remote_summary_fixed = pd.merge(remote_agg, inhouse_agg, on="Project Batch", how="outer").fillna(0)

    internal_cols = [
        "Project you worked on (Use Ctrl+F to search your required information)",
        "Annotation Time (Minutes)", "QA Time (Minutes)", "Crosscheck Time (Minutes)",
        "Meeting Time (Minutes)", "Project Study (Minutes)",
        "Resource Training (Minutes) - This section is for lead",
        "Q&A Group support (Minutes)", "Documentation (Minutes)", "Demo (Minutes)",
        "Break Time (Minutes)", "Server Downtime (Minutes)", "Free time (Minutes)",
    ]

    df_log = internal_log_data.copy()
    df_log = ensure_columns(df_log, internal_cols, 0)

    df_log["Project Batch"] = df_log[
        "Project you worked on (Use Ctrl+F to search your required information)"
    ].astype(str).str.strip()

    minute_cols = [
        "Annotation Time (Minutes)", "QA Time (Minutes)", "Crosscheck Time (Minutes)",
        "Meeting Time (Minutes)", "Project Study (Minutes)",
        "Resource Training (Minutes) - This section is for lead",
        "Q&A Group support (Minutes)", "Documentation (Minutes)", "Demo (Minutes)",
        "Break Time (Minutes)", "Server Downtime (Minutes)", "Free time (Minutes)",
    ]

    for c in minute_cols:
        df_log[c] = pd.to_numeric(df_log[c], errors="coerce").fillna(0)

    df_log["Total Logged Minutes"] = df_log[minute_cols].sum(axis=1)
    df_log["Total Logged Hours"] = df_log["Total Logged Minutes"] / 60
    df_log["Internal Logged Hours (Anno+QA)"] = (
        df_log["Annotation Time (Minutes)"] + df_log["QA Time (Minutes)"]
    ) / 60
    df_log["Other Hours (Excl. Anno+QA)"] = (
        df_log["Total Logged Hours"] - df_log["Internal Logged Hours (Anno+QA)"]
    )

    log_summary = (
        df_log.groupby("Project Batch")
        .agg({
            "Total Logged Hours": "sum",
            "Other Hours (Excl. Anno+QA)": "sum",
            "Internal Logged Hours (Anno+QA)": "sum",
        })
        .reset_index()
        .rename(columns={"Total Logged Hours": "Internal Logged Hours"})
    )

    df["Type"] = df["Type"].astype(str).str.upper()
    type_summary = (
        df.groupby(["Project Batch", "Type"])["Final Working Hour"]
        .sum()
        .reset_index()
    )
    type_pivot = (
        type_summary.pivot_table(index="Project Batch", columns="Type", values="Final Working Hour", fill_value=0)
        .reset_index()
    )
    type_pivot.columns.name = None
    type_pivot = type_pivot.rename(columns={
        "ANNOTATION": "Annotation Hours",
        "QC": "QC Hours",
        "TRACKING": "Tracking Hours",
    })

    final_df = (
        project_pivot.merge(remote_summary_fixed, on="Project Batch", how="left")
        .merge(log_summary, on="Project Batch", how="left")
        .merge(type_pivot, on="Project Batch", how="left")
        .fillna(0)
    )

    for col in final_df.columns:
        final_df[col] = final_df[col].apply(lambda x: x.strftime("%Y-%m-%d") if hasattr(x, "strftime") else x)

    final_df = final_df.replace([pd.NA, pd.NaT, float("inf"), float("-inf")], "").fillna("")
    LOGGER.info("Project report prepared | Rows=%s", len(final_df))
    return final_df


def build_merged_report(
    project_data: pd.DataFrame,
    internal_log_data: pd.DataFrame,
    clickup_industry_df: pd.DataFrame,
) -> pd.DataFrame:
    df_proj = project_data.copy()

    needed_cols = [
        "Project Batch", "Resource Type", "START DATE", "COMPLETION DATE",
        "Effective Work Hour", "Final Working Hour", "Client Billing Hours", "Bonus", "Penalty",
        "Accuracy", "Type", "QAI ID", "Full Name", "Client Source", "Industry Type",
        "Tool TYPE (POLYGON, POLYLINE ETC)", "Project", "Resource Allocation", "DL", "PDL",
    ]
    df_proj = ensure_columns(df_proj, needed_cols, "")

    df_proj["Project Batch"] = df_proj["Project Batch"].astype(str).str.strip()
    df_proj["Resource Type"] = df_proj["Resource Type"].astype(str).str.upper().str.strip()

    df_proj["START DATE"] = pd.to_datetime(df_proj["START DATE"], errors="coerce")
    df_proj["COMPLETION DATE"] = pd.to_datetime(df_proj["COMPLETION DATE"], errors="coerce")
    df_proj["COMPLETION DATE"] = df_proj["COMPLETION DATE"].fillna(df_proj["START DATE"])

    for c in ["Effective Work Hour", "Final Working Hour", "Client Billing Hours", "Bonus", "Penalty"]:
        df_proj[c] = pd.to_numeric(df_proj[c], errors="coerce").fillna(0)

    df_proj["Accuracy"] = df_proj["Accuracy"].apply(accuracy_to_ratio)

    df_proj = df_proj.drop(columns=["Industry Type"], errors="ignore")
    df_proj = df_proj.merge(clickup_industry_df, on="Project Batch", how="left")
    df_proj["Industry Type"] = df_proj["Industry Type"].fillna("Not Set")

    def split_hours(row):
        start, end = row["START DATE"], row["COMPLETION DATE"]
        if pd.isna(start) or pd.isna(end) or end < start:
            return []

        total_days = (end - start).days + 1
        rows = []
        cursor = start

        while cursor <= end:
            m_start = cursor.replace(day=1)
            m_end = m_start + MonthEnd(1)

            p_start = max(cursor, start)
            p_end = min(m_end, end)
            ratio = ((p_end - p_start).days + 1) / total_days

            r = row.copy()
            r["REPORT_MONTH"] = m_start.strftime("%Y-%m")
            r["Effective Work Hour"] *= ratio
            r["Final Working Hour"] *= ratio
            rows.append(r)

            cursor = m_end + pd.Timedelta(days=1)

        return rows

    expanded = []
    for _, r in df_proj.iterrows():
        expanded.extend(split_hours(r))

    df_proj = pd.DataFrame(expanded)
    if df_proj.empty:
        LOGGER.warning("No rows after month-splitting for merged report")
        return df_proj

    group_cols = [
        "REPORT_MONTH", "START DATE", "COMPLETION DATE", "Project Batch", "QAI ID", "Full Name",
        "Client Source", "Industry Type", "Tool TYPE (POLYGON, POLYLINE ETC)", "Project",
        "Resource Type", "Resource Allocation", "DL", "PDL",
    ]

    df_proj = (
        df_proj.groupby(group_cols, dropna=False)
        .agg({
            "Effective Work Hour": "sum",
            "Final Working Hour": "sum",
            "Bonus": "sum",
            "Penalty": "sum",
            "Accuracy": "mean",
            "Client Billing Hours": "sum",
        })
        .reset_index()
    )

    df_log = internal_log_data.copy()
    df_log = df_log.rename(columns={
        "Project you worked on (Use Ctrl+F to search your required information)": "Project Batch",
        "QAI ID (Use Ctrl+F to search your required information)": "QAI ID",
    })

    minute_cols = [
        "Annotation Time (Minutes)", "QA Time (Minutes)", "Crosscheck Time (Minutes)",
        "Meeting Time (Minutes)", "Project Study (Minutes)",
        "Resource Training (Minutes) - This section is for lead",
        "Q&A Group support (Minutes)", "Documentation (Minutes)", "Demo (Minutes)",
        "Break Time (Minutes)", "Server Downtime (Minutes)", "Free time (Minutes)",
    ]

    df_log = ensure_columns(df_log, minute_cols + ["Project Batch", "QAI ID"], 0)

    for c in minute_cols:
        df_log[c] = pd.to_numeric(df_log[c], errors="coerce").fillna(0) / 60

    df_log["Project Batch"] = df_log["Project Batch"].astype(str).str.strip()
    df_log["QAI ID"] = df_log["QAI ID"].astype(str).str.strip()
    df_log["Total Logged Hours"] = df_log[minute_cols].sum(axis=1)

    log_grouped = (
        df_log.groupby(["Project Batch", "QAI ID"], dropna=False)
        .agg({**{c: "sum" for c in minute_cols}, "Total Logged Hours": "sum"})
        .reset_index()
    )

    merged = df_proj.merge(log_grouped, on=["Project Batch", "QAI ID"], how="left")

    for c in merged.columns:
        if pd.api.types.is_datetime64_any_dtype(merged[c]):
            merged[c] = merged[c].dt.strftime("%Y-%m-%d")

    merged = merged.fillna("")
    LOGGER.info("Merged report prepared | Rows=%s", len(merged))
    return merged


def build_time_tracking_hours_report(internal_log_data: pd.DataFrame) -> pd.DataFrame:
    """
    Uses ONLY Internal Log data.
    Grouped by QAI ID + Month.
    Does NOT include Project name.
    """
    df_log = internal_log_data.copy()

    df_log = df_log.rename(columns={
        "QAI ID (Use Ctrl+F to search your required information)": "QAI ID",
    })

    minute_cols = [
        "Annotation Time (Minutes)",
        "QA Time (Minutes)",
        "Crosscheck Time (Minutes)",
        "Meeting Time (Minutes)",
        "Project Study (Minutes)",
        "Resource Training (Minutes) - This section is for lead",
        "Q&A Group support (Minutes)",
        "Documentation (Minutes)",
        "Demo (Minutes)",
        "Server Downtime (Minutes)",
        "Free time (Minutes)",
    ]

    possible_month_cols = [
        "Month",
        "Select Month",
        "Reporting Month",
        "Timestamp",
        "Date",
        "Submitted At",
    ]

    df_log = ensure_columns(df_log, ["QAI ID"] + minute_cols, 0)
    df_log["QAI ID"] = df_log["QAI ID"].astype(str).str.strip()

    for c in minute_cols:
        df_log[c] = pd.to_numeric(df_log[c], errors="coerce").fillna(0)

    df_log["Month"] = ""

    for col in possible_month_cols:
        if col in df_log.columns:
            if col in {"Timestamp", "Date", "Submitted At"}:
                parsed = pd.to_datetime(df_log[col], errors="coerce")
                candidate = parsed.dt.strftime("%B - %Y").fillna("")
            else:
                candidate = df_log[col].astype(str).str.strip()
                candidate = candidate.mask(candidate.str.lower().isin(["nan", "none"]), "")

            df_log["Month"] = df_log["Month"].where(df_log["Month"] != "", candidate)

    df_log["Production Hours"] = (
        df_log["Annotation Time (Minutes)"] + df_log["QA Time (Minutes)"]
    ) / 60

    df_log["Other Time Tracking Hours"] = (
        df_log["Crosscheck Time (Minutes)"]
        + df_log["Meeting Time (Minutes)"]
        + df_log["Project Study (Minutes)"]
        + df_log["Resource Training (Minutes) - This section is for lead"]
        + df_log["Q&A Group support (Minutes)"]
        + df_log["Documentation (Minutes)"]
        + df_log["Demo (Minutes)"]
        + df_log["Server Downtime (Minutes)"]
        + df_log["Free time (Minutes)"]
    ) / 60

    grouped = (
        df_log.groupby(["QAI ID", "Month"], dropna=False)
        .agg({
            "Annotation Time (Minutes)": "sum",
            "QA Time (Minutes)": "sum",
            "Crosscheck Time (Minutes)": "sum",
            "Meeting Time (Minutes)": "sum",
            "Project Study (Minutes)": "sum",
            "Resource Training (Minutes) - This section is for lead": "sum",
            "Q&A Group support (Minutes)": "sum",
            "Documentation (Minutes)": "sum",
            "Demo (Minutes)": "sum",
            "Server Downtime (Minutes)": "sum",
            "Free time (Minutes)": "sum",
            "Production Hours": "sum",
            "Other Time Tracking Hours": "sum",
        })
        .reset_index()
        .sort_values(["Month", "QAI ID"], ascending=[True, True])
    )

    LOGGER.info("Time tracking hours report prepared | Rows=%s", len(grouped))
    return grouped


# -----------------------------
# Second Flow: Summary and related tabs
# -----------------------------

def build_monthly_attendance_dataframe(attendance_df: pd.DataFrame) -> pd.DataFrame:
    require_columns(attendance_df, ATTENDANCE_REQUIRED_COLUMNS, "Team List & Activity worksheet")
    LOGGER.info("Building month-wise attendance dataframe")

    df = attendance_df.copy()
    df["QAI ID"] = df["QAI ID"].astype(str).str.strip()
    df["Joining Date"] = df["Joining Date"].apply(normalize_date_str)
    df["Role"] = df["Role"].astype(str).str.strip()

    date_columns: Dict[str, datetime] = {}
    for column in df.columns:
        try:
            date_columns[column] = datetime.strptime(column.strip(), "%m/%d/%Y")
        except ValueError:
            continue

    if not date_columns:
        raise DataValidationError("No attendance date columns found with MM/DD/YYYY format.")

    rows = []
    for _, row in df.iterrows():
        qai_id = str(row.get("QAI ID", "")).strip()
        if not qai_id:
            continue

        joining_date = str(row.get("Joining Date", "")).strip()
        role = str(row.get("Role", "")).strip()

        month_groups: Dict[str, List[str]] = {}
        for column, parsed_date in date_columns.items():
            month_label = parsed_date.strftime("%B - %Y")
            month_groups.setdefault(month_label, []).append(column)

        for month_label, cols in month_groups.items():
            active_days = 0
            for col in cols:
                val = str(row.get(col, "")).strip()
                if val and val.lower() != "unavailable":
                    active_days += 1

            rows.append(
                {
                    "QAI ID": qai_id,
                    "Month": month_label,
                    "Joining Date": joining_date,
                    "Role": role,
                    "Active Days": float(active_days),
                    "Active Hour": float(active_days * 8),
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(
            columns=["QAI ID", "Month", "Joining Date", "Role", "Active Days", "Active Hour"]
        )

    out = (
        out.groupby(["QAI ID", "Month"], as_index=False)
        .agg({
            "Joining Date": most_common_non_empty,
            "Role": most_common_non_empty,
            "Active Days": "max",
            "Active Hour": "max",
        })
    )
    return out


def build_summary_from_merged(report_df: pd.DataFrame, report_year_filter: Optional[int] = None) -> pd.DataFrame:
    require_columns(report_df, REPORT_REQUIRED_COLUMNS, "Merged worksheet")
    LOGGER.info(
        "Building month-wise summary directly from Merged%s",
        f" for year {report_year_filter}" if report_year_filter is not None else " for all years",
    )

    df = report_df[REPORT_REQUIRED_COLUMNS].copy()
    df["QAI ID"] = df["QAI ID"].astype(str).str.strip()
    df["Full Name"] = df["Full Name"].astype(str).str.strip().str.title()
    df["Resource Type"] = df["Resource Type"].astype(str).str.strip()
    df["Resource Allocation"] = df["Resource Allocation"].astype(str).str.strip()
    df["REPORT_MONTH"] = df["REPORT_MONTH"].astype(str).str.strip()

    for column in REPORT_NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)

    df = df[df["QAI ID"].ne("") & df["REPORT_MONTH"].ne("")].copy()
    df["Month"] = df["REPORT_MONTH"].apply(parse_report_month)
    df = df[df["Month"].notna()].copy()

    if report_year_filter is not None:
        df = df[df["Month"].str.endswith(str(report_year_filter))].copy()

    if df.empty:
        raise DataValidationError("No valid merged report rows found after filtering.")

    grouped = (
        df.groupby(["QAI ID", "Month"], as_index=False)
        .agg({
            "Full Name": most_common_non_empty,
            "Resource Type": most_common_non_empty,
            "Resource Allocation": most_common_non_empty,
            "Effective Work Hour": "sum",
        })
    )

    summary_df = grouped[
        [
            "QAI ID",
            "Full Name",
            "Resource Type",
            "Resource Allocation",
            "Month",
            "Effective Work Hour",
        ]
    ].copy()

    summary_df = summary_df.rename(columns={"Effective Work Hour": "Effective Hours"})
    summary_df["Effective Hours"] = pd.to_numeric(summary_df["Effective Hours"], errors="coerce").fillna(0)

    summary_df["Month_dt"] = pd.to_datetime(summary_df["Month"], format="%B - %Y", errors="coerce")
    summary_df = summary_df.sort_values(["QAI ID", "Month_dt", "Full Name"], kind="stable").drop(columns=["Month_dt"])
    summary_df = summary_df.reset_index(drop=True)
    return summary_df


def build_time_tracking_lookup(time_tracking_df: pd.DataFrame) -> pd.DataFrame:
    LOGGER.info("Building time tracking lookup from Time Tracking Hours")

    df = time_tracking_df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    rename_map = {}
    for col in df.columns:
        if col == "Production Hours":
            rename_map[col] = "total_prod_hours"
        elif col == "Other Time Tracking Hours":
            rename_map[col] = "total_other_time"

    df = df.rename(columns=rename_map)

    required_cols = ["QAI ID", "Month", "total_prod_hours", "total_other_time"]
    require_columns(df, required_cols, "Time Tracking Hours worksheet")

    df["QAI ID"] = df["QAI ID"].astype(str).str.strip()
    df["Month"] = df["Month"].astype(str).str.strip()
    df["total_prod_hours"] = pd.to_numeric(df["total_prod_hours"], errors="coerce").fillna(0)
    df["total_other_time"] = pd.to_numeric(df["total_other_time"], errors="coerce").fillna(0)

    df = df[df["QAI ID"].ne("") & df["Month"].ne("")].copy()

    df = (
        df.groupby(["QAI ID", "Month"], as_index=False)
        .agg({
            "total_prod_hours": "sum",
            "total_other_time": "sum",
        })
    )
    return df


def attach_time_tracking_to_summary(summary_df: pd.DataFrame, time_tracking_lookup_df: pd.DataFrame) -> pd.DataFrame:
    LOGGER.info("Attaching time tracking hours to summary")

    out = summary_df.merge(time_tracking_lookup_df, on=["QAI ID", "Month"], how="left")
    out["total_prod_hours"] = pd.to_numeric(out["total_prod_hours"], errors="coerce").fillna(0)
    out["total_other_time"] = pd.to_numeric(out["total_other_time"], errors="coerce").fillna(0)
    return out


def build_status_mapping(attendance_df: pd.DataFrame, resigned_df: pd.DataFrame) -> pd.DataFrame:
    require_columns(attendance_df, ["QAI ID"], "Team List & Activity worksheet")
    require_columns(resigned_df, RESIGNED_REQUIRED_COLUMNS, "Resigned/Terminated worksheet")

    active_df = attendance_df.copy()
    active_df["QAI ID"] = active_df["QAI ID"].astype(str).str.strip()
    active_df = active_df[active_df["QAI ID"].ne("")].copy()
    active_df = active_df[["QAI ID"]].drop_duplicates()
    active_df["Status"] = "Active"
    active_df["LWD"] = ""

    resigned_clean = resigned_df.copy()
    resigned_clean["QAI ID"] = resigned_clean["ID"].astype(str).str.strip()
    resigned_clean["LWD"] = resigned_clean["LWD"].apply(normalize_date_str)
    resigned_clean = resigned_clean[resigned_clean["QAI ID"].ne("")].copy()
    resigned_clean = resigned_clean[["QAI ID", "LWD"]].drop_duplicates(subset=["QAI ID"], keep="first")
    resigned_clean["Status"] = "Resigned/Terminated"

    status_df = pd.concat(
        [
            resigned_clean[["QAI ID", "Status", "LWD"]],
            active_df[["QAI ID", "Status", "LWD"]],
        ],
        ignore_index=True,
    )

    status_df["priority"] = status_df["Status"].map({"Active": 2, "Resigned/Terminated": 1}).fillna(0)
    status_df = status_df.sort_values(["QAI ID", "priority"], ascending=[True, False], kind="stable")
    status_df = status_df.drop_duplicates(subset=["QAI ID"], keep="first").drop(columns=["priority"])
    return status_df.reset_index(drop=True)


def enrich_summary(
    summary_df: pd.DataFrame,
    monthly_attendance_df: pd.DataFrame,
    status_df: pd.DataFrame,
) -> pd.DataFrame:
    LOGGER.info("Enriching summary with attendance, role, status, and LWD")

    out = summary_df.merge(monthly_attendance_df, on=["QAI ID", "Month"], how="left")
    out = out.merge(status_df, on="QAI ID", how="left")

    out["Joining Date"] = out["Joining Date"].fillna("")
    out["Role"] = out["Role"].fillna("")
    out["Active Days"] = pd.to_numeric(out["Active Days"], errors="coerce").fillna(0)
    out["Active Hour"] = pd.to_numeric(out["Active Hour"], errors="coerce").fillna(0)
    out["Status"] = out["Status"].fillna("Unknown")
    out["LWD"] = out["LWD"].fillna("")
    out["Effective Hours"] = pd.to_numeric(out["Effective Hours"], errors="coerce").fillna(0)
    out["total_prod_hours"] = pd.to_numeric(out["total_prod_hours"], errors="coerce").fillna(0)
    out["total_other_time"] = pd.to_numeric(out["total_other_time"], errors="coerce").fillna(0)

    out["Final Hour"] = out["Effective Hours"] + out["total_other_time"]
    out["Difference"] = out["Active Hour"] - out["Final Hour"]

    ordered_cols = [
        "QAI ID",
        "Full Name",
        "Joining Date",
        "Status",
        "LWD",
        "Role",
        "Resource Type",
        "Resource Allocation",
        "Month",
        "Effective Hours",
        "total_prod_hours",
        "total_other_time",
        "Final Hour",
        "Active Days",
        "Active Hour",
        "Difference",
    ]
    return out[ordered_cols].reset_index(drop=True)


def build_inhouse_report(summary_df: pd.DataFrame) -> pd.DataFrame:
    LOGGER.info("Building Inhouse Report from Summary")
    out = summary_df.copy()
    out["Resource Type_norm"] = out["Resource Type"].astype(str).str.upper().str.strip()
    out = out[~out["Resource Type_norm"].isin(EXCLUDED_RESOURCE_TYPES)].copy()
    out = out.drop(columns=["Resource Type_norm"]).reset_index(drop=True)
    return out


def build_teamlist_presence_outputs_from_summary(
    summary_df: pd.DataFrame,
    attendance_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    require_columns(summary_df, ["QAI ID", "Month"], "summary dataframe")
    require_columns(attendance_df, ["QAI ID"], "Team List & Activity worksheet")

    LOGGER.info("Building Present / Not Present tabs from Summary")

    attendance_ids = attendance_df["QAI ID"].astype(str).str.strip()
    attendance_ids = set(attendance_ids[attendance_ids.ne("")].tolist())

    roster_df = summary_df.copy()
    roster_df["QAI ID"] = roster_df["QAI ID"].astype(str).str.strip()
    roster_df["Month_dt"] = pd.to_datetime(roster_df["Month"], format="%B - %Y", errors="coerce")
    roster_df = roster_df.sort_values(["QAI ID", "Month_dt"], ascending=[True, False], kind="stable")

    latest_per_person = roster_df.drop_duplicates(subset=["QAI ID"], keep="first").copy()
    latest_per_person["Present in Team List & Activity"] = latest_per_person["QAI ID"].isin(attendance_ids)

    present_df = latest_per_person[latest_per_person["Present in Team List & Activity"]].copy()
    not_present_df = latest_per_person[~latest_per_person["Present in Team List & Activity"]].copy()

    not_present_df["Resource Type_norm"] = not_present_df["Resource Type"].astype(str).str.upper().str.strip()
    not_present_df = not_present_df[
        ~not_present_df["Resource Type_norm"].isin(EXCLUDED_RESOURCE_TYPES)
    ].copy()
    not_present_df = not_present_df.drop(columns=["Resource Type_norm"])

    present_df = present_df.drop(columns=["Month_dt"]).sort_values(
        ["Resource Type", "Full Name", "QAI ID"], kind="stable"
    ).reset_index(drop=True)

    not_present_df = not_present_df.drop(columns=["Month_dt"]).sort_values(
        ["Resource Type", "Full Name", "QAI ID"], kind="stable"
    ).reset_index(drop=True)

    return present_df, not_present_df


# -----------------------------
# Main Run
# -----------------------------

def run(config: Config) -> None:
    validate_config(config)
    client = build_gspread_client(config.creds_file)

    # Load inputs
    project_data = fetch_sheet_df_by_key(client, config.project_sheet_key, config.project_tab)
    internal_log_data = fetch_sheet_df_by_key(client, config.internal_sheet_key, config.internal_tab)
    attendance_df = fetch_sheet_df_by_key(client, config.delivery_sheet_key, config.delivery_worksheet_name)
    resigned_df = fetch_sheet_df_by_key(client, config.delivery_sheet_key, config.resigned_worksheet_name)

    # ClickUp
    fetcher = ClickUpIndustryFetcher(config.clickup_api_token, config.clickup_list_id)
    fetcher.fetch_dropdown_options()
    fetcher.fetch_tasks()
    clickup_industry_df = fetcher.build_industry_dataframe()

    # Build first flow
    project_report_df = build_project_report(project_data, internal_log_data, clickup_industry_df)
    merged_report_df = build_merged_report(project_data, internal_log_data, clickup_industry_df)
    time_tracking_hours_df = build_time_tracking_hours_report(internal_log_data)

    # Export first flow
    export_df_to_sheet(client, config.output_sheet_url, config.project_report_tab, project_report_df)
    export_df_to_sheet(client, config.output_sheet_url, config.merged_tab, merged_report_df)
    export_df_to_sheet(client, config.output_sheet_url, config.time_tracking_tab, time_tracking_hours_df)

    # Build second flow in memory from generated data
    monthly_attendance_df = build_monthly_attendance_dataframe(attendance_df)
    summary_base_df = build_summary_from_merged(merged_report_df, config.report_year_filter)
    time_tracking_lookup_df = build_time_tracking_lookup(time_tracking_hours_df)
    summary_base_df = attach_time_tracking_to_summary(summary_base_df, time_tracking_lookup_df)
    status_df = build_status_mapping(attendance_df, resigned_df)
    summary_df = enrich_summary(summary_base_df, monthly_attendance_df, status_df)

    inhouse_df = build_inhouse_report(summary_df)
    present_teamlist_df, not_present_teamlist_df = build_teamlist_presence_outputs_from_summary(
        summary_df,
        attendance_df,
    )

    # Export second flow
    export_df_to_sheet(client, config.output_sheet_url, config.summary_tab, summary_df)
    export_df_to_sheet(client, config.output_sheet_url, config.inhouse_tab, inhouse_df)
    export_df_to_sheet(client, config.output_sheet_url, config.present_teamlist_tab, present_teamlist_df)
    export_df_to_sheet(client, config.output_sheet_url, config.not_present_teamlist_tab, not_present_teamlist_df)

    LOGGER.info("Unified ETL completed successfully")


def main() -> int:
    _maybe_load_dotenv()
    config = parse_args()
    setup_logging(config.log_level, config.log_file)

    try:
        run(config)
        return 0
    except (ConfigurationError, DataValidationError) as exc:
        LOGGER.error("Validation failed: %s", exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Unhandled error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())