from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final


# -----------------------------
# Runtime Settings
# -----------------------------
@dataclass(frozen=True)
class Settings:
    # REQUIRED by SEC: identify who you are + contact email
    user_agent: str = os.getenv(
        "SEC_USER_AGENT",
        "Iakov Vainshtein (Reichman University; academic research) iakov.vainshtein@post.runi.ac.il",
    )

    # Global request throttling
    max_rps: float = float(os.getenv("MAX_RPS", "3"))
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "20"))
    timeout_seconds: float = float(os.getenv("HTTP_TIMEOUT", "30"))

    # “latest report” constraint: how far before event_date the report end can be
    # (~5 months ≈ 150–160 days)
    max_report_age_days: int = int(os.getenv("MAX_REPORT_AGE_DAYS", "160"))

    # IO
    input_xlsx: str = os.getenv("INPUT_XLSX", "input.xlsx")
    input_sheet: str | None = os.getenv("INPUT_SHEET") or None
    limit_rows: int = int(os.getenv("LIMIT_ROWS", "0"))  # 0 = all
    out_xlsx: str = os.getenv("OUT_XLSX", "rx_solvency_snapshot.xlsx")
    log_path: str = os.getenv("LOG_PATH", "run.log")


# -----------------------------
# Forms / Periods
# -----------------------------
ANNUAL_FORMS: Final[set[str]] = {"10-K", "20-F", "40-F"}
QUARTERLY_FORMS: Final[set[str]] = {"10-Q"}
ALLOWED_FORMS_FOR_REPORT: Final[set[str]] = ANNUAL_FORMS | QUARTERLY_FORMS

# Some filers have fp=Q4 on annuals; treat FY as annual; we don’t *require* fp strictness,
# but we keep it to help avoid weird points.
ANNUAL_FP: Final[set[str]] = {"FY"}
QUARTERLY_FP: Final[set[str]] = {"Q1", "Q2", "Q3"}


# -----------------------------
# Tag candidates (fallbacks)
# -----------------------------
# Base values we want (each is a list of candidate us-gaap tags)
TAG_CASH = ["CashAndCashEquivalentsAtCarryingValue",
            "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"]
TAG_LIAB_TOTAL = ["Liabilities"]
TAG_LIAB_CUR = ["LiabilitiesCurrent"]
TAG_LIAB_NONCUR = ["LiabilitiesNoncurrent"]

TAG_ASSETS = ["Assets"]
TAG_ASSETS_CUR = ["AssetsCurrent"]

TAG_AR = ["AccountsReceivableNetCurrent", "AccountsReceivableNet"]
TAG_INV = ["InventoryNet"]

TAG_DEBT_CUR = ["DebtCurrent"]
TAG_DEBT_LT = ["LongTermDebtNoncurrent", "LongTermDebt"]

TAG_OI = ["OperatingIncomeLoss"]
TAG_INT = ["InterestExpense"]

TAG_OCF = ["NetCashProvidedByUsedInOperatingActivities",
           "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]

FLOW_ANCHOR_TAGS = [
    TAG_OI,
    TAG_OCF,
]

# For selecting the “best report end date”, use these anchors (coverage scoring)
ANCHOR_TAGS = [
    TAG_CASH,
    TAG_LIAB_TOTAL,
    TAG_ASSETS,
    TAG_LIAB_CUR,
    TAG_ASSETS_CUR,
    TAG_OI,
    TAG_INT,
    TAG_OCF,
    TAG_DEBT_CUR,
    TAG_DEBT_LT,
    TAG_AR,
    TAG_INV,
]