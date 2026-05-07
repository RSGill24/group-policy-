"""
============================================================
Planview Migration Pipeline — EA Request Edition v2
============================================================
Config  : planview_ea_request_config.json (same folder as script)
Run     : py planview_ea_request_pipeline_v2.py
============================================================
"""

import pandas as pd
import pyodbc
import sys
import os
import io
import re
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIG — loaded from JSON config file at runtime
# ============================================================

INPUT_FILE             = ""
INPUT_SHEET_EPICS      = ""
INPUT_SHEET_INITS      = ""
SQL_SERVER             = ""
SQL_DATABASE           = ""
OUTPUT_FOLDER          = ""
EMAIL_ENABLED          = False
EMAIL_TO               = []
EMAIL_CC               = []
EMAIL_SUBJECT          = ""

# Rule assumptions — overrideable from config
BC_BLANK_EQUALS_NO         = True
NRB_BLANK_EQUALS_DISC_OTHER = True
NRB_FIELD                  = "L1 Net Recurring Benefits ($, annualized)-P&L/Hard"
NRB_THRESHOLD_M            = 10

# Output segment codes — aligned to new rules file Flow Type values
SEG = {
    "init_bc":       "TE-NonDisc-BusinessContinuity",
    "init_stopwork": "TE-Disc-StopWork-HOLD",
    "init_strat_inv":"TE-Disc-TransformationalInvestment",
    "init_disc_other":"TE-Disc-Other",
    "init_bo":       "TE-BusinessDemandMgmt",
    "init_pc":       "PC-StrategicProgram-PilotSandbox",
    "init_pending":  "PENDING-Stage-Lifecycle-Undefined",
    "init_excluded": "EXCLUDED-Status",
    "lcm":           "LCM-NonDisc-RunTheBusiness",
    "le_strat":      "LE-Disc-TransformationalInvestment",
    "le_stopwork":   "LE-Disc-StopWork-HOLD",
    "le_disc":       "LE-Disc-Other",
    "epic_pending":  "PENDING-Stage-Lifecycle-Undefined",
    "epic_excluded": "EXCLUDED-Status",
}

# Flow Type values — exact from new rules file Target Flow column
FLOW = {
    "init_bc":       "Non-Discretionary – Business Continuity",
    "init_stopwork": "STOP WORK Discretionary – Transformational Investment",
    "init_strat_inv":"Discretionary – Transformational Investment",
    "init_disc_other":"Discretionary – Other",
    "init_bo":       "Business Demand Management",
    "init_pc":       "Strategic Program (Business + Tech) — Pilot Sandbox",
    "lcm":           "Non-Discretionary - Run the Business",
    "le_strat":      "Discretionary – Transformational Investment",
    "le_stopwork":   "STOP WORK Discretionary – Transformational Investment",
    "le_disc":       "Discretionary – Other",
}

SEPARATOR = "=" * 60

# Allowed Work Status values for migration (BR_STATUS_001)
# From VALUES_DropDowns_Mappings_2026 — Wbs20 column
ALLOWED_STATUSES = {"Active"}  # Initiative Lifecycle Status filter
# Epic Work Status exclusion — Approved/Not Started/In Progress are valid active states
# Only exclude: Cancelled, Rejected, Completed/Closed (per VALUES_DropDowns_Mappings_2026)
EPIC_EXCL_STATUSES = {"Cancelled", "Rejected", "Completed/Closed"}
_logger   = None

# FY25 expired ESI values — BR_TE_001 parsed conditions from new rules file:
# "ESI NOT CONTAINS Purple Chip AND ESI = Expired (FY25) OR NULL"
# 0-None, blank, and FY25 expired confirmed as Non-PC (treated as null)
FY25_EXPIRED_ESI = {
    "Network Optimization (FY25 ESI)",
    "Digital Intelligence (FY25 ESI)",
    "Europe - Operations (FY25 ESI)",
    "Digital Experience (FY25 ESI)",
}


# ────────────────────────────────────────────────────────────
# CONFIG LOADER
# ────────────────────────────────────────────────────────────
def load_config(config_path=None):
    global INPUT_FILE, INPUT_SHEET_EPICS, INPUT_SHEET_INITS
    global SQL_SERVER, SQL_DATABASE, OUTPUT_FOLDER
    global EMAIL_ENABLED, EMAIL_TO, EMAIL_CC, EMAIL_SUBJECT
    global BC_BLANK_EQUALS_NO, NRB_BLANK_EQUALS_DISC_OTHER
    global NRB_FIELD, NRB_THRESHOLD_M

    if config_path is None:
        config_path = Path(__file__).parent / "planview_ea_request_config.json"

    config_path = Path(config_path)
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)

    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Config file is not valid JSON: {e}")
        sys.exit(1)

    INPUT_FILE             = cfg["input"]["file"]
    INPUT_SHEET_EPICS      = cfg["input"]["sheet_epics"]
    INPUT_SHEET_INITS      = cfg["input"]["sheet_initiatives"]
    SQL_SERVER             = cfg["sql"]["server"]
    SQL_DATABASE           = cfg["sql"]["database"]
    OUTPUT_FOLDER          = cfg["output"]["folder"]
    EMAIL_ENABLED          = cfg.get("email", {}).get("enabled", False)
    EMAIL_TO               = cfg.get("email", {}).get("to", [])
    EMAIL_CC               = cfg.get("email", {}).get("cc", [])
    EMAIL_SUBJECT          = cfg.get("email", {}).get("subject", "Planview Migration EA Request Output")
    BC_BLANK_EQUALS_NO     = cfg.get("rules", {}).get("bc_blank_equals_no", True)
    NRB_BLANK_EQUALS_DISC_OTHER = cfg.get("rules", {}).get("nrb_blank_equals_disc_other", True)
    NRB_FIELD              = cfg.get("rules", {}).get("nrb_field",
                                "L1 Net Recurring Benefits ($, annualized)-P&L/Hard")
    NRB_THRESHOLD_M        = cfg.get("rules", {}).get("nrb_threshold_m", 10)

    # on_hold_migrates — Q4 from customer email (unanswered). Default: Active only.
    on_hold = cfg.get("rules", {}).get("on_hold_migrates", False)
    ALLOWED_STATUSES.clear()
    ALLOWED_STATUSES.add("Active")
    if on_hold:
        ALLOWED_STATUSES.add("On Hold")

    # fy25_expired_esi — update in config each fiscal year when ESIs expire
    fy25_list = cfg.get("rules", {}).get("fy25_expired_esi", [])
    if fy25_list:
        FY25_EXPIRED_ESI.clear()
        FY25_EXPIRED_ESI.update(fy25_list)

    print(f"Config loaded : {config_path}")
    print(f"  Input file         : {INPUT_FILE}")
    print(f"  Epics sheet        : {INPUT_SHEET_EPICS}")
    print(f"  Initiatives sheet  : {INPUT_SHEET_INITS}")
    print(f"  SQL                : {SQL_SERVER} / {SQL_DATABASE}")
    print(f"  Output             : {OUTPUT_FOLDER}")
    print(f"  NRB field          : {NRB_FIELD}")
    print(f"  BC blank = No      : {BC_BLANK_EQUALS_NO}")
    print(f"  NRB blank = DiscOther: {NRB_BLANK_EQUALS_DISC_OTHER}")
    print(f"  Email              : {'ENABLED' if EMAIL_ENABLED else 'DISABLED'}")
    return config_path


# ────────────────────────────────────────────────────────────
# LOGGING
# ────────────────────────────────────────────────────────────
def init_logger(ts):
    global _logger
    log_dir  = Path(OUTPUT_FOLDER)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ea_request_run_{ts}.log"

    _logger = logging.getLogger("ea_pipeline")
    _logger.setLevel(logging.DEBUG)
    _logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(message)s",
                                       datefmt="%Y-%m-%d %H:%M:%S"))
    _logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(ch)

    return log_file


def log(msg, indent=0):
    line = "  " * indent + msg
    if _logger:
        _logger.info(line)
    else:
        print(line)


def log_step(num, msg):
    line = f"\n[{num}] {msg}"
    if _logger:
        _logger.info(line)
    else:
        print(line)


# ────────────────────────────────────────────────────────────
# STEP 1 — Read both Excel sheets
# ────────────────────────────────────────────────────────────
def read_excel_data():
    log_step("1/6", "Reading EA Request Excel — both sheets...")
    path = Path(INPUT_FILE)

    if not path.exists():
        log(f"ERROR: File not found: {path}", 1)
        sys.exit(1)

    # ── Sheet 1 — Epics ──────────────────────────────────────
    try:
        df_epics = pd.read_excel(path, sheet_name=INPUT_SHEET_EPICS,
                                  dtype=str, header=0)
    except Exception as e:
        log(f"ERROR reading Epics sheet '{INPUT_SHEET_EPICS}': {e}", 1)
        sys.exit(1)

    df_epics = df_epics.fillna("")
    if "Work ID #" in df_epics.columns:
        df_epics = df_epics[
            df_epics["Work ID #"].str.strip() != ""
        ].reset_index(drop=True)

    epic_required = ["Work ID #", "Work Type", "Work Status", "T-Shirt Size", "Stage"]
    epic_missing  = [c for c in epic_required if c not in df_epics.columns]
    if epic_missing:
        log(f"ERROR: Epics sheet missing columns: {epic_missing}", 1)
        log(f"Actual columns: {list(df_epics.columns)}", 1)
        sys.exit(1)

    log(f"Epics sheet  : '{INPUT_SHEET_EPICS}' — {len(df_epics):,} rows, {len(df_epics.columns)} cols", 1)

    # ── Sheet 2 — Initiatives ─────────────────────────────────
    try:
        df_inits = pd.read_excel(path, sheet_name=INPUT_SHEET_INITS,
                                  dtype=str, header=0)
    except Exception as e:
        log(f"ERROR reading Initiatives sheet '{INPUT_SHEET_INITS}': {e}", 1)
        sys.exit(1)

    df_inits = df_inits.fillna("")
    if "Strategy Seq. ID" in df_inits.columns:
        df_inits = df_inits[
            df_inits["Strategy Seq. ID"].str.strip() != ""
        ].reset_index(drop=True)

    init_required = ["Initiative Type", "Strategy Seq. ID", "T-Shirt Size",
                     "Stage", "Enterprise Strategic Initiative (ESI)"]
    init_missing  = [c for c in init_required if c not in df_inits.columns]
    if init_missing:
        log(f"ERROR: Initiatives sheet missing columns: {init_missing}", 1)
        log(f"Actual columns: {list(df_inits.columns)}", 1)
        sys.exit(1)

    log(f"Inits sheet  : '{INPUT_SHEET_INITS}' — {len(df_inits):,} rows, {len(df_inits.columns)} cols", 1)
    log(f"Total records: {len(df_epics) + len(df_inits):,}", 1)

    return df_epics, df_inits, path


# ────────────────────────────────────────────────────────────
# STEP 2 — Connect to SQL Server
# ────────────────────────────────────────────────────────────
def connect_sql():
    log_step("2/6", "Connecting to SQL Server...")
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"Trusted_Connection=yes;"
            f"Connection Timeout=30;"
        )
        conn.autocommit = True
        log(f"Server   : {SQL_SERVER}", 1)
        log(f"Database : {SQL_DATABASE}", 1)
        log("Status   : Connected", 1)
        return conn
    except pyodbc.Error as e:
        log("ERROR: Could not connect to SQL Server", 1)
        log(f"Detail : {e}", 1)
        sys.exit(1)


# ────────────────────────────────────────────────────────────
# STEP 3 — Load both sheets into permanent SQL input tables
# ────────────────────────────────────────────────────────────
def load_to_sql(conn, df, schema_name, table_name, ts):
    """Load a single dataframe into [{schema_name}].[{table_name}]."""
    cursor = conn.cursor()

    cursor.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}')
            EXEC('CREATE SCHEMA [{schema_name}]')
    """)
    cursor.execute(f"""
        IF OBJECT_ID('[{schema_name}].[{table_name}]') IS NOT NULL
            DROP TABLE [{schema_name}].[{table_name}]
    """)

    col_defs = ",\n            ".join(
        [f"[{col}] nvarchar(MAX)" for col in df.columns]
    )
    cursor.execute(f"""
        CREATE TABLE [{schema_name}].[{table_name}] (
            [Run_ID]         nvarchar(50)  DEFAULT '{ts}',
            [Load_Timestamp] datetime      DEFAULT GETDATE(),
            {col_defs}
        )
    """)

    col_names    = ", ".join([f"[{col}]" for col in df.columns])
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_sql   = (
        f"INSERT INTO [{schema_name}].[{table_name}] "
        f"([Run_ID], [Load_Timestamp], {col_names}) "
        f"VALUES ('{ts}', GETDATE(), {placeholders})"
    )

    df_clean = df.where(df.notna(), None)
    for col in df_clean.columns:
        if df_clean[col].dtype == object and hasattr(df_clean[col], "str"):
            df_clean[col] = (
                df_clean[col]
                .str.replace("\n", " ", regex=False)
                .str.replace("\r", " ", regex=False)
                .str.replace("|",  " ", regex=False)
                .str.replace('"',  "'", regex=False)
                .str.strip()
            )

    BATCH_SIZE = 500
    rows   = [tuple(r) for r in df_clean.itertuples(index=False, name=None)]
    loaded = 0
    conn.autocommit = False
    try:
        for i in range(0, len(rows), BATCH_SIZE):
            cursor.executemany(insert_sql, rows[i:i+BATCH_SIZE])
            loaded += len(rows[i:i+BATCH_SIZE])
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"ERROR: Insert failed at row {loaded}: {e}", 1)
        raise
    finally:
        conn.autocommit = True

    log(f"  [{schema_name}].[{table_name}] — {loaded:,} rows loaded", 1)
    return table_name


def create_input_schema_and_load(conn, df_epics, df_inits, input_path, ts):
    log_step("3/6", f"Loading both sheets into SQL Server input schema...")
    schema_name = f"input_{ts}"
    stem        = re.sub(r'[^A-Za-z0-9_]', '_', input_path.stem).strip('_')
    tbl_epics   = f"{stem}_Epics"
    tbl_inits   = f"{stem}_Initiatives"

    load_to_sql(conn, df_epics, schema_name, tbl_epics, ts)
    load_to_sql(conn, df_inits, schema_name, tbl_inits, ts)

    log(f"Input schema : [{schema_name}]", 1)
    log(f"  Epics table      : [{tbl_epics}] — {len(df_epics):,} rows", 1)
    log(f"  Initiatives table: [{tbl_inits}] — {len(df_inits):,} rows", 1)

    return schema_name, tbl_epics, tbl_inits


# ────────────────────────────────────────────────────────────
# STEP 4 — Business rules classification
# ────────────────────────────────────────────────────────────

def classify_initiatives(df_inits):
    """Apply all Initiative business rules in priority order.
    Returns a single classified DataFrame with Output_Segment column.
    """
    df = df_inits.copy()

    # ── Helper: NRB value as float ────────────────────────────
    def to_float(val):
        try:
            v = str(val).replace(',', '').replace('$', '').strip()
            return float(v) if v else None
        except Exception:
            return None

    # ── Classify each row ─────────────────────────────────────
    segments     = []
    migration_tracks = []
    demand_sub_types = []
    target_lifecycle = []

    lxl_sizes  = {'4: L', '5: XL'}
    sm_sizes   = {'1: XS', '2: S', '3: M'}

    for _, row in df.iterrows():
        init_type = str(row.get('Initiative Type', '')).strip()
        t_shirt   = str(row.get('T-Shirt Size', '')).strip()
        stage_raw = str(row.get('Stage', '')).strip()
        esi       = str(row.get('Enterprise Strategic Initiative (ESI)', '')).strip()
        pc_flag   = str(row.get('Purple Chip', '')).strip()
        bc_field  = str(row.get('Is this request vital to business continuity?', '')).strip()
        status    = str(row.get('Lifecycle Status', '')).strip()
        nrb_raw   = str(row.get(NRB_FIELD, '')).strip()
        nrb_val   = to_float(nrb_raw)

        # Stage letter extraction (e.g. "C: L1" → "L1", "G: L3" → "L3")
        stage_match = re.search(r'L(\d+)|SL(\d+)', stage_raw, re.IGNORECASE)
        if stage_match:
            stage_num = stage_raw.split(':')[-1].strip() if ':' in stage_raw else stage_raw
        else:
            stage_num = stage_raw

        # Stage/Lifecycle target (Column 1 — updated v2 step names from Lifecycle_Step_Index)
        stage_upper = stage_raw.upper()
        if 'L1' in stage_upper and 'SL' not in stage_upper:
            tgt_lifecycle = 'Initial Request Information'
            _pending_stage = False
        elif 'L2' in stage_upper and 'SL' not in stage_upper:
            tgt_lifecycle = 'Architecture Alignment'
            _pending_stage = False
        elif 'L3' in stage_upper and 'SL' not in stage_upper:
            tgt_lifecycle = 'Demand Bundle Decomp and Conceptual Architecture'
            _pending_stage = False
        elif 'L4' in stage_upper and 'SL' not in stage_upper:
            # L4 NOW DEFINED — Evaluate Outcome Achievement (Lifecycle_Step_Index v2)
            tgt_lifecycle = 'Evaluate Outcome Achievement'
            _pending_stage = False
        elif 'SL2' in stage_upper:
            tgt_lifecycle = 'PENDING — SL2 lifecycle step not yet defined in business rules'
            _pending_stage = True
        elif 'SL3' in stage_upper:
            tgt_lifecycle = 'PENDING — SL3 lifecycle step not yet defined in business rules'
            _pending_stage = True
        elif 'SL4' in stage_upper:
            tgt_lifecycle = 'PENDING — SL4 lifecycle step not yet defined in business rules'
            _pending_stage = True
        elif 'L5' in stage_upper:
            tgt_lifecycle = 'PENDING — L5 lifecycle step not yet defined in business rules'
            _pending_stage = True
        elif 'SL5' in stage_upper:
            tgt_lifecycle = 'PENDING — SL5 lifecycle step not yet defined in business rules'
            _pending_stage = True
        else:
            tgt_lifecycle = 'PENDING — Stage not recognised'
            _pending_stage = True
        target_lifecycle.append(tgt_lifecycle)

        # ── Classification priority ────────────────────────────

        # ── BR_STATUS_001 — Status exclusion (runs before all other rules) ─
        # Confirmed values from VALUES_DropDowns_Mappings_2026 Wbs20 column
        if status and status not in ALLOWED_STATUSES:
            segments.append(SEG['init_excluded'])
            migration_tracks.append(f'BR_STATUS_001 — Excluded: {status}')
            demand_sub_types.append('NOT MIGRATED — Status excluded')
            continue

        # Business Only — direct map (only 1 record but rule must run)
        if init_type == 'Business Only Initiative':
            segments.append(SEG['init_bo'])
            migration_tracks.append('BR_TE_006 — Business Only')
            demand_sub_types.append('Business Demand Management')
            continue

        # ── BR_TE_004 — Business Continuity (RUNS FIRST) ────────────
        # New rules file: explicitly covers L1,L2,L3,L4,L5,SL2,SL3,SL4,SL5
        # BC = Yes takes priority regardless of stage (fires before pending check)
        bc_yes = (bc_field == 'Yes') or (bc_field == '' and not BC_BLANK_EQUALS_NO)
        if bc_field == 'Yes':
            segments.append(SEG['init_bc'])
            migration_tracks.append('BR_TE_004 — Business Continuity')
            demand_sub_types.append('Non-Discretionary - Business Continuity')
            continue

        # Pending SL stages — L4 is now defined (Evaluate Outcome Achievement)
        # SL2/SL3/SL4/L5/SL5 still pending — no lifecycle step defined in rules file
        if _pending_stage:
            segments.append(SEG['init_pending'])
            migration_tracks.append('PENDING — Stage lifecycle step undefined')
            demand_sub_types.append('PENDING — Awaiting SL/L5 lifecycle step definition')
            continue

        # BR_PC_001 — Purple Chip (Pilot Sandbox path — not automated migration)
        if pc_flag == 'Purple Chip':
            segments.append(SEG['init_pc'])
            migration_tracks.append('BR_PC_001 — Purple Chip (Pilot Sandbox)')
            demand_sub_types.append('Strategic Program VB — Pilot Sandbox path')
            continue

        # Remaining rules apply to BwT Initiatives only
        if init_type != 'Business w/ Tech Initiative':
            segments.append(SEG['init_pending'])
            migration_tracks.append('REVIEW — Unknown Initiative Type')
            demand_sub_types.append('REVIEW REQUIRED — Unknown type')
            continue

        # Determine if ESI is active (not expired FY25, not None, not blank)
        esi_active = (
            esi != '' and
            esi != '0-None' and
            esi not in FY25_EXPIRED_ESI and
            pc_flag != 'Purple Chip'
        )

        # BR_TE_002 — Stop Work: T-shirt L/XL + NRB < $10M
        if t_shirt in lxl_sizes:
            if nrb_val is not None:
                if nrb_val < NRB_THRESHOLD_M:
                    segments.append(SEG['init_stopwork'])
                    migration_tracks.append('BR_TE_002 — Stop Work (NRB < $10M)')
                    demand_sub_types.append('Discretionary - Stop Work (Pending Finance/Triage)')
                else:
                    # NRB >= $10M — falls to BR_TE_001 Strategic Investment
                    if esi_active:
                        segments.append(SEG['init_strat_inv'])
                        migration_tracks.append('BR_TE_001 — Strategic Investment (ESI + L/XL + NRB >= $10M)')
                        demand_sub_types.append('Discretionary - Transformational Investment')
                    else:
                        # L/XL but ESI not active and NRB >= $10M — still Stop Work per BR_TE_002
                        segments.append(SEG['init_stopwork'])
                        migration_tracks.append('BR_TE_002 — Stop Work (L/XL, no active ESI)')
                        demand_sub_types.append('Discretionary - Stop Work (Pending Finance/Triage)')
            else:
                # NRB blank
                if NRB_BLANK_EQUALS_DISC_OTHER:
                    segments.append(SEG['init_disc_other'])
                    migration_tracks.append('BR_TE_003 — Disc Other (blank NRB assumed)')
                    demand_sub_types.append('Discretionary - Other (NRB blank — assumed default)')
                else:
                    segments.append(SEG['init_pending'])
                    migration_tracks.append('REVIEW — NRB missing for L/XL record')
                    demand_sub_types.append('REVIEW REQUIRED — NRB missing')
            continue

        # BR_TE_001 — Strategic Investment: ESI active + T-shirt L/XL (already handled above)
        # This catch covers edge cases where T-shirt is blank but ESI active
        if esi_active and t_shirt in lxl_sizes:
            segments.append(SEG['init_strat_inv'])
            migration_tracks.append('BR_TE_001 — Strategic Investment')
            demand_sub_types.append('Discretionary - Transformational Investment')
            continue

        # BR_TE_003 — Discretionary Other: ESI not active + T-shirt <= M or blank
        if t_shirt in sm_sizes or t_shirt == '':
            segments.append(SEG['init_disc_other'])
            migration_tracks.append('BR_TE_003 — Discretionary Other')
            demand_sub_types.append('Discretionary - Other')
            continue

        # Fallback
        segments.append(SEG['init_pending'])
        migration_tracks.append('REVIEW — No rule matched')
        demand_sub_types.append('REVIEW REQUIRED')

    df['Output_Segment']      = segments
    df['Migration_Track']     = migration_tracks
    df['Demand_Sub_Type']     = demand_sub_types
    df['Target_Lifecycle_Step'] = target_lifecycle
    df['Record_Type']         = 'Initiative'

    # ── Field renames ──────────────────────────────────────────
    # Field names are carried as-is from Current Prod source.
    init_rename = {}
    if init_rename:
        df = df.rename(columns={k: v for k, v in init_rename.items() if k in df.columns})

    # ── Drop columns — Migrate?=N-Calc only ───────────────────
    # are carried as-is — no explicit drop instruction in new rules file.
    drop_init = [
        "Adj Non-GAAP One-time Benefits (Latest Estimate)",
        "Adj Non-GAAP One-time Benefits (Plan)",
        "Adj Non-GAAP One-time Benefits (Working Version)",
        "Adj Non-GAAP One-time Costs (Latest Estimate)",
        "Adj Non-GAAP One-time Costs (Plan)",
        "Adj Non-GAAP One-time Costs (Working Version)",
        "Adjusted Net One time Benefits (Latest Estimate)",
        "Adjusted Net One time Benefits (Plan)",
        "Adjusted Net One time Benefits (Working Version)",
        "Adjusted Net Recurring Benefits (Latest Estimate)",
        "Adjusted Net Recurring Benefits (Plan)",
        "Adjusted Net Recurring Benefits (Working Version)",
        "Adjusted Non-GAAP OI (Latest Estimate)",
        "Adjusted Non-GAAP OI (Plan)",
        "Adjusted Non-GAAP OI (Working Version)",
        "Adjusted One time Benefits (Latest Estimate)",
        "Adjusted One time Benefits (Plan)",
        "Adjusted One time Benefits (Working Version)",
        "Adjusted One time Costs (Latest Estimate)",
        "Adjusted One time Costs (Plan)",
        "Adjusted One time Costs (Working Version)",
    ]
    df.drop(columns=[c for c in drop_init if c in df.columns], inplace=True)

    return df
def classify_epics(df_epics):
    """Apply all Epic business rules.
    Returns classified DataFrame with Output_Segment column.
    """
    df = df_epics.copy()

    lxl_sizes = {'4: L', '5: XL'}
    sm_sizes  = {'1: XS', '2: S', '3: M'}

    # Estimated Annualized Value Range proxy for NRB on Epics
    # 4: High = > $10M  → treat as NRB >= $10M → Strategic Investment
    # 3: Medium = $1M-$10M → treat as NRB < $10M → Stop Work
    # 1/2/blank → cannot confirm → default to Strategic Investment (conservative)
    HIGH_VALUE = {'4: High = > $10M'}
    MED_VALUE  = {'3: Medium = $1M < Value < $10M'}

    segments     = []
    migration_tracks = []
    demand_sub_types = []
    target_lifecycle = []
    epic_numbers = []
    epic_counter = [0]   # mutable for closure

    for _, row in df.iterrows():
        work_type  = str(row.get('Work Type', '')).strip()
        t_shirt    = str(row.get('T-Shirt Size', '')).strip()
        stage_raw  = str(row.get('Stage', '')).strip()
        val_range  = str(row.get('Estimated Annualized Value Range', '')).strip()
        work_status = str(row.get('Work Status', '')).strip()

        # Stage/Lifecycle target (Column 1 — v2 step names from Lifecycle_Step_Index)
        stage_upper = stage_raw.upper()
        if 'L2' in stage_upper and 'SL' not in stage_upper:
            tgt_lifecycle = 'Architecture Alignment'
            _pending = False
        elif 'L3' in stage_upper and 'SL' not in stage_upper:
            tgt_lifecycle = 'Demand Bundle Decomp and Conceptual Architecture'
            _pending = False
        elif 'L4' in stage_upper and 'SL' not in stage_upper:
            # L4 NOW DEFINED (Lifecycle_Step_Index v2)
            tgt_lifecycle = 'Evaluate Outcome Achievement'
            _pending = False
        elif 'SL3' in stage_upper:
            tgt_lifecycle = 'PENDING — SL3 lifecycle step not yet defined in business rules'
            _pending = True
        elif 'SL4' in stage_upper:
            tgt_lifecycle = 'PENDING — SL4 lifecycle step not yet defined in business rules'
            _pending = True
        elif 'SL2' in stage_upper:
            tgt_lifecycle = 'PENDING — SL2 lifecycle step not yet defined in business rules'
            _pending = True
        elif 'L5' in stage_upper:
            tgt_lifecycle = 'PENDING — L5 lifecycle step not yet defined in business rules'
            _pending = True
        elif 'SL5' in stage_upper:
            tgt_lifecycle = 'PENDING — SL5 lifecycle step not yet defined in business rules'
            _pending = True
        else:
            tgt_lifecycle = 'PENDING — Stage not recognised'
            _pending = True
        target_lifecycle.append(tgt_lifecycle)

        # Kanban Status for below-PPL
        if 'L3' in stage_upper:
            kanban_status = 'Solution Analysis'
        else:
            kanban_status = 'Intake/New'

        # Auto-increment Epic #
        epic_counter[0] += 1
        epic_numbers.append(epic_counter[0])

        # BR_STATUS_001 — Status exclusion for Epics
        # Epic Work Status values (Wbs20): Approved, Not Started, In Progress
        # are all valid active states — NOT exclusion triggers.
        # Exclude only: Cancelled, Rejected, Completed/Closed
        work_status = str(row.get('Work Status', '')).strip()
        if work_status in EPIC_EXCL_STATUSES:
            segments.append(SEG['epic_excluded'])
            migration_tracks.append(f'BR_STATUS_001 — Excluded: {work_status}')
            demand_sub_types.append('NOT MIGRATED — Status excluded')
            continue

        # ── Pending stage check — applies to BOTH LCM and LE ─────
        # L4 is now defined (Evaluate Outcome Achievement) — only SL3/SL4/SL2/L5/SL5 pending
        if _pending:
            segments.append(SEG['epic_pending'])
            migration_tracks.append('PENDING — Stage lifecycle step undefined')
            demand_sub_types.append('PENDING — Awaiting SL/L5 lifecycle step definition')
            continue

        # ── LCM direct map — BR_TE_LCM_001 ──────────────────────
        if work_type == 'Lifecycle Management Epic':
            segments.append(SEG['lcm'])
            migration_tracks.append('BR_TE_LCM_001 — LCM Run the Business')
            demand_sub_types.append('Non-Discretionary - Run the Business')
            continue

        # ── LE Epics ──────────────────────────────────────────────
        if work_type == 'Local Enhancement Epic':

            # Pending already handled above by _pending flag

            # BR_TE_LE_001 — T-shirt L/XL → Transformational Investment
            # But check if also Stop Work (BR_TE_LE_003) via value range proxy
            if t_shirt in lxl_sizes:
                if val_range in MED_VALUE:
                    # Proxy: Medium = $1M-$10M → treat as NRB < $10M → Stop Work
                    segments.append(SEG['le_stopwork'])
                    migration_tracks.append('BR_TE_LE_003 — Stop Work (L/XL, value range Medium <$10M proxy)')
                    demand_sub_types.append('Discretionary - Stop Work (Pending Finance/Triage)')
                else:
                    # High > $10M or Unknown → Strategic Investment
                    segments.append(SEG['le_strat'])
                    migration_tracks.append('BR_TE_LE_001 — LE L/XL Transformational Investment')
                    demand_sub_types.append('Discretionary - Transformational Investment')
                continue

            # BR_TE_LE_005 — T-shirt S/M/XS → Disc Other
            if t_shirt in sm_sizes:
                segments.append(SEG['le_disc'])
                migration_tracks.append('BR_TE_LE_005 — LE S/M/XS Disc Other')
                demand_sub_types.append('Discretionary - Other')
                continue

            # BR_BLANK_001 — blank T-shirt → Disc Other
            if t_shirt == '':
                segments.append(SEG['le_disc'])
                migration_tracks.append('BR_BLANK_001 — LE blank T-shirt Disc Other')
                demand_sub_types.append('Discretionary - Other (blank T-shirt default)')
                continue

        # Fallback
        segments.append(SEG['epic_pending'])
        migration_tracks.append('REVIEW — No rule matched')
        demand_sub_types.append('REVIEW REQUIRED')

    df['Output_Segment']        = segments
    df['Migration_Track']       = migration_tracks
    df['Demand_Sub_Type']       = demand_sub_types
    df['Target_Lifecycle_Step'] = target_lifecycle
    df['Epic_Number']           = epic_numbers
    df['Execution_Type']        = 'Demand Bundle Epic at PPL+2'
    df['Kanban_Status']         = df['Stage'].apply(
        lambda s: 'Solution Analysis' if ('L3' in str(s).upper() and 'SL' not in str(s).upper()) else 'Intake/New'
    )
    # EPG_Approval = Yes for L3 Epics per BR_TE_LE_002/004/006 and BR_TE_LCM_002
    df['EPG_Approval']           = df['Stage'].apply(
        lambda s: 'Yes' if ('L3' in str(s).upper() and 'SL' not in str(s).upper()) else ''
    )
    df['Record_Type']           = 'Epic'

    # ── Field renames ──────────────────────────────────────────
    # Field names are carried as-is from Current Prod source.
    epic_rename = {}
    if epic_rename:
        df = df.rename(columns={k: v for k, v in epic_rename.items() if k in df.columns})

    # ── Drop columns ───────────────────────────────────────────
    # All Epic columns carried as-is from source.
    drop_epic = []
    if drop_epic:
        df.drop(columns=[c for c in drop_epic if c in df.columns], inplace=True)

    return df


def run_transform(df_epics, df_inits):
    log_step("4/6", "Applying business rules to both sheets...")

    df_inits_classified = classify_initiatives(df_inits)
    df_epics_classified = classify_epics(df_epics)

    # ── Log initiative counts ─────────────────────────────────
    init_segs = df_inits_classified['Output_Segment'].value_counts()
    log(f"Initiatives ({len(df_inits_classified):,} total):", 1)
    log(f"  BR_TE_004 — Business Continuity     : {init_segs.get(SEG['init_bc'], 0):,}", 2)
    log(f"  BR_PC_001 — Purple Chip (Pilot SB)  : {init_segs.get(SEG['init_pc'], 0):,}", 2)
    log(f"  BR_TE_002 — Stop Work HOLD          : {init_segs.get(SEG['init_stopwork'], 0):,}", 2)
    log(f"  BR_TE_001 — Strategic Investment    : {init_segs.get(SEG['init_strat_inv'], 0):,}", 2)
    log(f"  BR_TE_003 — Discretionary Other     : {init_segs.get(SEG['init_disc_other'], 0):,}", 2)
    log(f"  BR_TE_006 — Business Only           : {init_segs.get(SEG['init_bo'], 0):,}", 2)
    log(f"  Pending Stage (SL2/SL3/SL4)         : {init_segs.get(SEG['init_pending'], 0):,}", 2)
    log(f"  Excluded (Status)                   : {init_segs.get(SEG['init_excluded'], 0):,}", 2)

    # ── Log epic counts ───────────────────────────────────────
    epic_segs = df_epics_classified['Output_Segment'].value_counts()
    log(f"Epics ({len(df_epics_classified):,} total):", 1)
    log(f"  BR_TE_LCM_001 — LCM Run the Business: {epic_segs.get(SEG['lcm'], 0):,}", 2)
    log(f"  BR_TE_LE_001  — LE Transformational  : {epic_segs.get(SEG['le_strat'], 0):,}", 2)
    log(f"  BR_TE_LE_005  — LE Disc Other        : {epic_segs.get(SEG['le_disc'], 0):,}", 2)
    log(f"  BR_TE_LE_003  — LE Stop Work HOLD    : {epic_segs.get(SEG['le_stopwork'], 0):,}", 2)
    log(f"  Pending Stage (SL3/SL4)              : {epic_segs.get(SEG['epic_pending'], 0):,}", 2)
    log(f"  Excluded (Status)                    : {epic_segs.get(SEG['epic_excluded'], 0):,}", 2)

    return df_inits_classified, df_epics_classified


# ────────────────────────────────────────────────────────────
# STEP 5 — Save output to SQL Server permanent tables
# ────────────────────────────────────────────────────────────
def save_output_to_sql(conn, df, schema_name, table_suffix, ts):
    """Save a single classified DataFrame to a permanent output table."""
    cursor = conn.cursor()
    cursor.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema_name}')
            EXEC('CREATE SCHEMA [{schema_name}]')
    """)
    cursor.execute(f"""
        IF OBJECT_ID('[{schema_name}].[{table_suffix}]') IS NOT NULL
            DROP TABLE [{schema_name}].[{table_suffix}]
    """)
    if df.empty:
        log(f"  [{schema_name}].[{table_suffix}] — 0 rows (skipped)", 1)
        return
    col_defs = ", ".join([f"[{c}] nvarchar(MAX)" for c in df.columns])
    cursor.execute(f"""
        CREATE TABLE [{schema_name}].[{table_suffix}] (
            [Run_ID]         nvarchar(50) DEFAULT '{ts}',
            [Save_Timestamp] datetime     DEFAULT GETDATE(),
            {col_defs}
        )
    """)
    col_names    = ", ".join([f"[{c}]" for c in df.columns])
    placeholders = ", ".join(["?" for _ in df.columns])
    insert_sql   = (
        f"INSERT INTO [{schema_name}].[{table_suffix}] "
        f"([Run_ID],[Save_Timestamp],{col_names}) "
        f"VALUES ('{ts}',GETDATE(),{placeholders})"
    )
    df_clean = df.where(df.notna(), None)
    rows = [tuple(r) for r in df_clean.itertuples(index=False, name=None)]
    conn.autocommit = False
    try:
        for i in range(0, len(rows), 500):
            cursor.executemany(insert_sql, rows[i:i+500])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True
    log(f"  [{schema_name}].[{table_suffix}] — {len(df):,} rows saved", 1)


FINAL_INIT_SEGS  = {'TE-NonDisc-BusinessContinuity','TE-Disc-TransformationalInvestment','TE-Disc-Other','TE-BusinessDemandMgmt'}
HOLD_INIT_SEGS   = {'TE-Disc-StopWork-HOLD','PC-StrategicProgram-PilotSandbox'}
REVIEW_INIT_SEGS = {'PENDING-Stage-Lifecycle-Undefined'}
EXCL_INIT_SEGS   = {'EXCLUDED-Status'}
FINAL_EPIC_SEGS  = {'LCM-NonDisc-RunTheBusiness','LE-Disc-TransformationalInvestment','LE-Disc-Other'}
HOLD_EPIC_SEGS   = {'LE-Disc-StopWork-HOLD'}
REVIEW_EPIC_SEGS = {'PENDING-Stage-Lifecycle-Undefined'}
EXCL_EPIC_SEGS   = {'EXCLUDED-Status'}


def create_output_schema_and_save(conn, df_inits_c, df_epics_c, stem, ts):
    log_step("5a/6", "Saving output tables to SQL Server...")
    output_schema = f"output_{ts}"

    # Split Initiatives into four output sets
    init_final    = df_inits_c[df_inits_c['Output_Segment'].isin(FINAL_INIT_SEGS)].copy()
    init_hold     = df_inits_c[df_inits_c['Output_Segment'].isin(HOLD_INIT_SEGS)].copy()
    init_review   = df_inits_c[df_inits_c['Output_Segment'].isin(REVIEW_INIT_SEGS)].copy()
    init_excl     = df_inits_c[df_inits_c['Output_Segment'].isin(EXCL_INIT_SEGS)].copy()

    # Split Epics into four output sets
    epic_final    = df_epics_c[df_epics_c['Output_Segment'].isin([
        SEG['lcm'], SEG['le_strat'],
        SEG['le_disc']])].copy()
    epic_hold     = df_epics_c[df_epics_c['Output_Segment'] == SEG['le_stopwork']].copy()
    epic_review   = df_epics_c[df_epics_c['Output_Segment'] == SEG['epic_pending']].copy()
    epic_excl     = pd.DataFrame()

    # Save Initiatives
    save_output_to_sql(conn, init_final,  output_schema, f"{stem}_Initiatives_Final_Output",    ts)
    save_output_to_sql(conn, init_hold,   output_schema, f"{stem}_Initiatives_StopWork_Hold",   ts)
    save_output_to_sql(conn, init_review, output_schema, f"{stem}_Initiatives_Review_Required", ts)

    # Save Epics
    save_output_to_sql(conn, epic_final,  output_schema, f"{stem}_Epics_Final_Output",    ts)
    save_output_to_sql(conn, epic_hold,   output_schema, f"{stem}_Epics_StopWork_Hold",   ts)
    save_output_to_sql(conn, epic_review, output_schema, f"{stem}_Epics_Review_Required", ts)

    return (output_schema,
            init_final, init_hold, init_review, init_excl,
            epic_final, epic_hold, epic_review, epic_excl)


# ────────────────────────────────────────────────────────────
# STEP 5b — Log run to run_history
# ────────────────────────────────────────────────────────────
def log_run_history(conn, ts, input_path, input_schema,
                    output_schema, init_final, init_hold, init_review,
                    epic_final, epic_hold, epic_review,
                    out_path, elapsed, status):
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'run_history')
            EXEC('CREATE SCHEMA [run_history]')
    """)
    # Drop and recreate if column schema changed — check for Pipeline_Version
    # which was added in v2. If missing, old table schema → drop and recreate.
    cursor.execute("""
        IF OBJECT_ID('run_history.Pipeline_Runs') IS NOT NULL
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM sys.columns
                WHERE object_id = OBJECT_ID('run_history.Pipeline_Runs')
                AND name = 'Pipeline_Version'
            )
            DROP TABLE run_history.Pipeline_Runs
        END
    """)
    cursor.execute("""
        IF OBJECT_ID('run_history.Pipeline_Runs') IS NULL
        CREATE TABLE run_history.Pipeline_Runs (
            Run_ID             nvarchar(50)  PRIMARY KEY,
            Run_Timestamp      datetime      DEFAULT GETDATE(),
            Pipeline_Name      nvarchar(200),
            Pipeline_Version   nvarchar(10),
            Input_File         nvarchar(500),
            Input_Schema       nvarchar(200),
            Output_Schema      nvarchar(200),
            Init_Final         int,
            Init_Hold          int,
            Init_Review        int,
            Epic_Final         int,
            Epic_Hold          int,
            Epic_Review        int,
            Output_File_Path   nvarchar(500),
            Runtime_Seconds    decimal(10,1),
            Run_Status         nvarchar(50)
        )
    """)
    cursor.execute("""
        INSERT INTO run_history.Pipeline_Runs (
            Run_ID, Pipeline_Name, Pipeline_Version, Input_File,
            Input_Schema, Output_Schema,
            Init_Final, Init_Hold, Init_Review,
            Epic_Final, Epic_Hold, Epic_Review,
            Output_File_Path, Runtime_Seconds, Run_Status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ts, 'EA Request Pipeline', 'v2', str(input_path),
        input_schema, output_schema,
        len(init_final), len(init_hold), len(init_review),
        len(epic_final), len(epic_hold), len(epic_review),
        str(out_path), elapsed, status
    ))
    conn.commit()
    log(f"Run logged : run_history.Pipeline_Runs — Run_ID: {ts}", 1)


# ────────────────────────────────────────────────────────────
# STEP 6a — Build Excel output (six sheets — three per data type)
# ────────────────────────────────────────────────────────────
def build_excel_bytes(init_final, init_hold, init_review, init_excl,
                      epic_final, epic_hold, epic_review, epic_excl):

    def safe_write(writer, df, sheet_name):
        # Drop internal tracking columns
        drop = ['Run_ID', 'Save_Timestamp']
        out  = df.drop(columns=[c for c in drop if c in df.columns])
        out.to_excel(writer, sheet_name=sheet_name, index=False)
        ws = writer.sheets[sheet_name]
        for col in ws.columns:
            max_len = max((len(str(c.value)) for c in col if c.value), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        safe_write(writer, init_final,  "Inits_Final_Output")
        safe_write(writer, init_hold,   "Inits_StopWork_Hold")
        safe_write(writer, init_review, "Inits_Review_Required")
        safe_write(writer, init_excl,   "Inits_Excluded")
        safe_write(writer, epic_final,  "Epics_Final_Output")
        safe_write(writer, epic_hold,   "Epics_StopWork_Hold")
        safe_write(writer, epic_review, "Epics_Review_Required")
        safe_write(writer, epic_excl,   "Epics_Excluded")

    log(f"  Inits_Final_Output    : {len(init_final):,}", 1)
    log(f"  Inits_StopWork_Hold   : {len(init_hold):,}  (inc. Purple Chip — Pilot Sandbox)", 1)
    log(f"  Inits_Review_Required : {len(init_review):,}  (SL2/SL3/SL4 stages undefined)", 1)
    log(f"  Inits_Excluded        : {len(init_excl):,}  (non-Active status)", 1)
    log(f"  Epics_Final_Output    : {len(epic_final):,}", 1)
    log(f"  Epics_StopWork_Hold   : {len(epic_hold):,}", 1)
    log(f"  Epics_Review_Required : {len(epic_review):,}  (SL3/SL4 stages undefined)", 1)
    log(f"  Epics_Excluded        : {len(epic_excl):,}  (non-Active status)", 1)

    return buffer.getvalue()


def save_to_folder(excel_bytes, ts, input_path):
    stem     = input_path.stem
    out_name = f"{stem}_Output_{ts}.xlsx"
    folder   = Path(OUTPUT_FOLDER)
    folder.mkdir(parents=True, exist_ok=True)
    out_path = folder / out_name
    with open(out_path, "wb") as f:
        f.write(excel_bytes)
    log(f"File saved : {out_path}", 1)
    return out_path


# ────────────────────────────────────────────────────────────
# STEP 6b — Email (disabled)
# ────────────────────────────────────────────────────────────
# To enable: install Outlook on the VM, set email.enabled=true in config,
# and uncomment the send_email function body.
#
# def send_email(...): ...


# ────────────────────────────────────────────────────────────
# CLEANUP
# ────────────────────────────────────────────────────────────
def cleanup(conn):
    conn.close()
    log("Cleanup  : SQL connection closed", 1)


# ────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Planview EA Request Pipeline")
    parser.add_argument("--config", default=None,
                        help="Path to JSON config file")
    args = parser.parse_args()

    load_config(args.config)

    start  = datetime.now()
    ts_run = start.strftime("%Y%m%d_%H%M%S")

    log_file = init_logger(ts_run)

    log(SEPARATOR)
    log("  Planview Migration Pipeline v2 — EA Request Edition")
    log("  Rules: Planview 2026 Platinum Consolidated Configuration and Data Migration Requirements")
    log(f"  Input  : {Path(INPUT_FILE).name}")
    log(f"  Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  Log    : {log_file}")
    log(SEPARATOR)
    log("  Rules: BusinessRules_DataMigration_Detail_breakdown_Latest.xlsx")
    log(f"  NRB field: {NRB_FIELD}")
    log(f"  BC blank = No: {BC_BLANK_EQUALS_NO} | NRB blank = DiscOther: {NRB_BLANK_EQUALS_DISC_OTHER}")
    log(SEPARATOR)

    # Step 1 — Read both sheets
    df_epics, df_inits, input_path = read_excel_data()

    # Step 2 — Connect SQL
    conn = connect_sql()

    # Step 3 — Load both sheets to SQL
    stem = re.sub(r'[^A-Za-z0-9_]', '_', input_path.stem).strip('_')
    input_schema, tbl_epics, tbl_inits = create_input_schema_and_load(
        conn, df_epics, df_inits, input_path, ts_run
    )

    # Step 4 — Apply business rules in Python
    df_inits_c, df_epics_c = run_transform(df_epics, df_inits)

    ts = ts_run

    # Step 5a — Save output to SQL Server
    log_step("5a/6", "Saving classified output to SQL Server...")
    (output_schema,
     init_final, init_hold, init_review, init_excl,
     epic_final, epic_hold, epic_review, epic_excl) = create_output_schema_and_save(
        conn, df_inits_c, df_epics_c, stem, ts
    )

    # Step 5b — Log run
    log_step("5b/6", "Logging run to run_history.Pipeline_Runs...")

    # Step 6a — Build Excel
    log_step("6a/6", "Building Excel output...")
    excel_bytes = build_excel_bytes(
        init_final, init_hold, init_review, init_excl,
        epic_final, epic_hold, epic_review, epic_excl
    )
    out_path = save_to_folder(excel_bytes, ts, input_path)

    elapsed = round((datetime.now() - start).total_seconds(), 1)

    log_run_history(
        conn, ts, input_path, input_schema, output_schema,
        init_final, init_hold, init_review,
        epic_final, epic_hold, epic_review,
        out_path, elapsed, "Completed"
    )

    # Step 6b — Email skipped
    log_step("6b/6", "Email skipped — Outlook not available on VM")

    cleanup(conn)

    log(f"\n{SEPARATOR}")
    log("  PIPELINE COMPLETE — EA REQUEST FILE")
    log(f"  Input schema   : [{input_schema}]")
    log(f"  Output schema  : [{output_schema}]")
    log(f"  Saved to       : {out_path}")
    log(f"  Log file       : {log_file}")
    log(f"  Email          : Disabled (set email.enabled=true in config)")
    log(f"")
    log(f"  INITIATIVES ({len(df_inits):,} total):")
    log(f"    Final Output  : {len(init_final):,} ready")
    log(f"    Stop Work HOLD: {len(init_hold):,} (inc. Purple Chip — Pilot Sandbox)")
    log(f"    Review / Pending: {len(init_review):,} (L4/SL stages undefined)")
    log(f"")
    log(f"  EPICS ({len(df_epics):,} total):")
    log(f"    Final Output  : {len(epic_final):,} ready")
    log(f"    Stop Work HOLD: {len(epic_hold):,}")
    log(f"    Review / Pending: {len(epic_review):,} (L4/SL3/SL4 stages undefined)")
    log(f"")
    log(f"  Runtime        : {elapsed}s")
    log(SEPARATOR)


if __name__ == "__main__":
    main()
