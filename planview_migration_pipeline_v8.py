"""
============================================================
Planview Migration Pipelinev4  — Customer Rules Edition
============================================================
How it works:
  1. Python reads the Excel Input_Data sheet and saves it
     as a temp CSV
  2. Python tells SQL Server to run BULK INSERT on that CSV
  3. SQL Server reads the file directly from disk — no Python
     looping, no row-by-row, no network overhead
  4. Runs the full 7-track transformation SQL derived from
     all three customer source files:
       File 1: BusinessRules_DataMigration_Detail_breakdown.xlsx
       File 2: DRAFT_Business_Rules_for_Data_Migration_(2).xlsx
       File 3: Copy_of_Non_PC_Classification_Base_Data_UNDER_REVIEW.xlsx
  5. Produces four result sets:
       - Final_Output       : in-scope, ready-to-migrate records
       - StopWork_Hold      : records needing Finance/Triage sign-off
       - Review_Required    : records where no rule matched
       - Excluded_Records   : status-based exclusions (Cancelled /
                              Rejected / Completed / On Hold)
  6. Builds the output Excel file in memory (never saved to disk)
     and emails it as an attachment to the configured recipients
  7. Deletes the temp CSV automatically

Usage:
  python planview_migration_pipeline_v4.py

One-time setup:
  pip install pandas pyodbc openpyxl
============================================================
"""

import pandas as pd
import pyodbc
import sys
import os
import io
from datetime import datetime
from pathlib import Path
import tempfile

# ============================================================
# CONFIG — UPDATE THESE BEFORE RUNNING
# ============================================================

INPUT_FILE    = r"E:\Inputfile\Planview_Migration_Prototype_v2_CustomerRules.xlsx"
INPUT_SHEET   = "Input_Data"
SQL_SERVER    = "DESKTOP-GLHDKO3"
SQL_DATABASE  = "PlanviewMigration"
STAGING_TABLE = "PV_Migration_Staging"

# Temp CSV — SQL Server reads this file directly from disk
# Must be a LOCAL path on the same machine as SQL Server
# This file is deleted automatically after the load completes
TEMP_CSV = r"C:\Demo\pv_migration_temp.csv"


# ── EMAIL CONFIG (Outlook Desktop) — UPDATE THESE BEFORE RUNNING ─────────
# Uses your already logged-in Outlook desktop app to send the email.
# No API key, no password, no SMTP — Outlook must be open when script runs.
# Sends FROM whatever account is logged in to Outlook on this machine.
# Works through any firewall, never goes to junk (sends via your own account).

# Recipients — update with actual email addresses
EMAIL_TO       = [
    "nilesh@invictadatacloud.com",        # update with actual recipient
]
EMAIL_CC       = []                     # optional: ["manager@yourcompany.com"]

EMAIL_SUBJECT  = "Planview Migration Output"   # subject line prefix
# ── END EMAIL CONFIG ─────────────────────────────────────────────────────

# Valid Output Segment values — used in validation step
# Any segment not in this set will be flagged as unexpected
VALID_SEGMENTS = {
    "PC-StrategicProgram-BwT",
    "PC-StrategicProgram-BO",
    "PC-StrategicProgram-Epic",
    "DT-ProgramVB",
    "DT-ProgramVB-Epic",
    "TE-NonDisc-BusinessContinuity",
    "TE-Disc-StrategicInvestment",
    "TE-Disc-StopWork-HOLD",
    "TE-Disc-Other",
    "TE-BusinessDemandMgmt",
    "LE-Disc-StrategicInvestment",
    "LE-Disc-StopWork-HOLD",
    "LE-Disc-Other",
    "LCM-NonDisc-RunTheBusiness",
    "EXCLUDED-Status",
    "EXCLUDED-OutOfScope-NonDrive",
    "REVIEW REQUIRED",
}

# ============================================================

SEPARATOR = "=" * 60

def log(msg, indent=0):
    print("  " * indent + msg)

def log_step(num, msg):
    print(f"\n[{num}] {msg}")


# ────────────────────────────────────────────────────────────
# STEP 1 — Read Excel sheet and save as temp CSV
# ────────────────────────────────────────────────────────────
def read_and_export_csv():
    log_step("1/6", "Reading Excel and saving to temp CSV...")
    path = Path(INPUT_FILE)

    if not path.exists():
        log(f"ERROR: File not found: {path}", 1)
        log("Check the INPUT_FILE path in the CONFIG section.", 1)
        sys.exit(1)

    try:
        # header=1 — row 0 is the banner, row 1 is the real column header row
        df = pd.read_excel(path, sheet_name=INPUT_SHEET, dtype=str, header=1)
    except Exception as e:
        log(f"ERROR reading sheet '{INPUT_SHEET}': {e}", 1)
        sys.exit(1)

    # Strip legend / blank rows appended below the data table.
    # Keep only rows where Strategy_Seq_ID is a non-empty, non-legend value.
    if "Strategy_Seq_ID" in df.columns:
        df = df[
            df["Strategy_Seq_ID"].notna() &
            (df["Strategy_Seq_ID"].str.strip() != "") &
            (df["Strategy_Seq_ID"].str.strip() != "LEGEND:")
        ].reset_index(drop=True)

    # NRB_M arrives as a string column (dtype=str).
    # Replace any non-numeric / blank / NaN values with empty string so
    # SQL BULK INSERT can cast them to DECIMAL(12,2) as NULL.
    if "NRB_M" in df.columns:
        def clean_nrb(val):
            if pd.isna(val) or str(val).strip() in ("", "nan", "None"):
                return ""
            try:
                float(val)      # confirm it is numeric
                return str(val)
            except ValueError:
                return ""
        df["NRB_M"] = df["NRB_M"].apply(clean_nrb)

    # Fill remaining NaN with empty string — SQL BULK INSERT treats empty as NULL
    df = df.fillna("")

    # Validate required columns are present
    required_cols = [
        "Strategy_Seq_ID", "Initiative_Name", "Entity_Type",
        "Initiative_Type", "Lifecycle_Status", "PC_Flag",
        "ESI_Flag", "T_Shirt_Size", "NRB_M", "BC_Flag",
        "DT_Flag", "Pilot_SB_VB_Flag", "Domain",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log(f"ERROR: Required column(s) missing from Input_Data:", 1)
        for m in missing:
            log(f"  - {m}", 2)
        log("Check that INPUT_SHEET matches 'Input_Data' tab exactly.", 1)
        sys.exit(1)

    # Save as pipe-delimited CSV to avoid issues with commas in text fields
    df.to_csv(TEMP_CSV, index=False, sep="|", encoding="utf-8-sig")

    log(f"Input    : {path.name}", 1)
    log(f"Sheet    : {INPUT_SHEET}", 1)
    log(f"Rows     : {len(df):,}", 1)
    log(f"Columns  : {len(df.columns)}", 1)
    log(f"Temp CSV : {TEMP_CSV}", 1)

    return df, path


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
        log("Troubleshooting:", 1)
        log("- Check SQL_SERVER name matches exactly what SSMS shows", 2)
        log("- Confirm SQL Server service is running", 2)
        log("- Confirm the database exists in SSMS", 2)
        sys.exit(1)


# ────────────────────────────────────────────────────────────
# STEP 3 — Create staging table and run SQL BULK INSERT
# ────────────────────────────────────────────────────────────
def bulk_insert(conn, df):
    log_step("3/6", f"Running SQL BULK INSERT — {len(df):,} rows...")
    cursor = conn.cursor()

    # Drop existing staging table from any previous run
    cursor.execute(f"""
        IF OBJECT_ID('{STAGING_TABLE}') IS NOT NULL
            DROP TABLE {STAGING_TABLE}
    """)

    # Build CREATE TABLE dynamically from the DataFrame columns.
    # All columns stored as nvarchar(500) — TRY_CAST in the transformation
    # SQL handles numeric conversion for NRB_M comparisons safely.
    col_defs = ",\n            ".join(
        [f"[{col}] nvarchar(500)" for col in df.columns]
    )

    cursor.execute(f"""
        CREATE TABLE {STAGING_TABLE} (
            {col_defs}
        )
    """)
    log(f"Table    : {STAGING_TABLE} created ({len(df.columns)} columns)", 1)

    # ── BULK INSERT — SQL Server reads CSV directly from disk ──
    # FIELDTERMINATOR : pipe | matches how we saved the CSV
    # ROWTERMINATOR   : \n for standard line endings
    # FIRSTROW = 2    : skip the header row
    # CODEPAGE        : 65001 = UTF-8 so special characters load correctly
    bulk_sql = f"""
        BULK INSERT {STAGING_TABLE}
        FROM '{TEMP_CSV}'
        WITH (
            FIELDTERMINATOR = '|',
            ROWTERMINATOR   = '\\n',
            FIRSTROW        = 2,
            CODEPAGE        = '65001',
            TABLOCK
        )
    """
    cursor.execute(bulk_sql)

    # Verify row count
    cursor.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
    db_count = cursor.fetchone()[0]
    log(f"Loaded   : {db_count:,} rows confirmed in SQL Server", 1)

    if db_count != len(df):
        log(f"WARNING: Mismatch — CSV had {len(df):,}, SQL has {db_count:,}", 1)

    return cursor


# ────────────────────────────────────────────────────────────
# STEP 4 — Run transformation SQL
# ────────────────────────────────────────────────────────────

TRANSFORM_SQL = f"""
WITH classified AS (
    SELECT
        [Strategy_Seq_ID],
        [Initiative_Name],
        [Entity_Type],
        [Lifecycle_Status],
        [Stage],
        [Domain]                          AS [Portfolio],
        [Initiative_Type],
        [PC_Flag],
        [ESI_Flag],
        [T_Shirt_Size],
        [NRB_M],
        [BC_Flag],
        [DT_Flag],
        [Pilot_SB_VB_Flag],
        [Est_Annual_Value],

        -- Work Status normalisation
        -- Source: File 2 Open Questions + File 3 Lifecycle_Status field values
        CASE [Lifecycle_Status]
            WHEN 'Active'    THEN 'Active'
            WHEN 'Completed' THEN 'Complete'
            WHEN 'On Hold'   THEN 'On Hold'
            WHEN 'Cancelled' THEN 'Cancelled'
            WHEN 'Rejected'  THEN 'Rejected'
            ELSE [Lifecycle_Status]
        END AS [Work_Status],

        -- Migration Track — priority order is critical
        -- Check status exclusion FIRST, then PC, then DT, then Tech-Enabled
        CASE

            -- ── BR_STATUS_001 (File 2, confirmed 4/8/26) ──────────────────
            -- Cancelled / Rejected / Completed / On Hold → exclude entirely
            WHEN [Lifecycle_Status] IN
                 ('Cancelled','Rejected','Completed','On Hold')
                THEN 'EXCLUDED-Status'

            -- ── BR_PC_001 / BR_PC_002 (File 1 — Strategic Programs PC) ───
            -- PC flag = Y AND already captured in Pilot SB VB
            -- Demand type: BwT → Strategic Program (Business + Tech)
            --              BO  → Strategic Program (Business Only)
            WHEN [PC_Flag] = 'Y'
             AND [Pilot_SB_VB_Flag] = 'Y'
             AND [Entity_Type] = 'Initiative'
             AND [Initiative_Type] = 'Business w/ Tech'
                THEN 'PC Strategic Programs'

            WHEN [PC_Flag] = 'Y'
             AND [Pilot_SB_VB_Flag] = 'Y'
             AND [Entity_Type] = 'Initiative'
             AND [Initiative_Type] = 'Business Only'
                THEN 'PC Strategic Programs'

            -- ── BR_PC_005 (File 1 — PC LE Epic, PM-designated) ───────────
            -- LE Epics in PC VBs only when PM explicitly confirms.
            -- No system flag — Pilot_SB_VB_Flag used as proxy.
            WHEN [PC_Flag] = 'Y'
             AND [Pilot_SB_VB_Flag] = 'Y'
             AND [Entity_Type] = 'Epic'
                THEN 'PC Strategic Programs'

            -- ── BR_ND_DT_001 / BR_ND_DT_003 (File 1 — DT Exception) ─────
            -- Digital Transformation: Pilot SB path, EPM/Marco Aletto owns
            WHEN [DT_Flag] = 'Y'
             AND [Entity_Type] = 'Initiative'
                THEN 'Non-Drive / DT Exception'

            WHEN [DT_Flag] = 'Y'
             AND [Entity_Type] = 'Epic'
                THEN 'Non-Drive / DT Exception'

            -- ── BR_TE_004 (File 1) ────────────────────────────────────────
            -- BwT Business Continuity — check BEFORE size/NRB rules
            -- Direct map regardless of T-shirt or NRB
            -- NOTE: Initiative_Type value in Excel is 'Business w/ Tech'
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Initiative'
             AND [Initiative_Type] = 'Business w/ Tech'
             AND [BC_Flag] = 'Y'
                THEN 'Tech-Enabled – Business Continuity'

            -- ── BR_TE_001 (File 1 + File 3 Definitions) ──────────────────
            -- BwT Strategic Investment: ESI=Y, T-shirt L or XL, NRB >= $10M
            -- File 3 Definitions: Strategic Investment = NRB >$10M AND XL/L
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Initiative'
             AND [Initiative_Type] = 'Business w/ Tech'
             AND [ESI_Flag] = 'Y'
             AND [T_Shirt_Size] IN ('L','XL')
             AND TRY_CAST([NRB_M] AS DECIMAL(12,2)) >= 10
                THEN 'Tech-Enabled – Strategic Investment'

            -- ── BR_TE_002 (File 1) ────────────────────────────────────────
            -- BwT Stop Work: T-shirt L or XL, NRB < $10M
            -- Finance/Triage sign-off required before migration
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Initiative'
             AND [Initiative_Type] = 'Business w/ Tech'
             AND [T_Shirt_Size] IN ('L','XL')
             AND TRY_CAST([NRB_M] AS DECIMAL(12,2)) < 10
                THEN 'Tech-Enabled – Stop Work (Pending Finance)'

            -- ── BR_TE_003 (File 1 + File 3) ──────────────────────────────
            -- BwT Discretionary Other: ESI=Y, T-shirt S, M, or XS
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Initiative'
             AND [Initiative_Type] = 'Business w/ Tech'
             AND [ESI_Flag] = 'Y'
             AND [T_Shirt_Size] IN ('S','M','XS')
                THEN 'Tech-Enabled – Discretionary Other'

            -- ── BR_TE_006 (File 1 + File 3 Definitions) ──────────────────
            -- Business Only → Business Demand Management
            -- File 3 Definitions: BO has NO T-shirt size criterion
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Initiative'
             AND [Initiative_Type] = 'Business Only'
                THEN 'Tech-Enabled – Business Demand Management'

            -- ── BR_TE_LE_001 (File 1 + File 3) ───────────────────────────
            -- LE Epic: T-shirt L or XL, NRB >= $10M → Strategic Investment
            -- Finance validation mandatory (File 1)
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Epic'
             AND [Initiative_Type] = 'Local Enhancement'
             AND [T_Shirt_Size] IN ('L','XL')
             AND TRY_CAST([NRB_M] AS DECIMAL(12,2)) >= 10
                THEN 'Tech-Enabled – LE Strategic Investment'

            -- ── BR_TE_LE_002 (File 1) ─────────────────────────────────────
            -- LE Epic: T-shirt L or XL, NRB < $10M → Stop Work
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Epic'
             AND [Initiative_Type] = 'Local Enhancement'
             AND [T_Shirt_Size] IN ('L','XL')
             AND TRY_CAST([NRB_M] AS DECIMAL(12,2)) < 10
                THEN 'Tech-Enabled – LE Stop Work (Pending Finance)'

            -- ── BR_TE_LE_003 (File 1 + File 3) ───────────────────────────
            -- LE Epic: T-shirt S, M, or XS → Discretionary Other
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Epic'
             AND [Initiative_Type] = 'Local Enhancement'
             AND [T_Shirt_Size] IN ('S','M','XS')
                THEN 'Tech-Enabled – LE Discretionary Other'

            -- ── BR_TE_LCM_005 (File 1 + File 3) ──────────────────────────
            -- LCM Epic → Non-Discretionary Run the Business
            -- File 3 Definitions: NRB not needed, any T-shirt size
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Entity_Type] = 'Epic'
             AND [Initiative_Type] = 'Lifecycle Management'
                THEN 'Tech-Enabled – LCM Run the Business'

            -- ── BR_BLANK_001 (File 2, confirmed 4/8/26) ──────────────────
            -- Blank NRB or blank T-shirt on BwT → Discretionary Other default
            WHEN [PC_Flag] <> 'Y'
             AND [DT_Flag] <> 'Y'
             AND [Initiative_Type] = 'Business w/ Tech'
             AND ([T_Shirt_Size] IS NULL
                  OR [T_Shirt_Size] = ''
                  OR [NRB_M] IS NULL
                  OR [NRB_M] = '')
                THEN 'Tech-Enabled – Discretionary Other'

            -- ── BR-ND-001 / BR-ND-005 (File 1) ───────────────────────────
            -- Standard Non-Drive (not DT): Out of Scope — hand-key in New Prod
            -- No automated migration; business owners hand-key VBs directly
            ELSE 'EXCLUDED-OutOfScope-NonDrive'

        END AS [Migration_Track]

    FROM {STAGING_TABLE}
),

-- Map Migration_Track to target Planview fields (File 1 Future State Flow
-- labels aligned with File 3 Demand_Subtypes column names)
final AS (
    SELECT
        [Strategy_Seq_ID]   AS [INITIATIVE_LEGACY_ID],
        [Initiative_Name],
        [Entity_Type],
        [Work_Status],

        -- Demand Type (from Initiative_Type — File 1 entity mapping)
        CASE [Initiative_Type]
            WHEN 'Business w/ Tech'      THEN 'Business w/Tech'
            WHEN 'Business Only'         THEN 'Business Only'
            WHEN 'Local Enhancement'     THEN 'Local Enhancement'
            WHEN 'Lifecycle Management'  THEN 'Lifecycle Management'
            ELSE [Initiative_Type]
        END AS [Demand_Type],

        -- Demand Sub-Type = Future State Flow
        -- Labels from File 1 (Future State Flow column) and
        -- File 3 (Demand_Subtypes Demand Sub-Type column)
        CASE [Migration_Track]
            WHEN 'PC Strategic Programs'
                THEN CASE [Initiative_Type]
                    WHEN 'Business w/ Tech' THEN 'Strategic Program (Business + Tech)'
                    WHEN 'Business Only'    THEN 'Strategic Program (Business Only)'
                    ELSE 'Strategic Program (Business + Tech)'
                END
            WHEN 'Non-Drive / DT Exception'
                THEN 'Program VB (Business + Tech)'
            WHEN 'Tech-Enabled – Business Continuity'
                THEN 'Non-Discretionary – Business Continuity'
            WHEN 'Tech-Enabled – Strategic Investment'
                THEN 'Discretionary – Strategic Investment'
            WHEN 'Tech-Enabled – Stop Work (Pending Finance)'
                THEN 'Discretionary – Strategic Inv. (Stop Work – Pending Finance)'
            WHEN 'Tech-Enabled – Discretionary Other'
                THEN 'Discretionary – Other'
            WHEN 'Tech-Enabled – Business Demand Management'
                THEN 'Business Demand Management'
            WHEN 'Tech-Enabled – LE Strategic Investment'
                THEN 'Discretionary – Strategic Investment'
            WHEN 'Tech-Enabled – LE Stop Work (Pending Finance)'
                THEN 'Discretionary – Strategic Inv. (Stop Work – Pending Finance)'
            WHEN 'Tech-Enabled – LE Discretionary Other'
                THEN 'Discretionary – Other'
            WHEN 'Tech-Enabled – LCM Run the Business'
                THEN 'Non-Discretionary – Run the Business'
            ELSE 'REVIEW REQUIRED'
        END AS [Demand_Sub_Type],

        [Portfolio],
        [Est_Annual_Value],

        -- Migration Source (Pilot SB vs Current Prod vs ESPM)
        -- File 1 data source decisions + File 2 ESPM confirmation
        CASE [Migration_Track]
            WHEN 'PC Strategic Programs'
                THEN 'Pilot Sandbox → New Prod'
            WHEN 'Non-Drive / DT Exception'
                THEN 'Pilot Sandbox → New Prod'
            WHEN 'Tech-Enabled – Business Demand Management'
                THEN 'Current Prod / ESPM Spreadsheets (TBC)'
            WHEN 'EXCLUDED-Status'
                THEN 'N/A – Excluded'
            WHEN 'EXCLUDED-OutOfScope-NonDrive'
                THEN 'N/A – Hand-key in New Prod'
            ELSE 'Current Prod → New Prod'
        END AS [Migration_Source],

        [Migration_Track],

        -- Output Segment — matches Scenario_Rules Output_Segment column
        CASE [Migration_Track]
            WHEN 'PC Strategic Programs'
                THEN CASE
                    WHEN [Entity_Type] = 'Epic' THEN 'PC-StrategicProgram-Epic'
                    WHEN [Initiative_Type] = 'Business Only'
                        THEN 'PC-StrategicProgram-BO'
                    ELSE 'PC-StrategicProgram-BwT'
                END
            WHEN 'Non-Drive / DT Exception'
                THEN CASE
                    WHEN [Entity_Type] = 'Epic' THEN 'DT-ProgramVB-Epic'
                    ELSE 'DT-ProgramVB'
                END
            WHEN 'Tech-Enabled – Business Continuity'
                THEN 'TE-NonDisc-BusinessContinuity'
            WHEN 'Tech-Enabled – Strategic Investment'
                THEN 'TE-Disc-StrategicInvestment'
            WHEN 'Tech-Enabled – Stop Work (Pending Finance)'
                THEN 'TE-Disc-StopWork-HOLD'
            WHEN 'Tech-Enabled – Discretionary Other'
                THEN 'TE-Disc-Other'
            WHEN 'Tech-Enabled – Business Demand Management'
                THEN 'TE-BusinessDemandMgmt'
            WHEN 'Tech-Enabled – LE Strategic Investment'
                THEN 'LE-Disc-StrategicInvestment'
            WHEN 'Tech-Enabled – LE Stop Work (Pending Finance)'
                THEN 'LE-Disc-StopWork-HOLD'
            WHEN 'Tech-Enabled – LE Discretionary Other'
                THEN 'LE-Disc-Other'
            WHEN 'Tech-Enabled – LCM Run the Business'
                THEN 'LCM-NonDisc-RunTheBusiness'
            WHEN 'EXCLUDED-Status'
                THEN 'EXCLUDED-Status'
            WHEN 'EXCLUDED-OutOfScope-NonDrive'
                THEN 'EXCLUDED-OutOfScope-NonDrive'
            ELSE 'REVIEW REQUIRED'
        END AS [Output_Segment],

        [Lifecycle_Status],
        [Stage],
        [PC_Flag],
        [ESI_Flag],
        [T_Shirt_Size],
        [NRB_M]

    FROM classified
)
SELECT * FROM final
ORDER BY [Migration_Track], [INITIATIVE_LEGACY_ID]
"""


def run_transform(conn):
    log_step("4/6", "Running transformation SQL...")

    all_df = pd.read_sql(TRANSFORM_SQL, conn)

    # ── Split into four result sets ────────────────────────────────────────────

    # 1. Final Output — in-scope, ready-to-migrate records
    final_df = all_df[
        ~all_df["Output_Segment"].isin([
            "EXCLUDED-Status",
            "EXCLUDED-OutOfScope-NonDrive",
            "REVIEW REQUIRED",
            "TE-Disc-StopWork-HOLD",
            "LE-Disc-StopWork-HOLD",
        ])
    ].reset_index(drop=True)

    # 2. Stop Work Hold — Finance/Triage sign-off needed (BR_TE_002 / BR_TE_LE_002)
    stopwork_df = all_df[
        all_df["Output_Segment"].isin([
            "TE-Disc-StopWork-HOLD",
            "LE-Disc-StopWork-HOLD",
        ])
    ].reset_index(drop=True)

    # 3. Review Required — no rule matched
    review_df = all_df[
        all_df["Output_Segment"] == "REVIEW REQUIRED"
    ].reset_index(drop=True)

    # 4. Excluded Records — status-based + out of scope
    excluded_df = all_df[
        all_df["Output_Segment"].isin([
            "EXCLUDED-Status",
            "EXCLUDED-OutOfScope-NonDrive",
        ])
    ].reset_index(drop=True)

    # ── Segment breakdown for final output ────────────────────────────────────
    seg_counts = final_df["Output_Segment"].value_counts()

    log(f"Total rows processed        : {len(all_df):,}", 1)
    log(f"", 1)
    log(f"Final Output (ready)        : {len(final_df):,}", 1)
    log(f"  PC-StrategicProgram-BwT   : {seg_counts.get('PC-StrategicProgram-BwT', 0):,}", 2)
    log(f"  PC-StrategicProgram-BO    : {seg_counts.get('PC-StrategicProgram-BO', 0):,}", 2)
    log(f"  PC-StrategicProgram-Epic  : {seg_counts.get('PC-StrategicProgram-Epic', 0):,}", 2)
    log(f"  DT-ProgramVB              : {seg_counts.get('DT-ProgramVB', 0):,}", 2)
    log(f"  DT-ProgramVB-Epic         : {seg_counts.get('DT-ProgramVB-Epic', 0):,}", 2)
    log(f"  TE-NonDisc-BusinessCont.  : {seg_counts.get('TE-NonDisc-BusinessContinuity', 0):,}", 2)
    log(f"  TE-Disc-StrategicInvest.  : {seg_counts.get('TE-Disc-StrategicInvestment', 0):,}", 2)
    log(f"  TE-Disc-Other             : {seg_counts.get('TE-Disc-Other', 0):,}", 2)
    log(f"  TE-BusinessDemandMgmt     : {seg_counts.get('TE-BusinessDemandMgmt', 0):,}", 2)
    log(f"  LE-Disc-StrategicInvest.  : {seg_counts.get('LE-Disc-StrategicInvestment', 0):,}", 2)
    log(f"  LE-Disc-Other             : {seg_counts.get('LE-Disc-Other', 0):,}", 2)
    log(f"  LCM-NonDisc-RunTheBusiness: {seg_counts.get('LCM-NonDisc-RunTheBusiness', 0):,}", 2)
    log(f"", 1)
    log(f"Stop Work HOLD (Finance)    : {len(stopwork_df):,}  ← awaiting Finance/Triage sign-off", 1)
    log(f"Review Required             : {len(review_df):,}  ← no rule matched, needs investigation", 1)
    log(f"Excluded (status-based)     : {excluded_df[excluded_df['Output_Segment']=='EXCLUDED-Status'].shape[0]:,}  ← BR_STATUS_001 (File 2)", 1)
    log(f"Excluded (out of scope)     : {excluded_df[excluded_df['Output_Segment']=='EXCLUDED-OutOfScope-NonDrive'].shape[0]:,}  ← BR-ND-001/005 (File 1)", 1)

    return final_df, stopwork_df, review_df, excluded_df


# ────────────────────────────────────────────────────────────
# STEP 5 — Validate output
# ────────────────────────────────────────────────────────────
def validate(final_df, stopwork_df, review_df):
    log_step("5/6", "Validating output...")
    errors   = []
    warnings = []

    # Hard errors — pipeline stops
    if len(final_df) == 0:
        errors.append("Final output is empty — no records passed all rules")

    required_out_cols = [
        "INITIATIVE_LEGACY_ID", "Demand_Type", "Demand_Sub_Type",
        "Work_Status", "Output_Segment", "Migration_Source",
    ]
    for col in required_out_cols:
        if col in final_df.columns:
            nulls = final_df[col].isnull().sum()
            if nulls > 0:
                errors.append(f"Column '{col}' has {nulls:,} null value(s) in Final_Output")

    unexpected = set(final_df["Output_Segment"].unique()) - VALID_SEGMENTS
    if unexpected:
        errors.append(f"Unexpected Output_Segment values in Final_Output: {unexpected}")

    dupes = final_df.duplicated(subset=["INITIATIVE_LEGACY_ID"]).sum()
    if dupes > 0:
        errors.append(
            f"{dupes:,} duplicate INITIATIVE_LEGACY_ID(s) in Final_Output — "
            "each initiative should appear once"
        )

    # Soft warnings — pipeline continues but flags for review
    if len(stopwork_df) > 0:
        warnings.append(
            f"{len(stopwork_df):,} Stop Work record(s) in HOLD — "
            "Finance/Triage sign-off needed before these can migrate "
            "(BR_TE_002 / BR_TE_LE_002, File 1)"
        )
    if len(review_df) > 0:
        warnings.append(
            f"{len(review_df):,} record(s) matched no rule — "
            "check Review_Required sheet (possible data quality issue)"
        )

    if errors:
        log("VALIDATION FAILED — pipeline stopped:", 1)
        for e in errors:
            log(f"  - {e}", 1)
        sys.exit(1)
    else:
        log("All checks passed", 1)
        log(f"  Row count       : OK ({len(final_df):,} output rows)", 1)
        log("  Null checks     : OK", 1)
        log("  Segment values  : OK", 1)
        log(f"  Duplicate IDs   : OK (0 duplicates)", 1)

    if warnings:
        log("", 1)
        log("Warnings (non-fatal):", 1)
        for w in warnings:
            log(f"  ! {w}", 1)


# ────────────────────────────────────────────────────────────
# STEP 6 — Build output Excel in memory and email it
# ────────────────────────────────────────────────────────────
def build_excel_bytes(final_df, stopwork_df, review_df, excluded_df):
    """Build the four-sheet Excel workbook in memory and return raw bytes.
    The file is never written to disk — it lives only in RAM until emailed.
    """
    # Columns to expose in each sheet (routing helpers dropped per Delete_Columns rules)
    final_cols = [
        "INITIATIVE_LEGACY_ID", "Initiative_Name", "Entity_Type",
        "Work_Status", "Demand_Type", "Demand_Sub_Type",
        "Portfolio", "Est_Annual_Value",
        "Migration_Source", "Migration_Track", "Output_Segment",
    ]
    hold_cols = [
        "INITIATIVE_LEGACY_ID", "Initiative_Name", "Entity_Type",
        "Work_Status", "Demand_Type", "T_Shirt_Size", "NRB_M",
        "Output_Segment", "Migration_Track",
    ]
    review_cols = [
        "INITIATIVE_LEGACY_ID", "Initiative_Name", "Entity_Type",
        "Initiative_Type", "Lifecycle_Status", "PC_Flag", "ESI_Flag",
        "T_Shirt_Size", "NRB_M", "BC_Flag", "DT_Flag",
        "Output_Segment",
    ]
    excluded_cols = [
        "INITIATIVE_LEGACY_ID", "Initiative_Name", "Entity_Type",
        "Lifecycle_Status", "Initiative_Type", "Output_Segment",
        "Migration_Track",
    ]

    def safe_cols(df, cols):
        return [c for c in cols if c in df.columns]

    # Write all four sheets into a BytesIO buffer — no temp file needed
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        final_df[safe_cols(final_df, final_cols)].to_excel(
            writer, sheet_name="Final_Output", index=False
        )
        stopwork_df[safe_cols(stopwork_df, hold_cols)].to_excel(
            writer, sheet_name="StopWork_Hold", index=False
        )
        review_df[safe_cols(review_df, review_cols)].to_excel(
            writer, sheet_name="Review_Required", index=False
        )
        excluded_df[safe_cols(excluded_df, excluded_cols)].to_excel(
            writer, sheet_name="Excluded_Records", index=False
        )

        # Auto-size columns on every sheet
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max(
                    (len(str(cell.value)) for cell in col if cell.value is not None),
                    default=10
                )
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    return buffer.getvalue()


def send_email(excel_bytes, ts, final_df, stopwork_df, review_df, excluded_df):
    """Send the Excel file via Outlook desktop app (COM automation).
    Uses your already logged-in Outlook session — no password or API key needed.
    Outlook must be open and logged in when this script runs.
    """
    log_step("6/6", "Sending output Excel via Outlook desktop...")

    out_name = f"Planview_Migration_Output_{ts}.xlsx"

    # Build email body
    body = (
        f"Hi team,\n\n"
        f"Please find attached the Planview Migration Pipeline output for run {ts}.\n\n"
        f"The file contains four sheets:\n"
        f"  - Final_Output       : Records ready to migrate to new Planview Prod\n"
        f"  - StopWork_Hold      : Records pending Finance / Triage sign-off\n"
        f"  - Review_Required    : Records that matched no rule (needs investigation)\n"
        f"  - Excluded_Records   : Status-based exclusions and out-of-scope records\n\n"
        f"Summary:\n"
        f"  Final Output (ready)  : {len(final_df):,} records\n"
        f"  Stop Work HOLD        : {len(stopwork_df):,} records\n"
        f"  Review Required       : {len(review_df):,} records\n"
        f"  Excluded              : {len(excluded_df):,} records\n\n"
        "This is an automated email from the Planview Migration Pipeline v4.\n\n"
        "Regards,\nMigration Pipeline\n"
    )

    # Save Excel bytes to a temp file so Outlook can attach it
    # Outlook COM requires a real file path for attachments
    tmp_path = None
    try:
        import win32com.client

        # Write Excel to a temp file — deleted automatically after send
        with tempfile.NamedTemporaryFile(
            suffix=".xlsx",
            prefix="pv_migration_",
            delete=False
        ) as tmp:
            tmp.write(excel_bytes)
            tmp_path = tmp.name

        # Connect to the running Outlook instance
        outlook = win32com.client.Dispatch("Outlook.Application")
        mail    = outlook.CreateItem(0)   # 0 = olMailItem

        # Set recipients
        mail.To = "; ".join(EMAIL_TO)
        if EMAIL_CC:
            mail.CC = "; ".join(EMAIL_CC)

        mail.Subject = f"{EMAIL_SUBJECT} - {ts}"
        mail.Body    = body

        # Attach the temp Excel file
        mail.Attachments.Add(tmp_path)

        # Send — uses the logged-in Outlook account, no password needed
        mail.Send()

        log(f"Sent via   : Outlook desktop (logged-in account)", 1)
        log(f"Sent to    : {'; '.join(EMAIL_TO)}", 1)
        if EMAIL_CC:
            log(f"CC         : {'; '.join(EMAIL_CC)}", 1)
        log(f"Attachment : {out_name}", 1)
        log(f"Sheets:", 1)
        log(f"  Final_Output      : {len(final_df):,} rows", 2)
        log(f"  StopWork_Hold     : {len(stopwork_df):,} rows", 2)
        log(f"  Review_Required   : {len(review_df):,} rows", 2)
        log(f"  Excluded_Records  : {len(excluded_df):,} rows", 2)

    except ImportError:
        log("ERROR: pywin32 is not installed.", 1)
        log("Run this command then try again:", 1)
        log("  pip install pywin32", 2)
        sys.exit(1)

    except Exception as e:
        log(f"ERROR: Could not send via Outlook: {e}", 1)
        log("- Make sure Outlook is open and logged in before running the script.", 2)
        log("- If Outlook is open and this still fails, try running the script", 2)
        log("  as the same Windows user that Outlook is logged in as.", 2)
        sys.exit(1)

    finally:
        # Always delete the temp file even if send fails
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


# ────────────────────────────────────────────────────────────
# CLEANUP — remove temp CSV and staging table
# ────────────────────────────────────────────────────────────
def cleanup(cursor, conn):
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
        log("Cleanup  : Staging table dropped", 1)
    except Exception:
        pass
    try:
        os.remove(TEMP_CSV)
        log("Cleanup  : Temp CSV deleted", 1)
    except Exception:
        pass
    conn.close()


# ────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────
def main():
    start = datetime.now()
    print(SEPARATOR)
    print("  Planview Migration Pipeline v4 — Customer Rules Edition")
    print(f"  Started : {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(SEPARATOR)
    print("  Rules source:")
    print("    File 1 — BusinessRules_DataMigration_Detail_breakdown.xlsx")
    print("    File 2 — DRAFT_Business_Rules_for_Data_Migration_(2).xlsx")
    print("    File 3 — Copy_of_Non_PC_Classification_Base_Data_UNDER_REVIEW.xlsx")
    print(SEPARATOR)

    df, input_path = read_and_export_csv()
    conn           = connect_sql()
    cursor         = bulk_insert(conn, df)

    final_df, stopwork_df, review_df, excluded_df = run_transform(conn)

    validate(final_df, stopwork_df, review_df)

    # Build Excel in memory then email — file is never written to disk
    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_bytes = build_excel_bytes(final_df, stopwork_df, review_df, excluded_df)
    send_email(excel_bytes, ts, final_df, stopwork_df, review_df, excluded_df)

    cleanup(cursor, conn)

    elapsed = round((datetime.now() - start).total_seconds(), 1)
    print(f"\n{SEPARATOR}")
    print("  PIPELINE COMPLETE")
    print(f"  Email sent to : {', '.join(EMAIL_TO)}")
    print(f"  Attachment    : Planview_Migration_Output_{ts}.xlsx")
    print(f"  Runtime       : {elapsed}s")
    print(SEPARATOR)

    # Preview of final output in console
    preview_cols = [
        "INITIATIVE_LEGACY_ID", "Demand_Type",
        "Demand_Sub_Type", "Work_Status", "Output_Segment",
    ]
    available = [c for c in preview_cols if c in final_df.columns]
    if available:
        print("\nFinal Output preview:")
        print(final_df[available].to_string(index=False))

    if len(stopwork_df) > 0:
        print(f"\nStop Work HOLD ({len(stopwork_df)} record(s) — need Finance/Triage sign-off):")
        hold_preview = [
            c for c in ["INITIATIVE_LEGACY_ID", "T_Shirt_Size", "NRB_M", "Output_Segment"]
            if c in stopwork_df.columns
        ]
        print(stopwork_df[hold_preview].to_string(index=False))

    if len(review_df) > 0:
        print(f"\nReview Required ({len(review_df)} record(s) — no rule matched):")
        rev_preview = [
            c for c in ["INITIATIVE_LEGACY_ID", "Initiative_Type",
                        "PC_Flag", "ESI_Flag", "T_Shirt_Size", "NRB_M"]
            if c in review_df.columns
        ]
        print(review_df[rev_preview].to_string(index=False))


if __name__ == "__main__":
    main()
