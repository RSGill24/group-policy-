"""
============================================================
Planview Migration Pipeline v3 — SQL BULK INSERT Edition
============================================================
How it works:
  1. Python reads the Excel sheet and saves it as a temp CSV
  2. Python tells SQL Server to run BULK INSERT on that CSV
  3. SQL Server reads the file directly from disk — no Python
     looping, no row-by-row, no network overhead
  4. Runs the transformation SQL
  5. Saves output Excel to the same folder as the input file
  6. Deletes the temp CSV automatically

Performance vs previous versions:
  v1 row-by-row INSERT : 1,000,000 rows = ~40 minutes
  v2 fast_executemany  : 1,000,000 rows = ~2 minutes
  v3 SQL BULK INSERT   : 1,000,000 rows = ~15-20 seconds

Usage:
  python planview_pipeline_v3.py

One-time setup:
  pip install pandas pyodbc openpyxl
============================================================
"""

import pandas as pd
import pyodbc
import sys
import os
from datetime import datetime
from pathlib import Path

# ============================================================
# CONFIG — UPDATE THESE BEFORE RUNNING
# ============================================================

INPUT_FILE    = r"C:\Input\Planview_Prototype_Hybrid_Demo.xlsx"
INPUT_SHEET   = "Input_Data"
SQL_SERVER    = "FDX-2MQ51504KD"
SQL_DATABASE  = "PlanviewDemo"
STAGING_TABLE = "PV_Pipeline_Staging"

# Temp CSV — SQL Server reads this file directly from disk
# Must be a LOCAL path on the same machine as SQL Server
# This file is deleted automatically after the load completes
TEMP_CSV = r"C:\Demo\pv_temp_import.csv"

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
        df = pd.read_excel(path, sheet_name=INPUT_SHEET, dtype=str)
    except Exception as e:
        log(f"ERROR reading sheet '{INPUT_SHEET}': {e}", 1)
        sys.exit(1)

    # Fill NaN with empty string — SQL BULK INSERT treats empty as NULL
    df = df.fillna("")

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
        log(f"ERROR: Could not connect to SQL Server", 1)
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

    # Build CREATE TABLE dynamically from the DataFrame columns
    # All columns as nvarchar(500) — safe for all text content
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
WITH base AS (
    SELECT
        [initiativeFDW #],
        [Type of opportunity],
        [DRIVE Initiative ID],
        [Weekly Status],
        [Is this Confidential?],
        [Epic T-Shirt Size],
        CASE
            WHEN [Type of opportunity] = 'DRIVE'
                THEN 'Drive Epic'
            WHEN [Type of opportunity] = 'Local Enhancement'
             AND ISNULL(NULLIF([DRIVE Initiative ID],''),'') = ''
                THEN 'Stand-alone Initiative'
            WHEN [Type of opportunity] = 'Local Enhancement'
             AND ISNULL(NULLIF([DRIVE Initiative ID],''),'') <> ''
                THEN 'Drive Epic'
            WHEN [Type of opportunity] IN ('Gen AI','GM Request','NON-DRIVE')
                THEN 'TBD'
            ELSE 'TBD'
        END AS [Temporary Placement],
        CASE
            WHEN [Type of opportunity] = 'DRIVE'             THEN 'Biz w/ Tech'
            WHEN [Type of opportunity] = 'Local Enhancement' THEN 'Local Enhancement'
            ELSE 'TBD'
        END AS [Demand Type],
        CASE
            WHEN [Weekly Status] = 'Not Assigned'         THEN 'Active'
            WHEN [Weekly Status] = 'Completed'            THEN 'Complete'
            WHEN [Weekly Status] = 'Leadership Attention' THEN 'Active'
            ELSE [Weekly Status]
        END AS [Work Status]
    FROM {STAGING_TABLE}
),
final AS (
    SELECT
        [initiativeFDW #]   AS [INITIATIVE_LEGACY_ID],
        [Demand Type],
        [Work Status],
        [Is this Confidential?],
        [Epic T-Shirt Size] AS [T-Shirt Size],
        CASE
            WHEN [Temporary Placement] = 'Stand-alone Initiative'
             AND [Is this Confidential?] <> 'No' THEN 'Biz w Tech Init-C'
            WHEN [Temporary Placement] = 'Stand-alone Initiative'
             AND [Is this Confidential?] = 'No'  THEN 'Biz w Tech Init-NonC'
            ELSE 'Excluded from demo output'
        END AS [Output Segment]
    FROM base
)
SELECT * FROM final
ORDER BY [INITIATIVE_LEGACY_ID]
"""

EXCLUDED_SQL = f"""
WITH base AS (
    SELECT
        [initiativeFDW #],
        [Type of opportunity],
        [DRIVE Initiative ID],
        CASE
            WHEN [Type of opportunity] = 'DRIVE' THEN 'Drive Epic'
            WHEN [Type of opportunity] = 'Local Enhancement'
             AND ISNULL(NULLIF([DRIVE Initiative ID],''),'') = ''
                THEN 'Stand-alone Initiative'
            WHEN [Type of opportunity] = 'Local Enhancement'
             AND ISNULL(NULLIF([DRIVE Initiative ID],''),'') <> ''
                THEN 'Drive Epic'
            ELSE 'TBD'
        END AS [Temporary Placement]
    FROM {STAGING_TABLE}
)
SELECT
    [initiativeFDW #]     AS [Initiative ID],
    [Type of opportunity] AS [Type],
    [DRIVE Initiative ID] AS [DRIVE ID],
    [Temporary Placement],
    'Not in this prototype path' AS [Reason]
FROM base
WHERE [Temporary Placement] <> 'Stand-alone Initiative'
ORDER BY [initiativeFDW #]
"""

def run_transform(conn):
    log_step("4/6", "Running transformation SQL...")
    all_df      = pd.read_sql(TRANSFORM_SQL, conn)
    final_df    = all_df[all_df["Output Segment"].isin(
                      ["Biz w Tech Init-C", "Biz w Tech Init-NonC"]
                  )].reset_index(drop=True)
    excluded_df = pd.read_sql(EXCLUDED_SQL, conn)

    log(f"Total rows processed  : {len(all_df):,}", 1)
    log(f"Final output rows     : {len(final_df):,}", 1)
    log(f"  Confidential (C)    : {(final_df['Output Segment']=='Biz w Tech Init-C').sum():,}", 1)
    log(f"  Non-confidential    : {(final_df['Output Segment']=='Biz w Tech Init-NonC').sum():,}", 1)
    log(f"Excluded rows         : {len(excluded_df):,}", 1)
    return final_df, excluded_df


# ────────────────────────────────────────────────────────────
# STEP 5 — Validate output
# ────────────────────────────────────────────────────────────
def validate(final_df):
    log_step("5/6", "Validating output...")
    errors = []

    if len(final_df) == 0:
        errors.append("Final output is empty — no rows passed the transformation")

    for col in ["INITIATIVE_LEGACY_ID", "Output Segment", "Work Status"]:
        if col in final_df.columns:
            nulls = final_df[col].isnull().sum()
            if nulls > 0:
                errors.append(f"Column '{col}' has {nulls:,} null value(s)")

    valid_segs = {"Biz w Tech Init-C", "Biz w Tech Init-NonC"}
    unexpected = set(final_df["Output Segment"].unique()) - valid_segs
    if unexpected:
        errors.append(f"Unexpected Output Segment values: {unexpected}")

    if errors:
        log("VALIDATION FAILED — pipeline stopped:", 1)
        for e in errors:
            log(f"  - {e}", 1)
        sys.exit(1)
    else:
        log("All checks passed", 1)
        log(f"  Row count    : OK ({len(final_df):,} output rows)", 1)
        log("  Null checks  : OK", 1)
        log("  Segment vals : OK", 1)


# ────────────────────────────────────────────────────────────
# STEP 6 — Write output Excel to same folder as input
# ────────────────────────────────────────────────────────────
def write_output(final_df, excluded_df, input_path):
    log_step("6/6", "Writing output Excel file...")
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"Planview_Output_{ts}.xlsx"
    out_path = input_path.parent / out_name

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="Final_Output", index=False)
        excluded_df.to_excel(writer, sheet_name="Excluded_Records", index=False)
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            for col in ws.columns:
                max_len = max(
                    (len(str(cell.value)) for cell in col if cell.value is not None),
                    default=10
                )
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    log(f"File     : {out_name}", 1)
    log(f"Location : {out_path.parent}", 1)
    log(f"Sheets   : Final_Output ({len(final_df):,} rows), "
        f"Excluded_Records ({len(excluded_df):,} rows)", 1)
    return out_path


# ────────────────────────────────────────────────────────────
# CLEANUP — remove temp CSV
# ────────────────────────────────────────────────────────────
def cleanup(cursor, conn):
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
        log("Cleanup  : Staging table dropped", 1)
    except:
        pass
    try:
        os.remove(TEMP_CSV)
        log("Cleanup  : Temp CSV deleted", 1)
    except:
        pass
    conn.close()


# ────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────
def main():
    start = datetime.now()
    print(SEPARATOR)
    print("  Planview Migration Pipeline v3 — SQL BULK INSERT")
    print(f"  Started : {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(SEPARATOR)

    df, input_path        = read_and_export_csv()
    conn                  = connect_sql()
    cursor                = bulk_insert(conn, df)
    final_df, excluded_df = run_transform(conn)
    validate(final_df)
    out_path              = write_output(final_df, excluded_df, input_path)
    cleanup(cursor, conn)

    elapsed = round((datetime.now() - start).total_seconds(), 1)
    print(f"\n{SEPARATOR}")
    print("  PIPELINE COMPLETE")
    print(f"  Output  : {out_path}")
    print(f"  Runtime : {elapsed}s")
    print(SEPARATOR)

    print("\nFinal Output preview:")
    print(final_df[["INITIATIVE_LEGACY_ID", "Demand Type",
                     "Work Status", "Is this Confidential?",
                     "Output Segment"]].to_string(index=False))

if __name__ == "__main__":
    main()
