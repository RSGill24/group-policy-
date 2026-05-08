"""
planview_prod_data_pipeline.py
===============================
"""

import pandas as pd
import pyodbc
import json
import argparse
import sys
import io
import re
import logging
from pathlib import Path
from datetime import datetime

# ── Config globals ─────────────────────────────────────────────
INPUT_FILE        = ""
INPUT_SHEET_INITS = "Initiatives Extract_05.01"
INPUT_SHEET_EPICS = "Epics Extract_05.01"
OUTPUT_FOLDER     = ""
SQL_SERVER        = ""
SQL_DATABASE      = ""
NRB_FIELD         = "L1 Net Recurring Benefits ($, annualized)-P&L/Hard"
NRB_THRESHOLD_M   = 10
_logger           = None
SEPARATOR         = "=" * 60

# ── Value mapping tables — all sourced from VALUES_DropDowns_Mappings_2026 ──
# Applied in apply_value_transformations() before <Removed> deletion and classification

# Work Status-old → Work Status-New (Wbs20, col 112→113)
# Note: Assumed Completed not in rules file — mapped to Completed/Closed as assumption
WORK_STATUS_OLD_NEW = {
    "Not Started":       "New",
    "Approved":          "New",           
    "In Progress":       "Active",
    "On Hold":           "On Hold",
    "Closed":            "Completed/Closed",
    "Completed":         "Completed/Closed",
    "Assumed Completed": "Completed/Closed",  # not in rules file — assumption
    "Cancelled":         "Cancelled",
    "Rejected":          "Rejected",
}

# Stage old→new (wbs28, col 145→146)
# L0 and SL1 map to <Removed> — those rows will be deleted in remove_removed_rows()
# SL5 and L5 both map to I: L4 — reclassified to L4 (Evaluate Outcome Achievement)
STAGE_OLD_NEW = {
    "A: L0":  "<Removed>",   # row deleted by remove_removed_rows()
    "B: SL1": "<Removed>",   # row deleted by remove_removed_rows()
    "C: L1":  "C: L1",
    "D: SL2": "D: SL2",
    "E: L2":  "E: L2",
    "F: SL3": "F: SL3",
    "G: L3":  "G: L3",
    "H: SL4": "H: SL4",
    "I: L4":  "I: L4",
    "J: SL5": "I: L4",       # SL5 → L4 (Evaluate Outcome Achievement)
    "K: L5":  "I: L4",       # L5  → L4 (Evaluate Outcome Achievement)
}

# Estimated Value Range old→new (WR36, col 57→58)
ESTIMATED_VALUE_RANGE_OLD_NEW = {
    "1: Unknown":                     "",  # blank — no value range in new system
    "2: Low = < $1M":                 "1: Low = < $1M",
    "3: Medium = $1M < Value < $10M": "2: Medium = $1M < Value < $10M",
    "4: High = > $10M":               "3: High = > $10M",
}

# Home Portfolio old→new (wr43, col 140→141)
HOME_PORTFOLIO_OLD_NEW = {
    "Data & AI": "Platforms",
}

# Demand SubType old→new (WR53, col 150→151)
DEMAND_SUBTYPE_OLD_NEW = {
    "Protect Purple": "Infosec (Protect Purple)",
}

# Milestone Type / Task or Milestone Type old→new (wbs712, col 175→176)
# 4 values change
MILESTONE_TYPE_OLD_NEW = {
    "Technology / Systems": "Technology",
    "Finance":              "Other",
    "Legal":                "Legal / Regulatory",
    "Other dependency":     "Other",
}

# ── FY25 expired ESIs ─────────────────────────────────────────
FY25_EXPIRED_ESI = {
    "Network Optimization (FY25 ESI)",
    "Digital Intelligence (FY25 ESI)",
    "Europe - Operations (FY25 ESI)",
    "Digital Experience (FY25 ESI)",
}

# ── Output segments ───────────────────────────────────────────
SEG = {
    "init_bc":        "TE-NonDisc-BusinessContinuity",
    "init_stopwork":  "TE-Disc-StopWork-HOLD",
    "init_strat_inv": "TE-Disc-TransformationalInvestment",
    "init_disc_other":"TE-Disc-Other",
    "init_bo":        "TE-BusinessDemandMgmt",
    "init_pc":        "PC-StrategicProgram-PilotSandbox",
    "init_pending":   "PENDING-Stage-Lifecycle-Undefined",
    "lcm":            "LCM-NonDisc-RunTheBusiness",
    "le_strat":       "LE-Disc-TransformationalInvestment",
    "le_stopwork":    "LE-Disc-StopWork-HOLD",
    "le_disc":        "LE-Disc-Other",
    "bwt_epic":       "BWT-Epic-BelowPPL",
    "milestone":      "MILESTONE-RISK",
    "epic_pending":   "PENDING-Stage-Lifecycle-Undefined",
    "review":         "REVIEW-NoRuleMatched",
}

FLOW = {
    "init_bc":        "Non-Discretionary \u2013 Business Continuity",
    "init_stopwork":  "STOP WORK Discretionary \u2013 Transformational Investment",
    "init_strat_inv": "Discretionary \u2013 Transformational Investment",
    "init_disc_other":"Discretionary \u2013 Other",
    "init_bo":        "Business Demand Management",
    "init_pc":        "Strategic Program (Business + Tech) \u2014 Pilot Sandbox",
    "lcm":            "Non-Discretionary - Run the Business",
    "le_strat":       "Discretionary \u2013 Transformational Investment",
    "le_stopwork":    "STOP WORK Discretionary \u2013 Transformational Investment",
    "le_disc":        "Discretionary \u2013 Other",
    "bwt_epic":       "Business w/Tech Epic \u2014 Below PPL Task",
    "milestone":      "Milestone/Risk \u2014 separate tab (not in Epic migration scope)",
}


# ── Logging ───────────────────────────────────────────────────
def init_logger(ts, out_folder):
    global _logger
    Path(out_folder).mkdir(parents=True, exist_ok=True)
    log_file = Path(out_folder) / f"prod_pipeline_run_{ts}.log"
    _logger = logging.getLogger("prod_pipeline")
    _logger.setLevel(logging.DEBUG)
    _logger.handlers.clear()
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s  %(message)s",
                                       datefmt="%Y-%m-%d %H:%M:%S"))
    _logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(ch)
    return log_file

def log(msg, indent=0):
    line = "  " * indent + msg
    _logger.info(line) if _logger else print(line)

def log_step(n, msg):
    _logger.info(f"\n[{n}] {msg}") if _logger else print(f"\n[{n}] {msg}")


# ── Config loader ─────────────────────────────────────────────
def load_config(config_path=None):
    global INPUT_FILE, INPUT_SHEET_INITS, INPUT_SHEET_EPICS
    global OUTPUT_FOLDER, SQL_SERVER, SQL_DATABASE, NRB_FIELD, NRB_THRESHOLD_M

    if config_path is None:
        config_path = Path(__file__).parent / "planview_prod_data_pipeline_config.json"
    config_path = Path(config_path)
    if not config_path.exists():
        print(f"ERROR: Config not found: {config_path}"); sys.exit(1)
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Bad JSON: {e}"); sys.exit(1)

    INPUT_FILE        = cfg["input_file"]
    OUTPUT_FOLDER     = cfg["output_folder"]
    SQL_SERVER        = cfg.get("sql", {}).get("server", "")
    SQL_DATABASE      = cfg.get("sql", {}).get("database", "")
    INPUT_SHEET_INITS = cfg.get("sheet_initiatives", "Initiatives Extract_05.01")
    INPUT_SHEET_EPICS = cfg.get("sheet_epics",        "Epics Extract_05.01")
    NRB_FIELD         = cfg.get("nrb_field",
                                "L1 Net Recurring Benefits ($, annualized)-P&L/Hard")
    NRB_THRESHOLD_M   = cfg.get("nrb_threshold_m", 10)


# ── STEP 1: Read 3-row header input file ─────────────────────
def read_3row_header(path, sheet_name):
    """
    Row 1 = old system field IDs | Row 2 = new system IDs | Row 3 = display names
    Data starts at Row 4. Display names (Row 3) used as column headers.
    """
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=str).fillna('')
    row1_ids  = [str(raw.iloc[0, c]).strip() for c in range(raw.shape[1])]
    row2_ids  = [str(raw.iloc[1, c]).strip() for c in range(raw.shape[1])]
    row3_names= [str(raw.iloc[2, c]).strip() for c in range(raw.shape[1])]

    seen = {}
    final_cols = []
    for name in row3_names:
        if not name or name == 'nan':
            name = f"_col_{len(final_cols)}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        final_cols.append(name)

    df_data = raw.iloc[3:].copy()
    df_data.columns = final_cols

    # Filter blank rows using first non-unnamed column
    filter_col = next((c for c in final_cols if not c.startswith('_col')), final_cols[0])
    df_data = df_data[df_data[filter_col].str.strip() != ''].reset_index(drop=True)

    return df_data, row1_ids, row2_ids


# ── STEP 2: Apply value transformations ───────────────────────
def apply_value_transformations(df_in, df_ep):
    """
    Apply ALL old→new mapping tables from VALUES_DropDowns_Mappings_2026.

    Mappings applied (in order):
      1. Stage          (wbs28)  — L0/SL1 → <Removed> (row deleted next step)
                                   L5/SL5 → I: L4 (reclassified to L4)
      2. Work Status    (Wbs20)  — Approved → New (fix), old statuses → new
      3. Estimated Value Range (WR36) — renumbering + Unknown removed
      4. Home Portfolio (wr43)   — Data & AI → Platforms
      5. Demand SubType (WR53)   — Protect Purple → Infosec (Protect Purple)
      6. Milestone Type (wbs712) — Technology/Systems → Technology etc.

    Note: Initiative sheet has Lifecycle Status (not Work Status) — Work Status
          mapping only applies to Epics. Stage mapping applies to Initiatives.
    """
    changes_in = {}
    changes_ep = {}

    def apply_map(df, col, mapping, label, changes):
        if col in df.columns:
            before = df[col].copy()
            df[col] = df[col].apply(
                lambda v: mapping.get(str(v).strip(), str(v).strip())
                if str(v).strip() not in ('', 'nan') else v)
            changed = int((before != df[col]).sum())
            if changed:
                changes[label] = changed
        return df

    # 1. Stage mapping — applies to Initiative sheet (has Stage column)
    #    L0/SL1 → <Removed> so remove_removed_rows() deletes those rows next
    #    L5/SL5 → I: L4 so they get classified as L4 (Evaluate Outcome Achievement)
    df_in = apply_map(df_in, 'Stage', STAGE_OLD_NEW,
                      'Stage (L0/SL1→<Removed>, L5/SL5→L4)', changes_in)

    # 2. Work Status — Epic sheet only (Initiative has Lifecycle Status not Work Status)
    df_ep = apply_map(df_ep, 'Work Status', WORK_STATUS_OLD_NEW,
                      'Work Status (old→new)', changes_ep)

    # 3. Estimated Value Range — Initiative sheet
    df_in = apply_map(df_in, 'Estimated Annualized Value Range',
                      ESTIMATED_VALUE_RANGE_OLD_NEW,
                      'Estimated Value Range (renumbered)', changes_in)
    # Also apply to Epic sheet if column exists
    df_ep = apply_map(df_ep, 'Estimated Annualized Value Range',
                      ESTIMATED_VALUE_RANGE_OLD_NEW,
                      'Estimated Value Range (renumbered)', changes_ep)

    # 4. Home Portfolio — both sheets
    df_in = apply_map(df_in, 'Demand Domain or Portfolio',
                      HOME_PORTFOLIO_OLD_NEW,
                      'Home Portfolio (Data & AI→Platforms)', changes_in)
    df_ep = apply_map(df_ep, 'Home Domain/Portfolio',
                      HOME_PORTFOLIO_OLD_NEW,
                      'Home Portfolio (Data & AI→Platforms)', changes_ep)
    # Also try alternate column names
    for col in ['Portfolio', 'Domain', 'Home Portfolio']:
        df_in = apply_map(df_in, col, HOME_PORTFOLIO_OLD_NEW,
                          f'Home Portfolio [{col}]', changes_in)
        df_ep = apply_map(df_ep, col, HOME_PORTFOLIO_OLD_NEW,
                          f'Home Portfolio [{col}]', changes_ep)

    # 5. Demand SubType — Initiative sheet
    df_in = apply_map(df_in, 'Demand SubType', DEMAND_SUBTYPE_OLD_NEW,
                      'Demand SubType (Protect Purple→Infosec)', changes_in)
    df_in = apply_map(df_in, 'Demand_SubType', DEMAND_SUBTYPE_OLD_NEW,
                      'Demand SubType (Protect Purple→Infosec)', changes_in)

    # 6. Milestone Type / Task or Milestone Type — Epic sheet
    for col in ['Task or Milestone Type', 'Milestone Type', 'Milestone Type (Old)']:
        df_ep = apply_map(df_ep, col, MILESTONE_TYPE_OLD_NEW,
                          f'Milestone Type [{col}]', changes_ep)

    return df_in, df_ep, changes_in, changes_ep


# ── STEP 3: Delete <Removed> rows ─────────────────────────────
def remove_removed_rows(df):
    mask = df.apply(lambda col: col.astype(str).str.contains(
        '<Removed>', na=False, regex=False)).any(axis=1)
    return df[~mask].reset_index(drop=True), int(mask.sum())


# ── STEP 4a: Stage → Lifecycle step ──────────────────────────
def stage_lifecycle(stage_raw):
    s = str(stage_raw).upper()
    if 'L1' in s and 'SL' not in s:  return "Initial Request Information",                              False
    if 'L2' in s and 'SL' not in s:  return "Architecture Alignment",                                   False
    if 'L3' in s and 'SL' not in s:  return "Demand Bundle Decomp and Conceptual Architecture",          False
    if 'L4' in s and 'SL' not in s:  return "Evaluate Outcome Achievement",                             False
    if 'L5' in s and 'SL' not in s:  return "PENDING \u2014 L5 lifecycle step not defined in rules file", True
    if 'SL2' in s: return "PENDING \u2014 SL2 lifecycle step not defined", True
    if 'SL3' in s: return "PENDING \u2014 SL3 lifecycle step not defined", True
    if 'SL4' in s: return "PENDING \u2014 SL4 lifecycle step not defined", True
    if 'SL5' in s: return "PENDING \u2014 SL5 lifecycle step not defined", True
    if 'L0' in s:  return "PENDING \u2014 L0 not in migration scope",       True
    return "PENDING \u2014 Stage not recognised", True


# ── STEP 4b: Classify Initiatives ────────────────────────────
def classify_initiatives(df_raw):
    lxl = {'4: L', '5: XL'}
    sm  = {'1: XS', '2: S', '3: M'}

    def to_float(v):
        try: return float(str(v).replace(',','').replace('$','').strip())
        except: return None

    df = df_raw.copy()
    segs, flows, lc_steps, rules_app, mig_srcs = [], [], [], [], []

    for _, row in df.iterrows():
        init_type = str(row.get('Demand Type', '')).strip()
        ts        = str(row.get('T-Shirt Size', '')).strip()
        stage_raw = str(row.get('Stage', '')).strip()
        esi       = str(row.get('Enterprise Strategic Initiative (ESI)', '')).strip()
        pc        = str(row.get('Purple Chip', row.get(
                    'Does this request support a DRIVE strategic program?', ''))).strip()
        bc        = str(row.get('Is this request vital to business continuity?',
                    row.get('Is this non-discretionary demand vital to business continuity?',
                    ''))).strip()
        nrb       = to_float(row.get(NRB_FIELD, ''))
        lc_step, pending = stage_lifecycle(stage_raw)

        esi_active = (esi not in ('', '0-None', 'nan') and
                      esi not in FY25_EXPIRED_ESI and
                      pc not in ('Purple Chip', 'DRIVE Strategic Program'))

        # BR_TE_006 — Business Only (fires first)
        if init_type in ('Business Only', 'Business Only Initiative'):
            segs.append(SEG['init_bo']); flows.append(FLOW['init_bo'])
            lc_steps.append(lc_step); rules_app.append('BR_TE_006')
            mig_srcs.append('FDXPROD \u2192 New Prod'); continue

        # BR_TE_004 — Business Continuity (fires before pending check)
        if bc in ('Yes', 'yes', 'YES'):
            segs.append(SEG['init_bc']); flows.append(FLOW['init_bc'])
            lc_steps.append(lc_step); rules_app.append('BR_TE_004')
            mig_srcs.append('FDXPROD \u2192 New Prod'); continue

        # Pending stage check
        if pending:
            segs.append(SEG['init_pending']); flows.append(f'PENDING \u2014 {lc_step}')
            lc_steps.append(lc_step); rules_app.append('PENDING-Stage')
            mig_srcs.append('PENDING'); continue

        # BR_PC_001 — Purple Chip / DRIVE Strategic Program
        if pc in ('Purple Chip', 'DRIVE Strategic Program'):
            segs.append(SEG['init_pc']); flows.append(FLOW['init_pc'])
            lc_steps.append(lc_step); rules_app.append('BR_PC_001')
            mig_srcs.append('FDXSANDBOXA \u2192 New Prod (ESPM owns)'); continue

        # BwT rules
        if init_type in ('Business w/ Tech', 'Business w/ Tech Initiative'):
            if ts in lxl:
                if nrb is not None:
                    if nrb < NRB_THRESHOLD_M:
                        segs.append(SEG['init_stopwork']); flows.append(FLOW['init_stopwork'])
                        rules_app.append('BR_TE_002 (Stop Work)')
                        mig_srcs.append('FDXPROD \u2192 New Prod (HOLD)')
                    elif esi_active:
                        segs.append(SEG['init_strat_inv']); flows.append(FLOW['init_strat_inv'])
                        rules_app.append('BR_TE_001 (Strategic Investment)')
                        mig_srcs.append('FDXPROD \u2192 New Prod')
                    else:
                        segs.append(SEG['init_stopwork']); flows.append(FLOW['init_stopwork'])
                        rules_app.append('BR_TE_002 (L/XL no active ESI)')
                        mig_srcs.append('FDXPROD \u2192 New Prod (HOLD)')
                else:
                    segs.append(SEG['init_disc_other']); flows.append(FLOW['init_disc_other'])
                    rules_app.append('BR_TE_003 (NRB blank \u2192 Disc Other)')
                    mig_srcs.append('FDXPROD \u2192 New Prod')
                lc_steps.append(lc_step); continue
            # T-shirt <= M or blank
            segs.append(SEG['init_disc_other']); flows.append(FLOW['init_disc_other'])
            lc_steps.append(lc_step)
            rules_app.append('BR_TE_003 (\u2264M or blank)')
            mig_srcs.append('FDXPROD \u2192 New Prod'); continue

        # LCM Initiatives
        if 'Lifecycle Management' in init_type:
            segs.append(SEG['init_disc_other']); flows.append('Lifecycle Management \u2014 Carry as-is')
            lc_steps.append(lc_step); rules_app.append('LCM-Initiative')
            mig_srcs.append('FDXPROD \u2192 New Prod'); continue

        # Fallback
        segs.append(SEG['review']); flows.append('REVIEW \u2014 No rule matched')
        lc_steps.append(lc_step); rules_app.append('REVIEW')
        mig_srcs.append('REVIEW')

    df['Output_Segment']        = segs
    df['Future_State_Flow']     = flows
    df['Target_Lifecycle_Step'] = lc_steps
    df['Rule_ID_Applied']       = rules_app
    df['Migration_Source']      = mig_srcs
    return df


# ── STEP 4c: Classify Epics ───────────────────────────────────
def classify_epics(df_raw):
    """
    Epic sheet has NO Stage column — stage-based pending check is skipped.
    Work Type values: 'Biz w/ Tech Epic', 'Local Enhancement Epic',
                      'Lifecycle Management Epic', 'Initiative Milestones & Risks'
    NewID_Temp generated for all classified (non-milestone) epics.
    """
    lxl = {'4: L', '5: XL'}
    sm  = {'1: XS', '2: S', '3: M'}
    MED = "3: Medium = $1M < Value < $10M"

    df = df_raw.copy()
    segs, flows, lc_steps, rules_app, mig_srcs = [], [], [], [], []
    new_ids, parent_ids, exec_types, kanban_stats, epg_approvals = [], [], [], [], []
    new_id_counter = 0
    lc_step = "Refer to parent Initiative lifecycle step"  # no Stage col in Epic sheet

    for _, row in df.iterrows():
        wt     = str(row.get('Work Type', '')).strip()
        ts     = str(row.get('T-Shirt Size', '')).strip()
        vr     = str(row.get('Estimated Annualized Value Range',
                   row.get('Estimated Value Range', ''))).strip()
        parent = str(row.get('Associated Initiative',
                   row.get('Associated Initiative Seq ID', ''))).strip()

        # Milestone/Risk — separate tab, not in Epic migration scope
        if 'Milestone' in wt or 'Risk' in wt or wt == 'Initiative Milestones & Risks':
            segs.append(SEG['milestone']); flows.append(FLOW['milestone'])
            lc_steps.append('N/A'); rules_app.append('MILESTONE-SEPARATE')
            mig_srcs.append('See Epics_Milestone_Risk tab')
            new_ids.append(''); parent_ids.append(parent)
            exec_types.append(''); kanban_stats.append(''); epg_approvals.append('')
            continue

        # All non-milestone classified epics get a NewID for parent-child linkage
        new_id_counter += 1
        new_id = f"NewID-{new_id_counter:04d}"

        # BwT Epics — "Biz w/ Tech Epic" in this prod extract
        if 'w/' in wt.lower() or 'biz w/' in wt.lower():
            segs.append(SEG['bwt_epic']); flows.append(FLOW['bwt_epic'])
            lc_steps.append(lc_step); rules_app.append('BR_TE_BWTE_001\u2013004')
            mig_srcs.append('FDXPROD \u2192 New Prod (Below PPL Task)')
            new_ids.append(new_id); parent_ids.append(parent)
            exec_types.append('Demand Bundle Epic at PPL+2')
            kanban_stats.append('Intake/New'); epg_approvals.append('')
            continue

        # LCM Epics
        if wt == 'Lifecycle Management Epic':
            segs.append(SEG['lcm']); flows.append(FLOW['lcm'])
            lc_steps.append(lc_step); rules_app.append('BR_TE_LCM_001')
            mig_srcs.append('FDXPROD \u2192 New Prod')
            new_ids.append(new_id); parent_ids.append(parent)
            exec_types.append('Demand Bundle Epic at PPL+2')
            kanban_stats.append('Intake/New'); epg_approvals.append('')
            continue

        # LE Epics
        if wt == 'Local Enhancement Epic':
            if ts in lxl:
                if vr == MED:
                    segs.append(SEG['le_stopwork']); flows.append(FLOW['le_stopwork'])
                    rules_app.append('BR_TE_LE_003 (value range proxy)')
                    mig_srcs.append('FDXPROD \u2192 New Prod (HOLD)')
                else:
                    segs.append(SEG['le_strat']); flows.append(FLOW['le_strat'])
                    rules_app.append('BR_TE_LE_001')
                    mig_srcs.append('FDXPROD \u2192 New Prod')
            else:
                rule = 'BR_TE_LE_005' if ts in sm else 'BR_BLANK_001'
                segs.append(SEG['le_disc']); flows.append(FLOW['le_disc'])
                rules_app.append(rule); mig_srcs.append('FDXPROD \u2192 New Prod')
            lc_steps.append(lc_step)
            new_ids.append(new_id); parent_ids.append(parent)
            exec_types.append('Demand Bundle Epic at PPL+2')
            kanban_stats.append('Intake/New'); epg_approvals.append('')
            continue

        # Fallback
        segs.append(SEG['review']); flows.append('REVIEW \u2014 Unknown work type')
        lc_steps.append(lc_step); rules_app.append('REVIEW')
        mig_srcs.append('REVIEW')
        new_ids.append(''); parent_ids.append(parent)
        exec_types.append(''); kanban_stats.append(''); epg_approvals.append('')

    df['Output_Segment']        = segs
    df['Future_State_Flow']     = flows
    df['Target_Lifecycle_Step'] = lc_steps
    df['Rule_ID_Applied']       = rules_app
    df['Migration_Source']      = mig_srcs
    df['NewID_Temp']            = new_ids
    df['Parent_Work_ID']        = parent_ids
    df['Execution_Type']        = exec_types
    df['Kanban_Status']         = kanban_stats
    df['EPG_Approval']          = epg_approvals
    return df, new_id_counter


# ── STEP 5: Build Excel output ────────────────────────────────
def build_excel(df_inits, df_epics, ts, input_path, out_folder,
                removed_in, removed_ep, changes_in, changes_ep):

    FINAL_I = {SEG['init_bc'], SEG['init_strat_inv'],
               SEG['init_disc_other'], SEG['init_bo']}
    HOLD_I  = {SEG['init_stopwork'], SEG['init_pc']}
    PEND_I  = {SEG['init_pending'], SEG['review']}
    FINAL_E = {SEG['lcm'], SEG['le_strat'], SEG['le_disc'], SEG['bwt_epic']}
    HOLD_E  = {SEG['le_stopwork']}
    PEND_E  = {SEG['epic_pending'], SEG['review']}
    MILE_E  = {SEG['milestone']}

    init_final    = df_inits[df_inits['Output_Segment'].isin(FINAL_I)].copy()
    init_hold     = df_inits[df_inits['Output_Segment'].isin(HOLD_I)].copy()
    init_review   = df_inits[df_inits['Output_Segment'].isin(PEND_I)].copy()
    epic_final    = df_epics[df_epics['Output_Segment'].isin(FINAL_E)].copy()
    epic_hold     = df_epics[df_epics['Output_Segment'].isin(HOLD_E)].copy()
    epic_review   = df_epics[df_epics['Output_Segment'].isin(PEND_E)].copy()
    epic_milestone= df_epics[df_epics['Output_Segment'].isin(MILE_E)].copy()

    def write_sheet(writer, df, sheet_name):
        if df.empty:
            pd.DataFrame({"Note": ["No records in this category"]}).to_excel(
                writer, sheet_name=sheet_name, index=False)
            return
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            mx = max((len(str(c.value)) for c in col_cells if c.value), default=10)
            ws.column_dimensions[col_cells[0].column_letter].width = min(mx + 4, 60)

    output_file = Path(out_folder) / f"Planview_Prod_Migration_Output_{ts}.xlsx"
    Path(out_folder).mkdir(parents=True, exist_ok=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        write_sheet(writer, init_final,     "Initiatives_Final")
        write_sheet(writer, init_hold,      "Initiatives_Hold")
        write_sheet(writer, init_review,    "Initiatives_Review")
        write_sheet(writer, epic_final,     "Epics_Final")
        write_sheet(writer, epic_hold,      "Epics_Hold")
        write_sheet(writer, epic_review,    "Epics_Review")
        write_sheet(writer, epic_milestone, "Epics_Milestone_Risk")

        # Summary
        summary = [
            ["RUN INFORMATION", ""],
            ["Run Timestamp",  ts],
            ["Input File",     str(input_path)],
            ["", ""],
            ["PRE-PROCESSING", ""],
            ["<Removed> rows deleted (Initiatives)", removed_in],
            ["<Removed> rows deleted (Epics)",       removed_ep],
            ["Value mapping applied (Initiatives)",
             f"Work Status: {changes_in.get('Work Status',0)} values remapped" if changes_in else "None"],
            ["Value mapping applied (Epics)",
             f"Work Status: {changes_ep.get('Work Status',0)} values remapped" if changes_ep else "None"],
            ["", ""],
            ["INITIATIVES", f"{len(df_inits):,} total"],
            ["Final Output",   f"{len(init_final):,}"],
            ["Stop Work HOLD", f"{len(init_hold):,}  (inc. Purple Chip/DRIVE \u2014 Pilot Sandbox)"],
            ["Review/Pending", f"{len(init_review):,}  (SL/L5 stages undefined in rules file)"],
            ["", ""],
            ["EPICS", f"{len(df_epics):,} total"],
            ["Final Output",   f"{len(epic_final):,}  (incl. BwT Epics with NewID_Temp)"],
            ["Stop Work HOLD", f"{len(epic_hold):,}"],
            ["Review/Pending", f"{len(epic_review):,}  (SL stages undefined)"],
            ["Milestone/Risk (separate tab)", f"{len(epic_milestone):,}  (Work Type = Initiative Milestones & Risks \u2014 not in Epic migration scope)"],
            ["", ""],
            ["NEWID TEMPORARY INDEX", ""],
            ["Purpose", "Epics classified as below-PPL tasks have a temporary NewID-xxxx. "
                        "Links task to parent Work ID. Replaced after import into Planview."],
            ["Records with NewID_Temp",
             f"{(df_epics['NewID_Temp'] != '').sum():,}"],
            ["", ""],
            ["OPEN ITEMS", ""],
            ["BC field (BR_TE_004)",
             "Is this request vital to business continuity? field does NOT exist in this "
             "prod extract. BC classification returns 0 records. Customer to confirm "
             "which field or value identifies BC initiatives in the full prod extract."],
            ["Purple Chip / PC (BR_PC_001)",
             "Purple Chip field does NOT exist in this prod extract. PC/DRIVE Strategic "
             "Program classification returns 0 records. No PC path to Pilot Sandbox in output. "
             "Customer to confirm how PC initiatives are identified in prod extract."],
            ["Epic Stop Work HOLD = 0",
             "Estimated Annualized Value Range field does not exist in Epic sheet of prod "
             "extract (it exists in Initiative sheet only). BR_TE_LE_003 Stop Work proxy "
             "cannot fire. All LE L/XL Epics go to Transformational Investment. Correct "
             "behaviour for this extract."],
            ["Status exclusion",
             "New rules file has no explicit status exclusion rule. All statuses pass through. "
             "confirm: which Lifecycle Status / Work Status values should be excluded?"],
            ["NRB field",
             "Pending Finance confirmation \u2014 using L1 NRB Hard as assumption. "
             "New rules file BR_TE_002 notes: 'What field are we using for NRB?'"],
            ["Stop Work meaning",
             "What does Stop Work mean operationally? Exclude or migrate as cancelled?"],
            ["SL/L5 stages",
             "Target lifecycle step not defined in rules file for SL/L5 stages. "
             f"{len(init_review):,} Initiative records in Review."],
        ]

        df_sum = pd.DataFrame(summary, columns=["Item", "Value"])
        df_sum.to_excel(writer, sheet_name="Summary", index=False)
        ws_sum = writer.sheets["Summary"]
        ws_sum.column_dimensions['A'].width = 40
        ws_sum.column_dimensions['B'].width = 80

    with open(output_file, 'wb') as f:
        f.write(buf.getvalue())

    return (output_file,
            init_final, init_hold, init_review,
            epic_final, epic_hold, epic_review, epic_milestone)



# ── SQL: Connect ──────────────────────────────────────────────
def connect_sql():
    log_step("2b/5", "Connecting to SQL Server...")
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};"
            f"Trusted_Connection=yes;Connection Timeout=30;"
        )
        conn.autocommit = True
        log(f"Server   : {SQL_SERVER} / {SQL_DATABASE}", 1)
        log("Status   : Connected", 1)
        return conn
    except pyodbc.Error as e:
        log(f"ERROR: Could not connect to SQL Server: {e}", 1)
        sys.exit(1)


# ── SQL: Load single dataframe into a table ───────────────────
def _sql_col(name):
    """Sanitise a column name for SQL Server.
    Removes/replaces characters that are invalid inside [] identifiers.
    Brackets inside a bracketed identifier break SQL even when quoted.
    """
    # Replace ] with nothing (would break the bracketed identifier)
    # Replace [ with nothing
    # Replace comma, slash, colon with underscore
    clean = (str(name)
             .replace(']', '')
             .replace('[', '')
             .replace(',', '_')
             .replace('/', '_')
             .replace('\\', '_')
             .replace(':', '_')
             .replace('?', '')
             .replace('(', '')
             .replace(')', '')
             .strip())
    # Truncate to 120 chars (SQL Server max identifier = 128)
    return clean[:120]


def load_to_sql(conn, df, schema_name, table_name, ts):
    cursor = conn.cursor()
    cursor.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema_name}')
            EXEC('CREATE SCHEMA [{schema_name}]')
    """)
    cursor.execute(f"""
        IF OBJECT_ID('[{schema_name}].[{table_name}]') IS NOT NULL
            DROP TABLE [{schema_name}].[{table_name}]
    """)

    # Sanitise column names — brackets/commas in names break SQL identifiers
    safe_cols = [_sql_col(c) for c in df.columns]
    # Handle duplicates after sanitising
    seen = {}
    final_cols = []
    for c in safe_cols:
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        final_cols.append(c)
    df = df.copy()
    df.columns = final_cols

    col_defs = ", ".join([f"[{c}] nvarchar(MAX)" for c in df.columns])
    cursor.execute(f"""
        CREATE TABLE [{schema_name}].[{table_name}] (
            [Run_ID]         nvarchar(50)  DEFAULT '{ts}',
            [Load_Timestamp] datetime      DEFAULT GETDATE(),
            {col_defs}
        )
    """)
    col_names    = ", ".join([f"[{c}]" for c in df.columns])
    placeholders = ", ".join(["?" for _ in df.columns])
    ins = (
        f"INSERT INTO [{schema_name}].[{table_name}] "
        f"([Run_ID],[Load_Timestamp],{col_names}) "
        f"VALUES ('{ts}',GETDATE(),{placeholders})"
    )
    df_c = df.where(df.notna(), None)
    for col in df_c.columns:
        if df_c[col].dtype == object and hasattr(df_c[col], "str"):
            df_c[col] = (df_c[col]
                .str.replace("\n", " ", regex=False)
                .str.replace("\r", " ", regex=False)
                .str.replace("|",  " ", regex=False)
                .str.replace('"', "'", regex=False)
                .str.strip())
    rows = [tuple(r) for r in df_c.itertuples(index=False, name=None)]
    conn.autocommit = False
    try:
        for i in range(0, len(rows), 500):
            cursor.executemany(ins, rows[i:i+500])
        conn.commit()
    except Exception as e:
        conn.rollback(); raise
    finally:
        conn.autocommit = True
    log(f"  [{schema_name}].[{table_name}] — {len(rows):,} rows loaded", 1)


# ── SQL: Load raw input into input schema ─────────────────────
def create_input_schema_and_load(conn, df_inits, df_epics, input_path, ts):
    log_step("3a/5", "Loading raw input data to SQL Server...")
    schema   = f"input_{ts}"
    stem     = re.sub(r'[^A-Za-z0-9_]', '_', input_path.stem).strip('_')
    tbl_init = f"{stem}_Initiatives"
    tbl_epic = f"{stem}_Epics"
    load_to_sql(conn, df_inits, schema, tbl_init, ts)
    load_to_sql(conn, df_epics, schema, tbl_epic, ts)
    log(f"Input schema : [{schema}]", 1)
    return schema, tbl_init, tbl_epic


# ── SQL: Save single output table ────────────────────────────
def save_output_to_sql(conn, df, schema_name, table_name, ts):
    cursor = conn.cursor()
    cursor.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema_name}')
            EXEC('CREATE SCHEMA [{schema_name}]')
    """)
    cursor.execute(f"""
        IF OBJECT_ID('[{schema_name}].[{table_name}]') IS NOT NULL
            DROP TABLE [{schema_name}].[{table_name}]
    """)
    if df.empty:
        log(f"  [{schema_name}].[{table_name}] — 0 rows (skipped)", 1)
        return

    # Sanitise column names — same as load_to_sql
    safe_cols = [_sql_col(c) for c in df.columns]
    seen = {}
    final_cols = []
    for c in safe_cols:
        if c in seen:
            seen[c] += 1
            c = f"{c}_{seen[c]}"
        else:
            seen[c] = 0
        final_cols.append(c)
    df = df.copy()
    df.columns = final_cols
    col_defs = ", ".join([f"[{c}] nvarchar(MAX)" for c in df.columns])
    cursor.execute(f"""
        CREATE TABLE [{schema_name}].[{table_name}] (
            [Run_ID]         nvarchar(50) DEFAULT '{ts}',
            [Save_Timestamp] datetime     DEFAULT GETDATE(),
            {col_defs}
        )
    """)
    col_names    = ", ".join([f"[{c}]" for c in df.columns])
    placeholders = ", ".join(["?" for _ in df.columns])
    ins = (
        f"INSERT INTO [{schema_name}].[{table_name}] "
        f"([Run_ID],[Save_Timestamp],{col_names}) "
        f"VALUES ('{ts}',GETDATE(),{placeholders})"
    )
    df_c = df.where(df.notna(), None)
    rows = [tuple(r) for r in df_c.itertuples(index=False, name=None)]
    conn.autocommit = False
    try:
        for i in range(0, len(rows), 500):
            cursor.executemany(ins, rows[i:i+500])
        conn.commit()
    except Exception as e:
        conn.rollback(); raise
    finally:
        conn.autocommit = True
    log(f"  [{schema_name}].[{table_name}] — {len(df):,} rows saved", 1)


# ── SQL: Save all output sets to output schema ────────────────
def create_output_schema_and_save(conn, df_inits, df_epics, stem, ts):
    log_step("5a/5", "Saving classified output to SQL Server...")
    schema = f"output_{ts}"

    FINAL_I = {SEG['init_bc'], SEG['init_strat_inv'],
               SEG['init_disc_other'], SEG['init_bo']}
    HOLD_I  = {SEG['init_stopwork'], SEG['init_pc']}
    PEND_I  = {SEG['init_pending'], SEG['review']}
    FINAL_E = {SEG['lcm'], SEG['le_strat'], SEG['le_disc'], SEG['bwt_epic']}
    HOLD_E  = {SEG['le_stopwork']}
    PEND_E  = {SEG['epic_pending'], SEG['review']}
    MILE_E  = {SEG['milestone']}

    init_final     = df_inits[df_inits['Output_Segment'].isin(FINAL_I)].copy()
    init_hold      = df_inits[df_inits['Output_Segment'].isin(HOLD_I)].copy()
    init_review    = df_inits[df_inits['Output_Segment'].isin(PEND_I)].copy()
    epic_final     = df_epics[df_epics['Output_Segment'].isin(FINAL_E)].copy()
    epic_hold      = df_epics[df_epics['Output_Segment'].isin(HOLD_E)].copy()
    epic_review    = df_epics[df_epics['Output_Segment'].isin(PEND_E)].copy()
    epic_milestone = df_epics[df_epics['Output_Segment'].isin(MILE_E)].copy()

    save_output_to_sql(conn, init_final,     schema, f"{stem}_Initiatives_Final",         ts)
    save_output_to_sql(conn, init_hold,      schema, f"{stem}_Initiatives_Hold",           ts)
    save_output_to_sql(conn, init_review,    schema, f"{stem}_Initiatives_Review",         ts)
    save_output_to_sql(conn, epic_final,     schema, f"{stem}_Epics_Final",                ts)
    save_output_to_sql(conn, epic_hold,      schema, f"{stem}_Epics_Hold",                 ts)
    save_output_to_sql(conn, epic_review,    schema, f"{stem}_Epics_Review",               ts)
    save_output_to_sql(conn, epic_milestone, schema, f"{stem}_Epics_Milestone_Risk",       ts)

    log(f"Output schema: [{schema}]", 1)
    return (schema,
            init_final, init_hold, init_review,
            epic_final, epic_hold, epic_review, epic_milestone)


# ── SQL: Log run to run_history ───────────────────────────────
def log_run_history(conn, ts, input_path, input_schema, output_schema,
                    init_final, init_hold, init_review,
                    epic_final, epic_hold, epic_review, epic_milestone,
                    out_path, elapsed, status, removed_in, removed_ep):
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='run_history')
            EXEC('CREATE SCHEMA [run_history]')
    """)
    # Drop and recreate if Prod_Pipeline_Version column missing
    cursor.execute("""
        IF OBJECT_ID('run_history.Pipeline_Runs_Prod') IS NOT NULL
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM sys.columns
                WHERE object_id=OBJECT_ID('run_history.Pipeline_Runs_Prod')
                AND name='Removed_Rows'
            )
            DROP TABLE run_history.Pipeline_Runs_Prod
        END
    """)
    cursor.execute("""
        IF OBJECT_ID('run_history.Pipeline_Runs_Prod') IS NULL
        CREATE TABLE run_history.Pipeline_Runs_Prod (
            Run_ID             nvarchar(50)  PRIMARY KEY,
            Run_Timestamp      datetime      DEFAULT GETDATE(),
            Pipeline_Name      nvarchar(200),
            Pipeline_Version   nvarchar(20),
            Input_File         nvarchar(500),
            Input_Schema       nvarchar(200),
            Output_Schema      nvarchar(200),
            Init_Final         int, Init_Hold    int, Init_Review  int,
            Epic_Final         int, Epic_Hold    int, Epic_Review  int,
            Epic_Milestone     int,
            Removed_Rows       int,
            Output_File_Path   nvarchar(500),
            Runtime_Seconds    decimal(10,1),
            Run_Status         nvarchar(50)
        )
    """)
    cursor.execute("""
        INSERT INTO run_history.Pipeline_Runs_Prod (
            Run_ID, Pipeline_Name, Pipeline_Version, Input_File,
            Input_Schema, Output_Schema,
            Init_Final, Init_Hold, Init_Review,
            Epic_Final, Epic_Hold, Epic_Review, Epic_Milestone,
            Removed_Rows, Output_File_Path, Runtime_Seconds, Run_Status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ts, 'Prod Data Pipeline', 'prod_v1', str(input_path),
        input_schema, output_schema,
        len(init_final), len(init_hold), len(init_review),
        len(epic_final), len(epic_hold), len(epic_review), len(epic_milestone),
        removed_in + removed_ep, str(out_path), elapsed, status
    ))
    conn.commit()
    log(f"Run logged : run_history.Pipeline_Runs_Prod — Run_ID: {ts}", 1)


# ── Main ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Planview Prod Data Pipeline")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    load_config(args.config)

    start = datetime.now()
    ts    = start.strftime("%Y%m%d_%H%M%S")
    log_file = init_logger(ts, OUTPUT_FOLDER)

    log(SEPARATOR)
    log("  Planview Prod Data Pipeline")
    log(f"  Input  : {INPUT_FILE}")
    log(f"  Output : {OUTPUT_FOLDER}")
    log(f"  Started: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"  Note   : Status exclusion NOT applied \u2014 all records pass through")
    log(f"           Customer to confirm which statuses to exclude (see Summary tab)")
    log(SEPARATOR)

    # Step 1 — Read input files (3-row header)
    log_step("1/5", "Reading input files (3-row header)...")
    ip = Path(INPUT_FILE)
    df_in_raw, in_r1, in_r2 = read_3row_header(ip, INPUT_SHEET_INITS)
    df_ep_raw, ep_r1, ep_r2 = read_3row_header(ip, INPUT_SHEET_EPICS)
    log(f"Initiatives : {len(df_in_raw):,} rows | {len(df_in_raw.columns)} cols", 1)
    log(f"Epics       : {len(df_ep_raw):,} rows | {len(df_ep_raw.columns)} cols", 1)
    log("Row 1=old IDs, Row 2=new IDs, Row 3=display names used as column headers", 1)
  

    # Step 2 — Connect to SQL and load RAW input first
    conn = connect_sql()
    stem = re.sub(r'[^A-Za-z0-9_]', '_', ip.stem).strip('_')

    # Load raw input into SQL BEFORE any transformations
    # This preserves the original source data exactly as extracted
    input_schema, tbl_init, tbl_epic = create_input_schema_and_load(
        conn, df_in_raw, df_ep_raw, ip, ts)

    # Step 2a — Apply value transformations (VALUES_DropDowns_Mappings_2026)
    log_step("2a/5", "Applying value transformation rules...")
    df_in_mapped, df_ep_mapped, changes_in, changes_ep = apply_value_transformations(
        df_in_raw.copy(), df_ep_raw.copy())
    if changes_in:
        for col, cnt in changes_in.items():
            log(f"Initiatives — {col}: {cnt:,} values remapped", 1)
    if changes_ep:
        for col, cnt in changes_ep.items():
            log(f"Epics       — {col}: {cnt:,} values remapped", 1)

    # Step 3 — Delete <Removed> rows
    log_step("3/5", "Deleting <Removed> rows...")
    df_in_clean, removed_in = remove_removed_rows(df_in_mapped)
    df_ep_clean, removed_ep = remove_removed_rows(df_ep_mapped)
    log(f"Initiatives: {removed_in} rows deleted | {len(df_in_clean):,} remaining", 1)
    log(f"Epics      : {removed_ep} rows deleted | {len(df_ep_clean):,} remaining", 1)

    # Step 4 — Apply business rules
    log_step("4/5", "Applying business rules...")
    df_inits = classify_initiatives(df_in_clean)
    df_epics, new_id_count = classify_epics(df_ep_clean)

    ivc = df_inits['Output_Segment'].value_counts()
    log(f"INITIATIVES ({len(df_inits):,} total):", 1)
    for k in ["init_bc","init_strat_inv","init_disc_other","init_bo"]:
        c = ivc.get(SEG[k], 0)
        if c: log(f"  {SEG[k]:<45}: {c:>6}", 2)
    log(f"  {'SUBTOTAL Final':<45}: {sum(ivc.get(SEG[k],0) for k in ['init_bc','init_strat_inv','init_disc_other','init_bo']):>6}", 2)
    for k in ["init_stopwork","init_pc"]:
        c = ivc.get(SEG[k], 0)
        if c: log(f"  {SEG[k]:<45}: {c:>6}", 2)
    c = ivc.get(SEG['init_pending'], 0) + ivc.get(SEG['review'], 0)
    if c: log(f"  {'Review/Pending':<45}: {c:>6}", 2)

    evc = df_epics['Output_Segment'].value_counts()
    log(f"\nEPICS ({len(df_epics):,} total):", 1)
    for k in ["lcm","le_strat","le_disc","bwt_epic"]:
        c = evc.get(SEG[k], 0)
        if c: log(f"  {SEG[k]:<45}: {c:>6}", 2)
    log(f"  {'SUBTOTAL Final':<45}: {sum(evc.get(SEG[k],0) for k in ['lcm','le_strat','le_disc','bwt_epic']):>6}", 2)
    c = evc.get(SEG['le_stopwork'], 0)
    if c: log(f"  {SEG['le_stopwork']:<45}: {c:>6}", 2)
    c = evc.get(SEG['epic_pending'], 0) + evc.get(SEG['review'], 0)
    if c: log(f"  {'Review/Pending':<45}: {c:>6}", 2)
    c = evc.get(SEG['milestone'], 0)
    if c: log(f"  {'Milestone/Risk (separate tab)':<45}: {c:>6}", 2)
    log(f"  NewID_Temp assigned: {new_id_count:,}", 1)

    # Step 5a — Save classified output to SQL
    (output_schema,
     init_final, init_hold, init_review,
     epic_final, epic_hold, epic_review, epic_milestone) = create_output_schema_and_save(
        conn, df_inits, df_epics, stem, ts)

    # Step 5b — Log run to run_history
    log_step("5b/5", "Logging run to run_history.Pipeline_Runs_Prod...")

    # Step 5c — Write Excel output
    log_step("5c/5", "Writing output Excel...")
    (out_path,
     init_final, init_hold, init_review,
     epic_final, epic_hold, epic_review, epic_milestone) = build_excel(
        df_inits, df_epics, ts, ip, OUTPUT_FOLDER,
        removed_in, removed_ep, changes_in, changes_ep)

    elapsed = round((datetime.now() - start).total_seconds(), 1)

    log(f"\n{SEPARATOR}")
    log("  PIPELINE COMPLETE")
    log(f"  Output : {out_path}")
    log(f"  Log    : {log_file}")
    log(f"")
    log(f"  INITIATIVES ({len(df_inits):,} total \u2014 all statuses included):")
    log(f"    Final Output   : {len(init_final):,}")
    log(f"    Stop Work HOLD : {len(init_hold):,}  (inc. Purple Chip/DRIVE)")
    log(f"    Review/Pending : {len(init_review):,}  (SL/L5 stages undefined)")
    log(f"")
    log(f"  EPICS ({len(df_epics):,} total \u2014 all statuses included):")
    log(f"    Final Output   : {len(epic_final):,}  (incl. BwT Epics with NewID_Temp)")
    log(f"    Stop Work HOLD : {len(epic_hold):,}")
    log(f"    Review/Pending : {len(epic_review):,}")
    log(f"    Milestone/Risk : {len(epic_milestone):,}  (separate tab)")
    log(f"")
    log(f"  <Removed> deleted : {removed_in} Init + {removed_ep} Epic rows")
    log(f"  NewID_Temp        : {new_id_count:,} Epic records")
    log(f"  Runtime           : {elapsed}s")
    # Step 5b (after Excel saved — we have out_path now)
    log_run_history(conn, ts, ip, input_schema, output_schema,
                    init_final, init_hold, init_review,
                    epic_final, epic_hold, epic_review, epic_milestone,
                    out_path, elapsed, "Completed", removed_in, removed_ep)

    conn.close()
    log("SQL      : Connection closed", 1)
    log(SEPARATOR)


if __name__ == "__main__":
    main()
