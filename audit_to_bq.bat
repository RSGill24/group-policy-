@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "CSV_PATH=%SCRIPT_DIR%DSC_Audit_Results.csv"

bq load --location=US --source_format=CSV --skip_leading_rows=1 pam_ww_instance_controls.pam-ww-instance-controls-table "%CSV_PATH%"
