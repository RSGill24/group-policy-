repository-root/
├── packer/
│   ├── docker_server_scripts/
│   │   ├── add_repo_to_apt.sh
│   │   ├── daemon.json
│   │   ├── install_docker_latest.sh
│   │   ├── official_docker_key_add.sh
│   │   └── patching/
│   │
│   ├── windows-workstation-files/
│   │   ├── WindowsServer-2022-MS-2.1.org.pa
│   │   ├── apply_mof.ps1
│   │   ├── audit.ps1
│   │   ├── audit_to_bq.bat
│   │   ├── create_audit_task.ps1
│   │   ├── create_mof.ps1
│   │   ├── install_PowerSTIG.ps1
│   │   ├── install_dsc_deps.ps1
│   │   ├── run_all.ps1
│   │   └── run_only_audit.ps1
│   │
│   ├── cloudbuild.yml
│   ├── harden_ww.pkr.hcl
│   └── update_ww.pkr.hcl
│
├── api.tf
├── application_development.tf
├── bucketlabels.json
├── buckets.tf
├── build_da.pkr.hcl
├── cloudbuild.tf
├── compliance_bq.tf
├── config steps.txt
├── custom_roles.tf
├── data_buckets_principles.tf
├── data_read_log_bq.tf
├── main.tf
├── network.tf
├── open_in_cloud_shell.txt
├── pam_ww_principles.tf
├── patching.tf
├── principles_misc.tf
├── roles.tf
├── snapshot.tf
├── sql.tf
├── transfer_appliance_sa_roles.tf
└── variables.tf
