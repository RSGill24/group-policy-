#!/usr/bin/env bash
# ============================================================
# docker-entrypoint.sh
# Runs inside the Cloud Run Job container.
# Fetches secrets, validates, and executes the Packer build.
# ============================================================

set -euo pipefail

echo "========================================================"
echo " Windows STIG Hardened Image Builder"
echo " $(date -u)"
echo "========================================================"

# ── Required environment variables ───────────────────────────
# These are injected by the Cloud Run Job definition (Terraform)
: "${PROJECT_ID:?PROJECT_ID env var is required}"
: "${SOURCE_IMAGE_PROJECT_ID:?SOURCE_IMAGE_PROJECT_ID env var is required}"
: "${SOURCE_IMAGE_FAMILY:?SOURCE_IMAGE_FAMILY env var is required}"
: "${IMAGE_FAMILY:?IMAGE_FAMILY env var is required}"
: "${ZONE:?ZONE env var is required}"
: "${MACHINE_TYPE:?MACHINE_TYPE env var is required}"
: "${SERVICE_ACCOUNT_EMAIL:?SERVICE_ACCOUNT_EMAIL env var is required}"
: "${WINRM_SECRET:?WINRM_SECRET env var is required}"
: "${HARDENING_TARGET_DIR:?HARDENING_TARGET_DIR env var is required}"
: "${PACKER_TEMPLATE:?PACKER_TEMPLATE env var is required}"

# ── Fetch WinRM password from Secret Manager ─────────────────
# ── Use WinRM password injected by Cloud Run ─────────────────
echo "Using WinRM password injected by Cloud Run..."
PACKER_PW="${WINRM_SECRET}"
export PACKER_PW

# ── Packer logging ───────────────────────────────────────────
export PACKER_LOG=1

# ── Validate template ────────────────────────────────────────
echo "Validating Packer template..."
packer validate \
  -var "project_id=${PROJECT_ID}" \
  -var "source_image_project_id=${SOURCE_IMAGE_PROJECT_ID}" \
  -var "source_image_family=${SOURCE_IMAGE_FAMILY}" \
  -var "service_account_email=${SERVICE_ACCOUNT_EMAIL}" \
  -var "image_family=${IMAGE_FAMILY}" \
  -var "machine_type=${MACHINE_TYPE}" \
  -var "zone=${ZONE}" \
  -var "hardening_source_dir=./scripts" \
  -var "hardening_target_dir=${HARDENING_TARGET_DIR}" \
  -var "hardening_entry_script=run_all.ps1" \
  "${PACKER_TEMPLATE}"

# ── Run Packer build ─────────────────────────────────────────
echo "Starting Packer build..."
packer build \
  -var "project_id=${PROJECT_ID}" \
  -var "source_image_project_id=${SOURCE_IMAGE_PROJECT_ID}" \
  -var "source_image_family=${SOURCE_IMAGE_FAMILY}" \
  -var "service_account_email=${SERVICE_ACCOUNT_EMAIL}" \
  -var "image_family=${IMAGE_FAMILY}" \
  -var "machine_type=${MACHINE_TYPE}" \
  -var "zone=${ZONE}" \
  -var "hardening_source_dir=./scripts" \
  -var "hardening_target_dir=${HARDENING_TARGET_DIR}" \
  -var "hardening_entry_script=run_all.ps1" \
  "${PACKER_TEMPLATE}"

echo "Packer build completed successfully."

# ── Deprecate older images ───────────────────────────────────
echo "Deprecating older images in family ${IMAGE_FAMILY}..."

LATEST_IMAGE=$(gcloud compute images list \
  --project="${PROJECT_ID}" \
  --filter="family=${IMAGE_FAMILY}" \
  --sort-by="~creationTimestamp" \
  --format="value(name)" \
  --limit=1)

echo "Latest image: ${LATEST_IMAGE}"

OLD_IMAGES=$(gcloud compute images list \
  --project="${PROJECT_ID}" \
  --filter="family=${IMAGE_FAMILY} AND name!=${LATEST_IMAGE}" \
  --format="value(name)")

if [ -z "${OLD_IMAGES}" ]; then
  echo "No older images to deprecate."
else
  for IMAGE in ${OLD_IMAGES}; do
    echo "Deprecating: ${IMAGE}"
    gcloud compute images deprecate "${IMAGE}" \
      --project="${PROJECT_ID}" \
      --state=DEPRECATED \
      --replacement="${LATEST_IMAGE}"
  done
  echo "Old images deprecated successfully."
fi

echo "========================================================"
echo " Build complete: ${LATEST_IMAGE}"
echo " $(date -u)"
echo "========================================================"
