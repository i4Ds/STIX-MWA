#!/bin/bash
# Download ready MWA ASVO jobs as tarballs (<obsid>_<jobid>_ms.tar) onto the
# calculon NAS, with giant-squid from the mwa-demo image (CPU node, Apptainer).
# Submit the ASVO jobs first (scripts/download_1419137296.sh submit-...); this
# job waits until they are ready, then downloads.
#
# Runs unattended: if it is not finished 10 min before the 24 h limit, it
# resubmits itself (up to MAX_RESUB times) and the download resumes, because
# the tarballs are kept (--keep-tar).
#
# One-time: put the ASVO key in ~/.config/mwa_asvo/api_key (mode 600) on calculon.
# Submit from ~/mwa on calculon:
#   sbatch -M calc-cpu ~/mwa/asvo-download.sbatch
#   sbatch -M calc-cpu --export=ALL,OBSIDS="1419137296",DEST=/path ~/mwa/asvo-download.sbatch
#SBATCH --job-name=asvo-dl
#SBATCH --partition=cpu-daily
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=1-00:00:00
#SBATCH --signal=B:USR1@600
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
source "$HOME/mwa/mwa-env.sh"

OBSIDS="${OBSIDS:-1419137296 1419161128 1419109008}"
# mwa_data (the shared archive) belongs to andrea_zanelli; mwa_data2 is André's
# folder next to it on the same NAS, with the same file naming.
DEST="${DEST:-/mnt/nas05/data02/MWA_data/data/mwa_data2}"
KEYFILE="${KEYFILE:-$HOME/.config/mwa_asvo/api_key}"
SCRIPT="${SCRIPT:-$HOME/mwa/asvo-download.sbatch}"   # $0 is a spool copy under Slurm
RESUB="${RESUB:-0}"
MAX_RESUB="${MAX_RESUB:-5}"

[[ -r "$KEYFILE" ]] || { echo "missing $KEYFILE"; exit 1; }
# Passed through the environment (Apptainer forwards it), never on a command line.
export MWA_ASVO_API_KEY="$(<"$KEYFILE")"
mkdir -p "$DEST"

resubmit() {
  if (( RESUB < MAX_RESUB )); then
    echo "$(date -Is) time limit near: resubmitting ($((RESUB + 1))/$MAX_RESUB)"
    cd "$HOME/mwa"
    sbatch -M calc-cpu --export=ALL,OBSIDS="$OBSIDS",DEST="$DEST",RESUB=$((RESUB + 1)) "$SCRIPT"
  else
    echo "$(date -Is) time limit near and MAX_RESUB reached: not resubmitting"
  fi
  kill "$child" 2>/dev/null || true
  exit 0
}
trap resubmit USR1

gs() { singularity exec -B /mnt/nas05/data02 "$MWA_DEMO_SIF" giant-squid "$@"; }

echo "$(date -Is) node=$(hostname) dest=$DEST obsids=$OBSIDS resub=$RESUB"
gs list $OBSIDS
# Run in the background and wait, so the USR1 trap can fire during long steps.
gs wait $OBSIDS & child=$!; wait "$child"
gs download --keep-tar --concurrent-downloads 3 -d "$DEST" $OBSIDS & child=$!; wait "$child"
echo "$(date -Is) done"
ls -lh "$DEST"
