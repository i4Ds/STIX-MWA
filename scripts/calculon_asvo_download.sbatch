#!/bin/bash
# Download ready MWA ASVO jobs as tarballs (<obsid>_<jobid>_ms.tar) onto the
# calculon NAS, with giant-squid from the mwa-demo image (CPU node, Apptainer).
# Submit the ASVO jobs first (scripts/download_1419137296.sh submit-...); this
# job waits until they are ready, then downloads. Rerunning resumes.
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
#SBATCH --time=12:00:00
#SBATCH --output=logs/%x-%j.out

set -euo pipefail
source "$HOME/mwa/mwa-env.sh"

OBSIDS="${OBSIDS:-1419137296 1419161128 1419109008}"
# mwa_data (the shared archive) belongs to andrea_zanelli and is not writable for
# us; this sibling folder is on the same NAS and keeps the same file naming.
DEST="${DEST:-/mnt/nas05/data02/MWA_data/data/mwa_data_sabbatical}"
KEYFILE="${KEYFILE:-$HOME/.config/mwa_asvo/api_key}"

[[ -r "$KEYFILE" ]] || { echo "missing $KEYFILE"; exit 1; }
# Passed through the environment (Apptainer forwards it), never on a command line.
export MWA_ASVO_API_KEY="$(<"$KEYFILE")"
mkdir -p "$DEST"

gs() { singularity exec -B /mnt/nas05/data02 "$MWA_DEMO_SIF" giant-squid "$@"; }

echo "node=$(hostname) dest=$DEST obsids=$OBSIDS"
gs list $OBSIDS
gs wait $OBSIDS
gs download --keep-tar --concurrent-downloads 3 -d "$DEST" $OBSIDS
ls -lh "$DEST"
