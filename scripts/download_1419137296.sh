#!/usr/bin/env bash
# Download MWA 1419137296 (2024-12-25 04:47:58 UTC, 240 s, type II burst) and
# two solar calibrators with the same 24 coarse channels, as measurement sets.
#
# Needs: giant-squid (cargo install mwa_giant_squid) and MWA_ASVO_API_KEY in
# the environment (from your ASVO profile; never commit it).
# Usage:  scripts/download_1419137296.sh [submit|status|download] [dest_dir]
set -euo pipefail

TARGET=1419137296            # OA004 Mondal2024B, 0.25 s / 160 kHz native, 45.7 GB raw
CALS=1419161128,1419109008   # D0006 Cal_solar_PKS2356-61 (+6.6 h), Cal_solar_HydA (-7.9 h); 2 s / 10 kHz native
DEST=${2:-$HOME/Projects/solar-burst-multiview/data}

command -v giant-squid >/dev/null || { echo "giant-squid not found: cargo install mwa_giant_squid"; exit 1; }
[ -n "${MWA_ASVO_API_KEY:-}" ] || { echo "MWA_ASVO_API_KEY is not set"; exit 1; }

case "${1:-status}" in
submit)
    # Target: 0.5 s keeps burst structure; 160 kHz = native. Edge flag must be a multiple of 160 kHz.
    giant-squid submit-conv $TARGET -p output=ms,avg_time_res=0.5,avg_freq_res=160,flag_edge_width=160
    # Calibrators: native 2 s, averaged to the target's 160 kHz.
    giant-squid submit-conv $CALS -p output=ms,avg_time_res=2,avg_freq_res=160,flag_edge_width=160
    ;;
status)
    giant-squid list $TARGET,$CALS
    ;;
download)
    mkdir -p "$DEST"
    for o in ${TARGET} ${CALS//,/ }; do
        mkdir -p "$DEST/$o"
        giant-squid download -d "$DEST/$o" "$o"
    done
    ;;
*) echo "usage: $0 [submit|status|download] [dest_dir]"; exit 2 ;;
esac
