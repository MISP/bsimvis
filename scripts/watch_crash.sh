#!/bin/sh
# Log memory/load every 2s so a hard freeze leaves evidence behind.
# Usage: scripts/watch_crash.sh [logfile]   (default /tmp/bsimvis-watch.log)
# Read after reboot: which process RSS was climbing right before the freeze.
LOG=${1:-/tmp/bsimvis-watch.log}
while :; do
    {
        echo "=== $(date +%T) load:$(cut -d' ' -f1-3 /proc/loadavg) $(free -m | awk '/Mem:/{print "used="$3"M avail="$7"M"}')"
        ps -eo rss=,pid=,comm= --sort=-rss | head -8 | awk '{printf "  %6d MB  pid=%s %s\n", $1/1024, $2, $3}'
    } >> "$LOG"
    sync "$LOG" 2>/dev/null || sync   # ponytail: flush, a freeze never flushes the page cache
    sleep 2
done
