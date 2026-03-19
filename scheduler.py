"""
Scheduler for Media Monitor
============================
Runs the pipeline at configurable intervals.

Usage:
    python scheduler.py                    # Default: every 60 minutes
    python scheduler.py --interval 30      # Every 30 minutes
    python scheduler.py --once             # Single run then exit

For production, prefer system cron:
    crontab -e
    0 * * * * cd /path/to/media_monitor && python monitor.py >> cron.log 2>&1
"""

import time
import sys
import signal
from datetime import datetime

from monitor import run_pipeline, log


def main():
    interval_min = 60  # default
    once = False

    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--interval" and i < len(sys.argv) - 1:
            interval_min = int(sys.argv[i + 1])
        if arg == "--once":
            once = True

    interval_sec = interval_min * 60

    # Graceful shutdown
    running = True
    def shutdown(signum, frame):
        nonlocal running
        log.info("Shutdown signal received. Finishing current cycle...")
        running = False
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info(f"Media Monitor Scheduler started (interval: {interval_min}min)")

    while running:
        start = time.time()
        log.info(f"Starting cycle at {datetime.now().isoformat()}")

        try:
            new = run_pipeline()
            elapsed = time.time() - start
            log.info(f"Cycle done in {elapsed:.1f}s — {new} new articles")
        except Exception as e:
            log.error(f"Pipeline error: {e}", exc_info=True)

        if once:
            break

        # Sleep until next cycle
        sleep_time = max(0, interval_sec - (time.time() - start))
        log.info(f"Sleeping {sleep_time/60:.0f}min until next cycle...")

        # Sleep in small chunks so we can catch SIGINT
        end_sleep = time.time() + sleep_time
        while running and time.time() < end_sleep:
            time.sleep(min(5, end_sleep - time.time()))

    log.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
