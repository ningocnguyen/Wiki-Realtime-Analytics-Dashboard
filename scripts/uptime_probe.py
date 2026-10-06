"""Sample /readyz during a rollout and report observed availability."""
import argparse
import json
import time
import urllib.error
import urllib.request


def run(args):
    start = time.monotonic()
    checks = failures = 0
    while True:
        try:
            with urllib.request.urlopen(args.url.rstrip("/") + "/readyz", timeout=args.timeout) as response:
                healthy = response.status == 200
        except (urllib.error.URLError, TimeoutError, OSError):
            healthy = False
        checks += 1
        failures += not healthy
        if time.monotonic() - start >= args.seconds:
            break
        time.sleep(args.interval)
    percent = round(100 * (checks - failures) / checks, 3)
    print(json.dumps({"checks": checks, "failures": failures, "uptime_percent": percent,
                      "elapsed_seconds": round(time.monotonic() - start, 2)}))
    if percent < args.min_uptime:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--timeout", type=float, default=2)
    parser.add_argument("--min-uptime", type=float, default=0)
    args = parser.parse_args()
    if args.seconds <= 0 or args.interval <= 0 or args.timeout <= 0 or not 0 <= args.min_uptime <= 100:
        parser.error("seconds, interval, timeout must be positive; min-uptime must be 0–100")
    run(args)
