"""Send a bounded synthetic demo stream to the trusted ingest API."""
import argparse
import asyncio
import json
import random
import time
import uuid

import aiohttp

EVENT_TYPES = ("page_view", "click", "signup", "purchase", "error")
PAGES = ("/", "/docs", "/pricing", "/blog", "/checkout")


def event(run_id, index, users):
    kind = random.choices(EVENT_TYPES, weights=(60, 25, 5, 5, 5))[0]
    return {"source": "demo", "event_id": f"sim:{run_id}:{index}", "type": kind,
            "user_id": f"sim-user-{index % users}",
            "value": round(random.uniform(10, 100), 2) if kind == "purchase" else 1,
            "props": {"path": random.choice(PAGES), "run": run_id}}


async def run(args):
    run_id = uuid.uuid4().hex
    started = time.monotonic()
    accepted = 0
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for offset in range(0, args.total, args.batch):
            count = min(args.batch, args.total - offset)
            due = started + offset / args.rate
            await asyncio.sleep(max(0, due - time.monotonic()))
            events = [event(run_id, offset + index, args.users) for index in range(count)]
            async with session.post(args.url.rstrip("/") + "/api/events", json={"events": events}) as response:
                body = await response.json()
                if response.status != 202 or body.get("accepted") != count:
                    raise RuntimeError(f"batch at {offset} failed: HTTP {response.status}, {body}")
                accepted += body["accepted"]
    elapsed = time.monotonic() - started
    print(json.dumps({"run_id": run_id, "accepted": accepted, "seconds": round(elapsed, 3),
                      "accepted_per_second": round(accepted / elapsed, 1), "source": "demo"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--total", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=100, help="target events per second")
    parser.add_argument("--batch", type=int, default=20)
    parser.add_argument("--users", type=int, default=500)
    args = parser.parse_args()
    if args.total < 1 or args.rate <= 0 or not 1 <= args.batch <= 1000 or args.users < 1:
        parser.error("total, rate, users must be positive; batch must be 1–1000")
    asyncio.run(run(args))
