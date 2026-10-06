"""Measure HTTP ingest to WebSocket frame delivery for demo events."""
import argparse
import asyncio
import json
import math
import time
import uuid

import aiohttp


def percentile(values, fraction):
    values = sorted(values)
    return round(values[min(len(values) - 1, math.ceil(fraction * len(values)) - 1)], 2)


async def run(args):
    base = args.url.rstrip("/")
    run_id = uuid.uuid4().hex
    sent_at = {}
    seen = {}
    latency_ms = []
    complete = asyncio.Event()
    sockets = []
    listeners = []
    expected = args.clients * args.events
    connector = aiohttp.TCPConnector(limit=0)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async def listen(index, socket):
            async for message in socket:
                if message.type != aiohttp.WSMsgType.TEXT:
                    break
                frame = json.loads(message.data)
                if frame.get("kind") != "events":
                    continue
                arrived = time.perf_counter()
                for item in frame.get("events", []):
                    tag = item.get("event_id", "")
                    if tag not in sent_at or tag in seen[index]:
                        continue
                    seen[index].add(tag)
                    latency_ms.append((arrived - sent_at[tag]) * 1000)
                if len(latency_ms) >= expected:
                    complete.set()

        try:
            connection_started = time.perf_counter()
            async def open_client(index):
                socket = await asyncio.wait_for(
                    session.ws_connect(base + "/ws?source=demo", heartbeat=20), timeout=30)
                try:
                    message = await asyncio.wait_for(socket.receive(), timeout=20)
                    if message.type != aiohttp.WSMsgType.TEXT or json.loads(message.data).get("kind") != "hello":
                        raise RuntimeError(f"client {index} did not receive hello: {message.type}, {message.data}")
                    return socket
                except BaseException:
                    await socket.close()
                    raise

            for start in range(0, args.clients, 50):
                chunk = min(50, args.clients - start)
                opened = await asyncio.gather(*(
                    open_client(start + index) for index in range(chunk)))
                sockets.extend(opened)
                for index, socket in enumerate(opened, start):
                    seen[index] = set()
                    listeners.append(asyncio.create_task(listen(index, socket)))
            connect_seconds = time.perf_counter() - connection_started

            for index in range(args.events):
                tag = f"bench:{run_id}:{index}"
                sent_at[tag] = time.perf_counter()
                event = {"source": "demo", "event_id": tag, "type": "bench",
                         "user_id": f"bench-{run_id}", "props": {"run": run_id}}
                async with session.post(base + "/api/events", json=event,
                                        timeout=aiohttp.ClientTimeout(total=30)) as response:
                    body = await response.json()
                    if response.status != 202 or body.get("accepted") != 1:
                        raise RuntimeError(f"ingest failed: HTTP {response.status}, {body}")
                if args.interval:
                    await asyncio.sleep(args.interval)
            try:
                await asyncio.wait_for(complete.wait(), timeout=args.drain)
            except asyncio.TimeoutError:
                pass
            result = {"run_id": run_id, "clients_connected": len(sockets),
                      "connect_seconds": round(connect_seconds, 3),
                      "events_sent": args.events, "deliveries": len(latency_ms),
                      "expected_deliveries": expected,
                      "delivery_percent": round(100 * len(latency_ms) / expected, 2)}
            if latency_ms:
                result["latency_ms"] = {"p50": percentile(latency_ms, .50),
                                        "p95": percentile(latency_ms, .95),
                                        "p99": percentile(latency_ms, .99),
                                        "max": round(max(latency_ms), 2)}
            print(json.dumps(result))
            if len(latency_ms) != expected:
                raise RuntimeError("some WebSocket deliveries were missing")
        finally:
            await asyncio.gather(*(socket.close() for socket in sockets), return_exceptions=True)
            for listener in listeners:
                listener.cancel()
            await asyncio.gather(*listeners, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--events", type=int, default=50)
    parser.add_argument("--interval", type=float, default=.03)
    parser.add_argument("--drain", type=float, default=10)
    args = parser.parse_args()
    if not 1 <= args.clients <= 1000 or args.events < 1 or args.interval < 0 or args.drain <= 0:
        parser.error("clients must be 1–1000; events and drain positive; interval nonnegative")
    asyncio.run(run(args))
