"""Verify WebSockets through the proxy; this is a correctness check, not a load benchmark."""
import argparse
import asyncio
import json
import uuid

import aiohttp


async def receive_kind(socket, kind, timeout=5):
    async with asyncio.timeout(timeout):
        while True:
            message = await socket.receive_json()
            if message["kind"] == kind:
                return message


async def main(args):
    base = args.url.rstrip("/")
    sockets = []
    async with aiohttp.ClientSession() as session:
        try:
            workers = set()
            for _ in range(args.clients):
                socket = await session.ws_connect(base + "/ws?source=demo")
                sockets.append(socket)
                workers.add((await receive_kind(socket, "hello"))["worker_id"])
            wiki = await session.ws_connect(base + "/ws?source=wikipedia")
            sockets.append(wiki)
            await receive_kind(wiki, "hello")
            event_id = "ws-check:" + uuid.uuid4().hex
            event = {"source": "demo", "event_id": event_id, "type": "page_view", "user_id": "ws-check"}
            async with session.post(base + "/api/events", json=event) as response:
                assert response.status == 202, await response.text()
                assert (await response.json())["accepted"] == 1

            async def delivered(socket):
                async with asyncio.timeout(5):
                    while True:
                        frame = await socket.receive_json()
                        if frame["kind"] == "events":
                            assert all(item["source"] == "demo" for item in frame["events"])
                            if any(item["event_id"] == event_id for item in frame["events"]):
                                return True

            deliveries = await asyncio.gather(*(delivered(socket) for socket in sockets[:-1]))
            # Exercise Wikipedia filtering while real traffic continues, or until timeout.
            try:
                async with asyncio.timeout(1):
                    while True:
                        frame = await wiki.receive_json()
                        if frame["kind"] == "events":
                            assert all(item["source"] == "wikipedia" for item in frame["events"])
            except asyncio.TimeoutError:
                pass
            async with session.post(base + "/api/events", json=event) as response:
                assert response.status == 202
                assert (await response.json())["accepted"] == 0
            hosts = {worker.split(":", 1)[0] for worker in workers}
            if args.require_replicas:
                assert len(hosts) >= 2, "connections did not reach two backend containers"
            print(json.dumps({"clients": args.clients, "deliveries": sum(deliveries),
                              "workers": len(workers), "backend_containers": len(hosts),
                              "source_filtering": "passed", "retry_deduplication": "passed"}))
        finally:
            await asyncio.gather(*(socket.close() for socket in sockets), return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--clients", type=int, default=12)
    parser.add_argument("--require-replicas", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.clients <= 100:
        parser.error("clients must be 1–100; use the later load benchmark for larger runs")
    asyncio.run(main(args))
