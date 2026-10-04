import os

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, multiprocess


class Metrics:
    def __init__(self):
        self.multiprocess = bool(os.getenv("PROMETHEUS_MULTIPROC_DIR"))
        self.registry = None if self.multiprocess else CollectorRegistry()
        self.ingest_seconds = Histogram("rta_ingest_seconds", "Batch ingestion duration", registry=self.registry)
        self.events = Counter("rta_events_ingested", "Newly stored events", ["source"], registry=self.registry)
        self.clients = Gauge("rta_ws_clients", "Open WebSocket clients", registry=self.registry,
                             multiprocess_mode="livesum")
        self.frames = Counter("rta_ws_frames", "WebSocket frames sent", registry=self.registry)
        self.drops = Counter("rta_ws_dropped_frames", "Frames dropped by bounded queues", ["queue"], registry=self.registry)
        self.cache_errors = Counter("rta_live_cache_errors", "Failed Redis updates after commit", registry=self.registry)

    def render(self):
        if self.multiprocess:
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
            return generate_latest(registry)
        return generate_latest(self.registry)
