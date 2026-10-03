from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.ingest_seconds = Histogram("rta_ingest_seconds", "Batch ingestion duration", registry=self.registry)
        self.events = Counter("rta_events_ingested", "Newly stored events", ["source"], registry=self.registry)
        self.clients = Gauge("rta_ws_clients", "WebSocket clients on this worker", registry=self.registry)
        self.frames = Counter("rta_ws_frames", "WebSocket frames sent", registry=self.registry)
        self.drops = Counter("rta_ws_dropped_frames", "Frames dropped by bounded queues", ["queue"], registry=self.registry)
        self.cache_errors = Counter("rta_live_cache_errors", "Failed Redis updates after commit", registry=self.registry)
