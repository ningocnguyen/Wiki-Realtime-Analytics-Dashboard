"""Verify two Gunicorn-style workers contribute to one scrape per backend pod."""
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1] / "backend"


class MultiprocessMetricsTests(unittest.TestCase):
    def test_counters_aggregate_and_dead_worker_gauge_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = {**os.environ, "PROMETHEUS_MULTIPROC_DIR": directory,
                           "PYTHONPATH": str(BACKEND)}
            pids = []
            for amount in (2, 3):
                result = subprocess.check_output([sys.executable, "-c",
                    "import os; from app.metrics import Metrics; m=Metrics(); "
                    f"m.events.labels(source='wikipedia').inc({amount}); m.clients.set({amount}); print(os.getpid())"],
                    env=environment, text=True)
                pids.append(int(result.strip()))
            output = subprocess.check_output([sys.executable, "-c",
                "from prometheus_client import multiprocess; from app.metrics import Metrics; "
                f"multiprocess.mark_process_dead({pids[0]}); print(Metrics().render().decode())"],
                env=environment, text=True)
            self.assertIn('rta_events_ingested_total{source="wikipedia"} 5.0', output)
            self.assertIn("rta_ws_clients 3.0", output)
