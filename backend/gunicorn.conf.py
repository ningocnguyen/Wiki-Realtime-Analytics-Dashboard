import os

bind = f"0.0.0.0:{os.getenv('PORT', '5050')}"
worker_class = "gevent"
workers = int(os.getenv("WEB_CONCURRENCY", "2"))
worker_connections = 2000
timeout = 60
graceful_timeout = 20
accesslog = "-"


def worker_exit(server, worker):
    app = getattr(worker, "wsgi", None)
    if app is not None and hasattr(app, "extensions"):
        cleanup = app.extensions.get("close_resources")
        if cleanup:
            cleanup()
