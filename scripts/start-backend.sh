#!/bin/sh
set -eu

if [ -n "${PROMETHEUS_MULTIPROC_DIR:-}" ]; then
  mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
  # The directory belongs to this one backend pod. A restart must not reuse old worker files.
  find "$PROMETHEUS_MULTIPROC_DIR" -maxdepth 1 -type f -name '*.db' -delete
fi

exec gunicorn -c gunicorn.conf.py wsgi:app
