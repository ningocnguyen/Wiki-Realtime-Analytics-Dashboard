#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
if command -v docker >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then
  if command -v open >/dev/null 2>&1; then
    open -a Docker >/dev/null 2>&1 || true
    for ((attempt=0; attempt<30; attempt++)); do
      docker info >/dev/null 2>&1 && break
      sleep 2
    done
  fi
fi
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  ./run-local.sh --open "$@"
else
  ./run-native.sh --open "$@"
fi
status=$?
if [ "$status" -ne 0 ] && [ -t 0 ]; then
  printf '\nStartup failed. Press Return to close this window.'
  read -r _
fi
exit "$status"
