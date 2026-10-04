FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY backend/requirements.txt .
RUN pip install -r requirements.txt
COPY backend/ .
COPY tracker/ tracker/
COPY scripts/rebuild_live.py /app/scripts/rebuild_live.py
COPY scripts/start-backend.sh /app/scripts/start-backend.sh
RUN useradd -u 10001 -m app
USER 10001
EXPOSE 5050
CMD ["sh", "scripts/start-backend.sh"]
