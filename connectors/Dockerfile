FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY wikipedia.py .
COPY fixtures/ fixtures/
RUN useradd -u 10001 -m app && mkdir /state && chown app:app /state
USER 10001
ENTRYPOINT ["python", "wikipedia.py"]
CMD ["--api", "http://backend:5050", "--checkpoint", "/state/wikipedia.json"]
