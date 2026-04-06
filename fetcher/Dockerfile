FROM python:3.12-slim

RUN pip install --no-cache-dir requests

COPY fetcher.py /app/fetcher.py

CMD ["python", "/app/fetcher.py"]

