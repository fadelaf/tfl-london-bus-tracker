FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_ingest.py .
COPY producers.py .
CMD ["python","data_ingest.py"]