FROM python:3.9-slim

WORKDIR /app
COPY dashboard-requirements.txt .
RUN pip install --no-cache-dir -r dashboard-requirements.txt

COPY stream.py .
EXPOSE 8501
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501","--server.address=0.0.0.0" ]
