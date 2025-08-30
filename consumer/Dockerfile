FROM python:3.9-slim

WORKDIR /app

#  Copy file
COPY consumer-requirements.txt .

# Install Python dependencies.
RUN pip install -r consumer-requirements.txt

COPY consumers.py .

CMD ["python", "consumers.py"]