FROM python:3.9-slim

WORKDIR /app

# Copy requirements first
COPY spark-requirements.txt .  

# Install Python dependencies
RUN pip install --no-cache-dir -r spark-requirements.txt

COPY data_cleaning.py .
CMD ["python", "data_cleaning.py"] 