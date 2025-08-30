FROM bitnami/spark:3.5.0

WORKDIR /app

COPY spark-requirements.txt .
RUN pip install --no-cache-dir -r spark-requirements.txt

COPY data_cleaning.py .

CMD ["spark-submit", "data_cleaning.py"]