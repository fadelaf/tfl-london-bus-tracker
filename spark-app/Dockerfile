FROM python:3.9-slim

WORKDIR /app

COPY spark-requirements.txt .

RUN pip install -r spark-requirements.txt

COPY data_cleaning.py .

CMD ["python", "data_cleaning.py"]