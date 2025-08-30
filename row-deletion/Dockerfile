FROM python:3.9-slim

WORKDIR /app

COPY deletion-requirements.txt .
RUN pip install -r deletion-requirements.txt

COPY row_deletion.py .

CMD ["python", "row_deletion.py"]