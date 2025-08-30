FROM pythonL3.9-slim

WORKDIR /app

COPY deletion-requirements.txt
RUN pip install --no-cache-dir -r deletion-requirements.txt

COPY row_deletion.py .

CMD ["python", "row_deletion.pys"]