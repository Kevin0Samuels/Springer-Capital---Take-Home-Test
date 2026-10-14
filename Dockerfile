FROM apache/spark:3.5.0-python3

USER root

WORKDIR /app

COPY requirements.txt /app/
COPY pipeline.py /app/

RUN pip install --no-cache-dir -r requirements.txt

ENV DATA_DIR=/app/data
ENV OUTPUT_FILE=/app/output/result_data.csv

CMD ["python3", "pipeline.py"]
