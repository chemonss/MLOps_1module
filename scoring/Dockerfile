FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mlops /app/mlops
COPY scoring /app/scoring
COPY scripts /app/scripts
CMD ["python", "-m", "scoring.consumer"]
