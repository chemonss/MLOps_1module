FROM python:3.11-slim

WORKDIR /app

COPY results_writer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY results_writer/ /app/results_writer/

CMD ["python", "-m", "results_writer.consumer"]
