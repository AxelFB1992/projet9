FROM python:3.10-slim
WORKDIR /app
RUN pip install kafka-python
COPY ticket_producer.py .
CMD ["python", "ticket_producer.py"]
