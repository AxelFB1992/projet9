FROM python:3.10-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install pyspark==4.0.0 pandas pyarrow

COPY ticket_processor.py .

CMD ["python", "ticket_processor.py"]
