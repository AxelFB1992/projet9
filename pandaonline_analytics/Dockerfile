FROM python:3.10-alpine
USER root
RUN adduser -D muser
RUN apk update && apk add bash openjdk11 wget
RUN pip install pyspark==3.3.0
RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz -P /home/muser
RUN cd /home/muser && tar -xzvf /home/muser/spark-3.3.0-bin-hadoop3.tgz
RUN chmod a+x /home/muser/spark-3.3.0-bin-hadoop3/bin/spark-submit
ADD classified_streamer.py /home/muser
USER muser
ENTRYPOINT ["/home/muser/spark-3.3.0-bin-hadoop3/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0","/home/muser/classified_streamer.py"]
