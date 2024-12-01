FROM bde2020/spark-base:3.1.1-hadoop3.2

ENV SPARK_MODE=worker

USER root
RUN apk add --no-cache python3 py3-pip build-base python3-dev
RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN ln -sf /usr/bin/pip3 /usr/bin/pip

RUN pip install numpy nltk happybase

RUN wget -q https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar -O /tmp/spark-xml_2.12-0.15.0.jar
RUN mv /tmp/spark-xml_2.12-0.15.0.jar /spark/jars/

# # Cài đặt thư viện hbase-spark-connector
# RUN wget -q https://repo1.maven.org/maven2/org/apache/hbase/connectors/spark/hbase-spark/1.0.1/hbase-spark-1.0.1.jar -O /tmp/hbase-spark-1.0.1.jar
# RUN mv /tmp/hbase-spark-1.0.1.jar /spark/jars/

# Xóa bỏ file tạm thời
RUN rm -f /tmp/spark-xml_2.12-0.15.0.jar
# /tmp/hbase-spark-1.0.1.jar

ENTRYPOINT ["/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]