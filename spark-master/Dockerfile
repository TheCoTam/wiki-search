FROM bde2020/spark-base:3.1.1-hadoop3.2

ENV SPARK_MODE=master

USER root
RUN apk add --no-cache python3 py3-pip build-base python3-dev
RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN ln -sf /usr/bin/pip3 /usr/bin/pip

RUN pip install numpy nltk

RUN wget -q https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar -O /tmp/spark-xml_2.12-0.15.0.jar
RUN mv /tmp/spark-xml_2.12-0.15.0.jar /spark/jars/

CMD ["/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]