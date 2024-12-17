FROM  bitnami/spark:latest
ENV SPARK_MODE=master

# Cài đặt python3
USER root
RUN apk add --no-cache python3 py3-pip build-base python3-dev
RUN ln -sf /usr/bin/python3 /usr/bin/python
RUN ln -sf /usr/bin/pip3 /usr/bin/pip

# Cài đặt thư viện python để xử lý stem word
# RUN apk add cmake libarrow-dev libarrow-python-dev py3-numpy
RUN pip install numpy nltk
# pandas pyarrow

# Cài đặt thư viện python để xử lý Lưu trữ dữ liệu vào HBase
# RUN pip install happybase

# Cài đặt thư viện spark-xml
RUN wget -q https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar -O /tmp/spark-xml_2.12-0.15.0.jar
RUN mv /tmp/spark-xml_2.12-0.15.0.jar /spark/jars/

# # Cài đặt thư viện hbase-spark-connector
# RUN wget -q https://repo1.maven.org/maven2/org/apache/hbase/connectors/spark/hbase-spark/1.0.1/hbase-spark-1.0.1.jar -O /tmp/hbase-spark-1.0.1.jar
# RUN mv /tmp/hbase-spark-1.0.1.jar /spark/jars/

# Xóa bỏ file tạm thời
RUN rm -f /tmp/spark-xml_2.12-0.15.0.jar
# /tmp/hbase-spark-1.0.1.jar
# Cấu hình các tham số Spark (nếu bạn muốn cấu hình thêm)
# COPY spark-defaults.conf /spark/conf/spark-defaults.conf
# COPY search.py /tmp/search.py
# COPY extract_info.py /tmp/extract_info.py

# # thư viện để kết nối đến RabbitMQ
# RUN pip install pika

# Cài đặt các biến môi trường mặc định
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_REST_ENABLED=true
ENV SPARK_MASTER_REST_PORT=6066
ENV SPARK_MASTER_WEBUI_PORT=8080

# Khởi động Spark Master
CMD ["/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]