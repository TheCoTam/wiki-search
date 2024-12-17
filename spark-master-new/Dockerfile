FROM bitnami/spark:latest
ENV SPARK_MODE=master

# Chuyển sang người dùng root để cài đặt các gói cần thiết
USER root

# Cập nhật danh sách gói và cài đặt Python 3 cùng các gói liên quan
RUN apt-get update && apt-get install -y \
    python3 python3-pip build-essential python3-dev curl wget

# Tạo các alias cho Python 3 và pip 3
RUN ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip

# Cài đặt môi trường Node.js và npm
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs

# Xác minh Node.js và npm đã được cài đặt
RUN node -v && npm -v

# Cài đặt thư viện Python để xử lý stem word
RUN pip install numpy nltk

# Cài đặt thư viện spark-xml
RUN wget -q https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar -O /tmp/spark-xml_2.12-0.15.0.jar
RUN mv /tmp/spark-xml_2.12-0.15.0.jar /opt/bitnami/spark/jars/

# Xóa các file tạm thời
RUN rm -f /tmp/spark-xml_2.12-0.15.0.jar

# Thiết lập các biến môi trường mặc định
ENV SPARK_MASTER_HOST=spark-master-new
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_REST_ENABLED=true
ENV SPARK_MASTER_REST_PORT=6066
ENV SPARK_MASTER_WEBUI_PORT=8080

# Khởi động Spark Master
CMD ["/opt/bitnami/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
