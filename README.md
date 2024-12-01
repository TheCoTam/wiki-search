# Quick Setup

B1: Chuẩn bị file wiki_dump

B2: Chạy các container

```
  docker-compose up -d
```

Đối với lần đầu sử dụng hoặc muốn build lại các container

```
  docker-compose up --build -d
```

B3: Copy file wiki_dump vào hdfs để spark có thể sử dụng

  - Tạo thư mục để lưu file

```
  docker exec -it namenode hdfs dfs -mkdir -p /user/root/input
```
  - Copy file dump vào hdfs:

```
  docker cp /path/to/your_file namenode:/tmp
  docker exec -it namenode hdfs dfs -put /tmp/file_name /user/root/input
```  

B4: Sử dụng Spark tạo danh sách indexes của các từ và lưu vào Hadoop
  - Thay đổi đường dẫn trong file extract_info tại dòng 9

  - Copy file xử lý vào Spark:

```
  docker cp extract_info.py spark-master:/tmp
```
  - Chạy file xử lý với spark-submit

```
  docker exec -it spark-master /spark/bin/spark-submit /tmp/extract_info.py
```
Sau khi chạy xong lệnh trên, danh sách chỉ mục sẽ được lưu trữ trên hadoop.

Truy cập địa chỉ: http://localhost:9870, lựa chọn "Utilities" (trên thanh công cụ) -> "Browse the file system" -> chọn thư mục "user" -> "root" -> "indexes"

# Quick Search

B1: thay từ cần tìm kiếm trong file search.py dòng 7

B2: Copy file vào Spark-master container

```
docker cp search.py spark-master:/tmp
```
B3: Chạy file search và nhận kết quả

```
docker exec -it spark-master /spark/bin/spark-submit /tmp/search.py
```

# Note

Kiểm tra các Session của Spark tại: http://localhost:8080