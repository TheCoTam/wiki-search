import happybase
from pyspark.sql import SparkSession

# Kết nối tới HBase thông qua Thrift
connection = happybase.Connection(host='hbase', port=9090)

# Tạo bảng trong HBase
table_name = "DOCUMENTS"

table_list = [table.decode('utf-8') for table in connection.tables()]

# Kiểm tra nếu bảng đã tồn tại, xóa bảng cũ
if table_name not in table_list:
    connection.create_table(table_name, {'cf': dict()})
    print(f"Bảng {table_name} đã được tạo.")

# Ghi dữ liệu vào bảng HBase
table = connection.table(table_name)

Dữ liệu mẫu
data = [
    (1, "Title 1", "Text for document 1", "Category A", "Infobox 1", "http://link1.com"),
    (2, "Title 2", "Text for document 2", "Category B", "Infobox 2", "http://link2.com"),
]

for id, title, text, categories, indobox, external_links in data:
    table.put(str(id).encode('utf-8'), {
        b'cf:Title': title.encode('utf-8'),
        b'cf:Text': text.encode('utf-8'),
        b'cf:Categories': categories.encode('utf-8'),
        b'cf:Indobox': indobox.encode('utf-8'),
        b'cf:External_Links': external_links.encode('utf-8'),
    })
print("Dữ liệu đã được ghi vào HBase.")

# # Kiểm tra dữ liệu đã ghi
# rows = table.scan()
# for key, data in rows:
#     print(key, data)

# Kết thúc kết nối
connection.close()

# Nếu dùng Spark, dữ liệu có thể được đọc từ RDD/DataFrame
# spark = SparkSession.builder.appName("HBaseData").master("spark://spark-master:7077").config("spark.executor.memory", "2g").getOrCreate()
# rdd = spark.sparkContext.parallelize(data)
# print(rdd.collect())
