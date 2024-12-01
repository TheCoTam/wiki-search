from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lower, regexp_replace, expr, regexp_extract, udf, transform, trim, explode, split, count, collect_list, concat_ws, size, filter, row_number
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql.window import Window
from pyspark.ml.feature import StopWordsRemover
import re
from nltk.stem import PorterStemmer

# Định nghĩa hàm tách Infobox từ phần nội dung
schema = StructType([
    StructField("Infobox", StringType(), True),
    StructField("Remaining_Text", StringType(), True)
])

def end_infobox(text):
  matches = re.finditer(r"{{|}}", text)
  count = 0
  for match in matches:
    if match.group() == "{{":
      count += 1
    if match.group() == "}}":
      count -= 1
      if count == 0:
        return match.end()
        break
  return None

@udf(schema)
def extract_infobox(text):
  infobox = ''
  while True:
    match = re.search(r"{{infobox", text)
    if not match:
          break
    start = match.start()
    text = text[start:]
    end = end_infobox(text)

    if not end:
      break

    infobox += text[9:end-2]
    text = text[end:]

  return (infobox.strip(), text.strip())


# Định nghĩa hàm chuyển từ về dạng gốc
stem_cache = {}

@udf(ArrayType(StringType()))
def stem_words(words):
    if words is None:
        return []
    return [stem_cache.setdefault(word, stemmer.stem(word)) for word in words]

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("Wiki XML").master("spark://spark-master:7077").config("spark.executor.memory", "2g").getOrCreate()
stemmer = PorterStemmer()

# Đọc dữ liệu từ file (chỉ lấy 5 cột đầu của dữ liệu)
_ = spark.read.format("xml").option("rowTag", "page").load('hdfs://namenode:9000/user/root/input/mini_wiki_dump.txt')
_ = _.select(
    initcap(col("id")).alias("Id"),
    initcap(col("title")).alias("Title"),
    initcap(col("revision.text._VALUE")).alias("Text")
)
df = _.limit(5)
_.unpersist()

# Lọc và trích xuất thông tin từ phần nội dung
df = df.withColumn("Title", lower(col("Title")))
df = df.withColumn("Text", lower(col("Text")))
# remove {{cite **}} or {{vcite **}}
df = df.withColumn("Text", regexp_replace(col("Text"), r'\{\{(cite|vcite)\s+[^\}]*\}\}', ''))
# remove Punctuation (. , ; _ ( ) / " ' =)
df = df.withColumn("Text", regexp_replace(col("Text"), r'[.,;_()/"\'=]', ' '))
# remove [[file:]]
df = df.withColumn("Text", regexp_replace(col("Text"), r'\[\[file:[^\]]*\]\]', ''))
# remove <...> tag
df = df.withColumn("Text", regexp_replace(col("Text"), r'<[^>]*>', ''))
# remove none ASCII characters
df = df.withColumn("Text", regexp_replace(col("Text"), r'[^\x00-\x7F]+', ''))
# extract categories
df = df.withColumn("Categories", expr("regexp_extract_all(Text, '\\\\\\[\\\\[category:(.*?)\\\\]\\]', 1)"))
# extract infobox
df = df.withColumn("Extracted", extract_infobox(df["Text"]))
df = df.withColumn("Infobox", df["Extracted"].getItem("Infobox"))
df = df.withColumn("Text", df["Extracted"].getItem("Remaining_Text"))
df = df.drop("Extracted")
df = df.withColumn("Infobox", regexp_replace(col("Infobox"), r'\n', ' '))
# extract external links
df = df.withColumn("External_Links",regexp_extract("text", r"(?s).*external links\s*([\s\S]*?)\s*\[\[category", 1))
# remove external links from Text
df = df.withColumn("Text", regexp_extract("Text", r"(?s)(.*)external links", 1))
# remove junk from Text
df = df.withColumn("Text", regexp_replace(col("Text"), r"[~`!@#$%\-^*+{}\[\]|\\<>/?,:&]", " "))
df = df.withColumn("Text", regexp_replace(col("Text"), r"\n", " "))
# remove junk from Categories
df = df.withColumn("Categories", transform("Categories", lambda x: trim(regexp_replace(x, r"[~`!@#$%\-^*+{}\[\]|\\<>/?,:&]", ""))))
# remove junk from External Links
df = df.withColumn("External_Links", regexp_replace(col("External_Links"), r"[~`!@#$%\-^*+{}\[\]|\\<>/?,:&]", " "))
df = df.withColumn("External_Links", regexp_replace(col("External_Links"), r"\n", " "))
# reomve junk from Infobox
df = df.withColumn("Infobox", regexp_replace("Infobox", r'[^a-zA-Z0-9_]', ' '))
# remove junk from Title
df = df.withColumn("Title", regexp_replace(col("Title"), r"[~`!@#$%\-^*+{}\[\]|\\<>/?,:&]", " "))

# Loại bỏ các khoảng trắng dư thừa ở đầu, thân và cuối của phần nội dung sau khi xử lý
df = df.withColumn("Categories", concat_ws(" ", col("Categories")))
df = df.withColumn("Title", trim(regexp_replace(col("Title"), "\\s+", " "))).withColumn("Text", trim(regexp_replace(col("Text"), "\\s+", " "))).withColumn("Infobox", trim(regexp_replace(col("Infobox"), "\\s+", " "))).withColumn("External_Links", trim(regexp_replace(col("External_Links"), "\\s+", " ")))

# TODO: Lưu dữ liệu vào HBase ở đây

# Chuyển nội dung các cột về dạng mảng để tiếp tục xử lý
df = df.withColumn("Text", split(df.Text, " ")).withColumn("Title", split(df.Title, " ")).withColumn("Categories", split(df.Categories, " ")).withColumn("Infobox", split(df.Infobox, " ")).withColumn("External_Links", split(df.External_Links, " "))

columns = ["Title", "Text", "Categories", "Infobox", "External_Links"]

# Loại bỏ các stop_words
for column in columns:
  stopwords_remover = StopWordsRemover(inputCol=column, outputCol=column + "_processed")
  df = stopwords_remover.transform(df).drop(column).withColumnRenamed(column + "_processed", column)

# Chuyển các từ về dạng gốc bằng hàm đã được định nghĩa
for column in columns:
  df = df.withColumn(column, stem_words(df[column]))
df.cache()

# Đếm các từ xuất hiện tại cột Title
words_from_title = df.select("Id", "Title").withColumn("Title", filter(col("Title"), lambda x: x != "")).filter(size(col("Title")) != 0)
words_from_title = words_from_title.withColumn("Word", explode("Title")).drop("Title").groupBy("Id", "Word").agg(count("Word").alias("Title"))

# Đếm các từ xuất hiện trong cột Categories
words_from_categories = df.select(["Id", "Categories"]).withColumn("Categories", filter(col("Categories"), lambda x: x != "")).filter(size(col("Categories")) != 0)
words_from_categories = words_from_categories.withColumn("Word", explode("Categories")).drop("Categories").groupBy("Id", "Word").agg(count("Word").alias("Categories"))

# Đếm các từ xuất hiện trong Infobox
words_from_infobox = df.select("Id", "Infobox").withColumn("Infobox", filter(col("Infobox"), lambda x: x != "")).filter(size(col("Infobox")) != 0)
words_from_infobox = words_from_infobox.withColumn("Word", explode("Infobox")).drop("Categories").groupBy("Id", "Word").agg(count("Word").alias("Infobox"))

# Đếm các từ xuất hiện trong External_Links
words_from_external_links = df.select("Id", "External_Links").withColumn("External_Links", filter(col("External_Links"), lambda x: x != "")).filter(size(col("External_Links")) != 0)
words_from_external_links = words_from_external_links.withColumn("Word", explode("External_Links")).drop("External_Links").groupBy("Id", "Word").agg(count("Word").alias("External_Links"))

# Đếm các từ xuất hiện trong Text
words_from_text = df.select("Id", "Text").withColumn("Text", filter(col("Text"), lambda x: x != "")).filter(size(col("Text")) != 0)
words_from_text = words_from_text.withColumn("Word", explode("Text")).drop("Text").groupBy("Id", "Word").agg(count("Word").alias("Text"))

# Hợp nhất các bảng từ để tổng hợp
df.unpersist()
df = words_from_categories.join(words_from_title, ["Id", "Word"], 'outer')
df = df.join(words_from_text, ["Id", "Word"], 'outer')
df = df.join(words_from_infobox, ["Id", "Word"], 'outer')
df = df.join(words_from_external_links, ["Id", "Word"], 'outer')
df = df.fillna({
    "Title": 0,
    "Text": 0,
    "Categories": 0,
    "Infobox": 0,
    "External_Links": 0
})

# Nhóm các từ giống nhau lại và ghi chỉ mục tính được
final_index = df.groupBy("Word").agg(collect_list(concat_ws(":", "id", concat_ws("-",col("Title"), col("Text"), col("Categories"), col("Infobox"), col("External_Links")))).alias("Document_Counts"))
df.unpersist()
final_index = final_index.withColumn("Document_Counts", concat_ws("#", col("Document_Counts")))
final_index.cache()
# final_index = final_index.orderBy("Word")
# print("Number of all words:", final_index.count())

batch_size = 100
num_batches = (final_index.count() // batch_size) + 1

window_spec = Window.orderBy("Word")
df_with_index = final_index.withColumn("Batch_Number", (row_number().over(window_spec) - 1) / batch_size).withColumn("Batch_Number", col("Batch_number").cast("int"))
final_index.unpersist()
df_with_index.cache()
# df_with_index.show()

for i in range(num_batches):
  tmp_df = df_with_index.filter(col("Batch_Number") == i).drop("Batch_Number")
  tmp_df.write.parquet(f"hdfs://namenode:9000/user/root/indexes/index_{i}", mode="overwrite")
  tmp_df.unpersist()
  # print("Saved batch ", i+1)
  if i == 0:
    df_2nd = tmp_df.limit(1)
  else:
    df_2nd = df_2nd.union(tmp_df.limit(1))

df_with_index.unpersist()
# Xóa cột không cần thiết và lưu secondary index
df_2nd = df_2nd.drop("Document_Counts")
# df_2nd.show()
# Lưu file secondary index
# df_2nd = df_2nd.coalesce(1)
df_2nd.coalesce(1).write.mode("overwrite").text("hdfs://namenode:9000/user/root/indexes/secondary_index")

spark.stop()