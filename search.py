from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from nltk.stem import PorterStemmer
import re

stop_words = [
    'a',
    'about',
    'above',
    'after',
    'again',
    'against',
    'ain',
    'all',
    'am',
    'an',
    'and',
    'any',
    'are',
    'aren',
    "aren't",
    'as',
    'at',
    'be',
    'because',
    'been',
    'before',
    'being',
    'below',
    'between',
    'both',
    'but',
    'by',
    'can',
    'couldn',
    "couldn't",
    'd',
    'did',
    'didn',
    "didn't",
    'do',
    'does',
    'doesn',
    "doesn't",
    'doing',
    'don',
    "don't",
    'down',
    'during',
    'each',
    'few',
    'for',
    'from',
    'further',
    'had',
    'hadn',
    "hadn't",
    'has',
    'hasn',
    "hasn't",
    'have',
    'haven',
    "haven't",
    'having',
    'he',
    'her',
    'here',
    'hers',
    'herself',
    'him',
    'himself',
    'his',
    'how',
    'i',
    'if',
    'in',
    'into',
    'is',
    'isn',
    "isn't",
    'it',
    "it's",
    'its',
    'itself',
    'just',
    'll',
    'm',
    'ma',
    'me',
    'mightn',
    "mightn't",
    'more',
    'most',
    'mustn',
    "mustn't",
    'my',
    'myself',
    'needn',
    "needn't",
    'no',
    'nor',
    'not',
    'now',
    'o',
    'of',
    'off',
    'on',
    'once',
    'only',
    'or',
    'other',
    'our',
    'ours',
    'ourselves',
    'out',
    'over',
    'own',
    're',
    's',
    'same',
    'shan',
    "shan't",
    'she',
    "she's",
    'should',
    "should've",
    'shouldn',
    "shouldn't",
    'so',
    'some',
    'such',
    't',
    'than',
    'that',
    "that'll",
    'the',
    'their',
    'theirs',
    'them',
    'themselves',
    'then',
    'there',
    'these',
    'they',
    'this',
    'those',
    'through',
    'to',
    'too',
    'under',
    'until',
    'up',
    've',
    'very',
    'was',
    'wasn',
    "wasn't",
    'we',
    'were',
    'weren',
    "weren't",
    'what',
    'when',
    'where',
    'which',
    'while',
    'who',
    'whom',
    'why',
    'will',
    'with',
    'won',
    "won't",
    'wouldn',
    "wouldn't",
    'y',
    'you',
    "you'd",
    "you'll",
    "you're",
    "you've",
    'your',
    'yours',
    'yourself',
    'yourselves'
]

spark = SparkSession.builder.appName("Search").master("spark://spark-master:7077").config("spark.executor.memory", "2g").getOrCreate()
stemmer = PorterStemmer()

# Tìm kiếm từ bằng binary search
def search(arr, word):
    left = 0
    right = len(arr) - 1

    while left < right:
        mid = (left + right) // 2
        if arr[mid] < word:
            left = mid + 1
        else:
            right = mid

    return left if arr[left] <= word else left - 1

# Tính điểm của từng document
def calc_score(input):
  docs = input.split("#")
  for doc in docs:
    docId, count = doc.split(":")
    title, text, categories, infobox, external_links = count.split("-")
    score = weight['t'] * int(title) + weight['b'] * int(text) + weight['i'] * int(infobox) + weight['c'] * int(categories) + weight['e'] * int(external_links) + weight['r']
    print(f"DocId: {docId}, Score: {score}")

# Đọc file secondary index
df_2nd = spark.read.text("hdfs://namenode:9000/user/root/indexes/secondary_index")
rdd = df_2nd.rdd.map(lambda row: row["value"])
secondary_index_list = rdd.collect()

# Trọng số của các trường
weight = {
    't': 500,
    'b': 1,
    'i': 50,
    'c': 50,
    'r': 50,
    'e': 50,
}

# Từ cần tìm kiếm
search_string = "1914 abeldo"
word_list = re.findall(r'[a-zA-Z0-9]+', search_string.lower())
word_list = [stemmer.stem(word) for word in word_list if word not in stop_words]

for word in word_list:
    # Tìm chỉ số của file index có chứa từ cần tìm
    target_index = search(secondary_index_list, word)

    # Đọc file index chứa từ cần tìm
    target_df = spark.read.parquet(f"hdfs://namenode:9000/user/root/indexes/index_{target_index}")

    # Lấy thông tin về các lần xuất hiện của từ trong các document
    documents_count = target_df.filter(col("Word") == search_word).select(col("Document_Counts")).collect()

    result = ""
    if documents_count:
    result = documents_count[0]["Document_Counts"]

    calc_score(result)