from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PurchasesAnalysis").getOrCreate()

# Загружаем CSV-файлы
users = spark.read.csv(
    "users.csv",
    header=True,
    inferSchema=True,
)
purchases = spark.read.csv(
    "purchases.csv",
    header=True,
    inferSchema=True,
)
products = spark.read.csv(
    "products.csv",
    header=True,
    inferSchema=True,
)

# Проверим, что данные загрузились
users.show(5)
purchases.show(5)
products.show(5)


users = users.dropna()
purchases = purchases.dropna()
products = products.dropna()

purchases_with_products = purchases.join(products, "product_id")

purchases_with_products = purchases_with_products.withColumn(
    "total", purchases_with_products["quantity"] * purchases_with_products["price"]
)

total_by_category = purchases_with_products.groupBy("category").agg(
    F.sum("total").alias("total_spent")
)

total_by_category.show()

all_data = purchases_with_products.join(users, "user_id")
age_18_25 = all_data.filter((all_data.age >= 18) & (all_data.age <= 25))

total_by_category_18_25 = age_18_25.groupBy("category").agg(
    F.sum("total").alias("total_spent")
)

total_by_category_18_25.show()

total_sum = total_by_category_18_25.agg(F.sum("total_spent")).collect()[0][0]

share_by_category = total_by_category_18_25.withColumn(
    "share_percent", (F.col("total_spent") / total_sum) * 100
)

share_by_category.show()

top3 = share_by_category.orderBy(F.desc("share_percent")).limit(3)
top3.show()

spark.stop()
