"""Lab 07 - Bronze to silver with Spark DataFrames.

Run from the project root:
    spark-submit --packages "$ICEBERG_PACKAGE" spark_labs/lab07_silver.py
"""

import os
import re
import sys
import timeit

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RUN_ICEBERG = True


def title(text):
    print(f"\n{'=' * 70}\n{text}\n{'=' * 70}")


os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
BASE = f"s3a://{BUCKET}"

# iceberg-spark-runtime-<spark major.minor>_<scala version>
SPARK_MINOR = ".".join(pyspark.__version__.split(".")[:2])
SCALA = "2.13" if SPARK_MINOR.startswith("4") else "2.12"
ICEBERG_PACKAGE = os.environ.get(
    "ICEBERG_PACKAGE", f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MINOR}_{SCALA}:1.11.0"
)

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"{BASE}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version, "| Iceberg package:", ICEBERG_PACKAGE)

# ---------------------------------------------------------------------------
title("Bronze layer")
users_bronze = spark.read.csv(
    f"{BASE}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"{BASE}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
# Exercise 3: keep the bronze DataFrames in memory, S3 is read only once
users_bronze = users_bronze.cache()
orders_bronze = orders_bronze.cache()

users_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)

# ---------------------------------------------------------------------------
title("Silver users - DataFrame API")
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
# regexp_extract() returns "" instead of NULL when there is no match
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)

# ---------------------------------------------------------------------------
title("Silver orders - DataFrame API with window deduplication")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())

duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()

dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

# ---------------------------------------------------------------------------
title("The same transformation with Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("DataFrame and SQL versions agree: OK")

print("--- Catalyst plans: DataFrame vs SQL (same physical operators) ---")
stg_orders_sql.explain()

stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
""").show(10)

# ---------------------------------------------------------------------------
title("Write the silver layer as Parquet")
stg_users.write.mode("overwrite").parquet(f"{BASE}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"{BASE}/silver/spark/orders.parquet")

users_silver = spark.read.parquet(f"{BASE}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"{BASE}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print(f"Parquet read back: {users_silver.count()} users, {orders_silver.count()} orders: OK")
print("users.parquet part-files:", len(users_silver.inputFiles()))
print("orders.parquet part-files:", len(orders_silver.inputFiles()))

# ---------------------------------------------------------------------------
if RUN_ICEBERG:
    title("Write the silver layer as Iceberg tables")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
    stg_users.writeTo("lab.silver.users").createOrReplace()
    stg_orders.writeTo("lab.silver.orders").createOrReplace()

    spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
    spark.sql(
        "SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files"
    ).show(truncate=False)

    users_iceberg = spark.table("lab.silver.users")
    orders_iceberg = spark.table("lab.silver.orders")
    assert users_iceberg.count() == users_bronze.count()
    assert orders_iceberg.count() == stg_orders.count()
    print("Iceberg tables read back: OK")

# ===========================================================================
title("Exercise 1 - birthdate_is_valid (DataFrame API and Spark SQL)")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))
users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        F.coalesce(F.col("birthdate").cast("timestamp") <= F.col("first_ordered_at"), F.lit(True)),
    )
    .drop("first_ordered_at")
)

stg_orders.createOrReplaceTempView("stg_orders")
users_valid_sql = spark.sql("""
    WITH first_orders AS (
        SELECT user_id, min(ordered_at) AS first_ordered_at
        FROM stg_orders
        GROUP BY user_id
    )
    SELECT u.*,
           coalesce(CAST(u.birthdate AS TIMESTAMP) <= f.first_ordered_at, true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN first_orders f ON u.user_id = f.user_id
""")

users_valid_df.groupBy("birthdate_is_valid").count().orderBy("birthdate_is_valid").show()
differences = (
    users_valid_df.select("user_id", "birthdate_is_valid")
    .exceptAll(users_valid_sql.select("user_id", "birthdate_is_valid"))
    .count()
)
print("Rows that differ between DataFrame and SQL:", differences)
assert differences == 0

# ===========================================================================
title("Exercise 2 - zip_code: regexp_extract vs Python UDF vs pandas_udf")
ZIP = re.compile(r"([A-Z]{2}) ([0-9]{5})$")


@F.udf("string")
def zip_python(address):
    if address is None:
        return None
    match = ZIP.search(address.split("\n")[-1])
    return match.group(2) if match else ""


def run(column):
    # noop sink: forces every row to be computed, without collecting them
    users_bronze.select(column.alias("zip_code")).write.format("noop").mode("overwrite").save()


versions = {
    "regexp_extract": F.regexp_extract(
        F.element_at(F.split("address", "\n"), 2), "([A-Z]{2}) ([0-9]{5})$", 2
    ),
    "python udf": zip_python("address"),
}
try:
    import pandas as pd  # noqa: F401

    @F.pandas_udf("string")
    def zip_pandas(address: "pd.Series") -> "pd.Series":
        return address.str.split("\n").str[-1].str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")

    versions["pandas_udf"] = zip_pandas("address")
except ImportError as error:
    print("pandas_udf skipped, pandas/pyarrow missing:", error)

for name, column in versions.items():
    run(column)  # warm-up
    seconds = min(timeit.repeat(lambda c=column: run(c), number=5, repeat=3)) / 5
    print(f"{name:<15} {seconds * 1000:8.1f} ms per run")

check = users_bronze.select(*[c.alias(n) for n, c in versions.items()])
assert check.filter(
    " OR ".join(f"`{n}` != `regexp_extract`" for n in versions if n != "regexp_extract") or "false"
).count() == 0
print("All versions return the same zip codes: OK")

# ===========================================================================
title("Exercise 3 - cache")
print("users_bronze cached :", users_bronze.is_cached, users_bronze.storageLevel)
print("orders_bronze cached:", orders_bronze.is_cached, orders_bronze.storageLevel)
print("Physical plan of stg_orders reads from the cache (InMemoryTableScan):")
stg_orders.explain()

# ===========================================================================
title("Exercise 4 - append + time travel, Iceberg vs Parquet")
new_order = spark.createDataFrame(
    [("11111111-1111-1111-1111-111111111111", "bdd640fb-0667-4ad1-9c80-317fa3b1799d", 3, "cookie")],
    "order_id string, user_id string, quantity int, product string",
).select("order_id", "user_id", F.current_timestamp().alias("ordered_at"), "quantity", "product")

if RUN_ICEBERG:
    new_order.writeTo("lab.silver.orders").append()
    snapshots = spark.sql(
        "SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at"
    )
    snapshots.show(truncate=False)
    first_snapshot = snapshots.first()["snapshot_id"]
    print("Current rows       :", spark.table("lab.silver.orders").count())
    print(
        f"VERSION AS OF {first_snapshot}:",
        spark.sql(f"SELECT count(*) AS n FROM lab.silver.orders VERSION AS OF {first_snapshot}").first()["n"],
    )

# Parquet: append to a copy, there is no history to go back to
parquet_copy = f"{BASE}/silver/spark/orders_ex4.parquet"
stg_orders.write.mode("overwrite").parquet(parquet_copy)
new_order.write.mode("append").parquet(parquet_copy)
print("Parquet rows after append:", spark.read.parquet(parquet_copy).count())
try:
    spark.sql(f"SELECT count(*) FROM parquet.`{parquet_copy}` VERSION AS OF 1").show()
except Exception as error:  # noqa: BLE001
    print("Parquet VERSION AS OF ->", type(error).__name__, ": a Parquet directory has no snapshots")

# ---------------------------------------------------------------------------
if sys.stdin.isatty():
    input("\nSpark UI on http://localhost:4040 (Storage, SQL, Stages tabs). Press Enter to stop Spark...")
spark.stop()
