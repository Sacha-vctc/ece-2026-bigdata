"""Lab 06 - Introduction to Spark's RDD API.

Run from the project root, after downloading the bronze CSVs:
    spark-submit spark_labs/lab06_rdd.py
"""

import re
import time
from operator import add

from pyspark.sql import SparkSession


def title(text):
    print(f"\n{'=' * 70}\n{text}\n{'=' * 70}")


spark = SparkSession.builder.appName("lab-rdd").master("local[2]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# ---------------------------------------------------------------------------
title("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users_lines partitions :", users_lines.getNumPartitions())
print("orders_lines partitions:", orders_lines.getNumPartitions())
print("defaultMinPartitions   :", sc.defaultMinPartitions)

orders_repartitioned = orders_lines.repartition(8)
print("after repartition(8)   :", orders_repartitioned.getNumPartitions())
print("after coalesce(1)      :", orders_repartitioned.coalesce(1).getNumPartitions())

# ---------------------------------------------------------------------------
title("Lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
print("orders_data.count() #1:", orders_data.count())
print("orders_data.count() #2:", orders_data.count())

# ---------------------------------------------------------------------------
title("The cost of no schema: the multiline address")
for line in users_lines.take(6):
    print(repr(line))
print("users_lines.count():", users_lines.count())

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users_records.count():", users_records.count())

# ---------------------------------------------------------------------------
title("Manual parsing of orders")


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
for order in orders_rdd.take(3):
    print(order)

# A line with an extra comma inside the product makes the unpacking fail
sc.setLogLevel("OFF")  # hide the expected stack trace
malformed = sc.parallelize(["a,b,2020-01-01,1,choco,late"]).map(parse_order)
print("Transformation defined: no error yet (lazy)")
try:
    malformed.collect()
except Exception as error:  # noqa: BLE001
    print("collect() -> ValueError 'too many values to unpack': raised at the action, not at read time")
sc.setLogLevel("ERROR")

# ---------------------------------------------------------------------------
title("Key-value RDDs: total quantity per user")
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print("reduceByKey:", user_quantity.take(5))

user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
print("groupByKey :", user_quantity_grouped.take(5))
print("Same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

# ---------------------------------------------------------------------------
title("Manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)
print("before / after dedup:", orders_rdd.count(), orders_dedup.count())

# ---------------------------------------------------------------------------
title("Joining orders with users")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
for row in enriched.take(3):
    print(row)
print("join          :", enriched.count())
print("leftOuterJoin :", orders_kv.leftOuterJoin(users_kv).count())

# ===========================================================================
title("Exercise 1 - aggregateByKey: (order_count, total_quantity) per user")
user_stats = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, quantity: (acc[0] + 1, acc[1] + quantity),  # within a partition
    lambda a, b: (a[0] + b[0], a[1] + b[1]),  # across partitions
)
user_avg = user_stats.mapValues(lambda s: (s[0], s[1], round(s[1] / s[0], 2)))
for user_id, (count, total, avg) in user_avg.take(5):
    print(f"{user_id}  orders={count:3d}  quantity={total:4d}  avg={avg}")

# Consistency check with reduceByKey
totals = dict(user_quantity.collect())
assert all(totals[user_id] == total for user_id, (_, total) in user_stats.collect())
print("Totals match reduceByKey: OK")

# ===========================================================================
title("Exercise 2 - fake user_id and leftOuterJoin")
fake_order = {
    "order_id": "00000000-0000-0000-0000-000000000000",
    "user_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
    "date": "2020-01-01 00:00:00+00:00",
    "quantity": 1,
    "product": "bread",
}
orders_with_fake = orders_rdd.union(sc.parallelize([fake_order]))
orphans = (
    orders_with_fake
    .map(lambda o: (o["user_id"], o))
    .leftOuterJoin(users_kv)
    .filter(lambda kv: kv[1][1] is None)  # no matching username
)
print("Orders without a matching user:", orphans.count())
print(orphans.take(1))

# ===========================================================================
title("Exercise 3 - same aggregation with the DataFrame API")
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
user_quantity_df = orders_df.groupBy("user_uuid").sum("quantity")
user_quantity_df.show(5, truncate=False)
print("--- DataFrame .explain() ---")
user_quantity_df.explain()
print("--- RDD .toDebugString() ---")
print(user_quantity.toDebugString().decode())

# ===========================================================================
title("Exercise 4 - effect of .cache()")


def timed(label, fn):
    start = time.perf_counter()
    result = fn()
    print(f"{label:<42} {time.perf_counter() - start:7.3f}s  -> {result}")


def build(cached):
    rdd = orders_data.map(parse_order)
    if cached:
        rdd = rdd.cache()
    quantity = rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
    return rdd, quantity


for cached in (True, False):
    rdd, quantity = build(cached)
    label = "with cache" if cached else "without cache"
    timed(f"orders_rdd.count()    1st run ({label})", rdd.count)
    timed(f"orders_rdd.count()    2nd run ({label})", rdd.count)
    timed(f"user_quantity.count() ({label})", quantity.count)
    print("is_cached:", rdd.is_cached, "| storage level:", rdd.getStorageLevel())
    rdd.unpersist()

spark.stop()
