from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import (
    col, to_timestamp, window, avg, when,
    min as spark_min, max as spark_max,
    date_format, round as spark_round
)

spark = SparkSession.builder.appName("CPU_MEM_Monitoring_v3").getOrCreate()

CPU_THRESHOLD = 85.93
MEM_THRESHOLD = 76.84
EPS = 0.0001

schema = StructType([
    StructField("ts", StringType(), True),
    StructField("server_id", StringType(), True),
    StructField("value", StringType(), True)   # temporarily StringType to handle any bad data
])

cpu_df = (
    spark.read.csv("cpu_data1.csv", header=True, schema=schema)
    .withColumn("cpu_pct", col("value").cast(FloatType()))      # ensure float
    .drop("value")
    .withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))
)

mem_df = (
    spark.read.csv("mem_data1.csv", header=True, schema=schema)
    .withColumn("mem_pct", col("value").cast(FloatType()))      # ensure float
    .drop("value")
    .withColumn("timestamp", to_timestamp(col("ts"), "HH:mm:ss"))
)


time_bounds = cpu_df.select(
    spark_min("timestamp").alias("min_ts"),
    spark_max("timestamp").alias("max_ts")
).first()

min_ts = time_bounds["min_ts"]
max_ts = time_bounds["max_ts"]


merged_df = cpu_df.join(mem_df, ["server_id", "timestamp"])


windowed = (
    merged_df.groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("server_id")
    )
    .agg(
        avg("cpu_pct").alias("avg_cpu"),
        avg("mem_pct").alias("avg_mem")
    )
)


filtered_windows = windowed.where(
    (col("window.start") >= min_ts) & (col("window.start") <= max_ts)
)


alerts_df = (
    filtered_windows.withColumn(
        "alert",
        when(
            (col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") > MEM_THRESHOLD),
            "High CPU + Memory stress"
        )
        .when(
            (col("avg_cpu") > CPU_THRESHOLD) & (col("avg_mem") <= MEM_THRESHOLD),
            "CPU spike suspected"
        )
        .when(
            (col("avg_mem") > MEM_THRESHOLD + EPS) & (col("avg_cpu") <= CPU_THRESHOLD),
            "Memory saturation suspected"
        )
        .otherwise("")
    )
)

final_df = (
    alerts_df.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        spark_round(col("avg_cpu"), 2).alias("avg_cpu"),
        spark_round(col("avg_mem"), 2).alias("avg_mem"),
        col("alert")
    )
    .orderBy("server_id", "window_start")
)

final_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv("team_77_CPU_MEM_v6")

final_df.show(truncate=False)