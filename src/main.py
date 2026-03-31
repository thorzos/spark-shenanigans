from collections import Counter, defaultdict
from pathlib import Path

import time
import os

from pyspark.sql.functions import col, count, avg, round as spark_round

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession, DataFrame

def win_rate_by_color(games):
    wins = Counter(game["winner"] for game in games)
    total = len(games)
    return {color: round(count / total * 100, 2) for color, count in wins.items()}

def avg_rating_per_opening_top_25(games):
    all_games = defaultdict(lambda: [0, 0])
    for game in games:
        all_games[game["opening_name"]][0] += int(game["white_rating"]) + int(game["black_rating"])
        all_games[game["opening_name"]][1] += 2

    averages = {k: round(v[0] / v[1]) for k, v in all_games.items()}
    return sorted(averages.items(), key=lambda x: x[1], reverse=True)[:25]

def top_10_openings(games):
    return Counter(game["opening_name"] for game in games).most_common(10)

def bronze(spark: SparkSession, base_dir: Path) -> DataFrame:
    df_bronze = spark.read.csv(str(base_dir / "lichess-data" / "games.csv"), header=True, inferSchema=True)
    return df_bronze

def silver(spark: SparkSession, base_dir: Path) -> DataFrame:
    df_silver = spark.read.parquet(str(base_dir / "output" / "bronze"))

    return (
        df_silver
        .filter(col("turns") > 5)
        .filter(col("rated") == "True")
        .filter(col("white_rating") > 0)
        .filter(col("black_rating") > 0)
        .filter(col("winner").isin("white", "black", "draw"))
        .dropDuplicates(["id"])
        .withColumn("white_rating", col("white_rating").cast("integer"))
        .withColumn("black_rating", col("black_rating").cast("integer"))
    )

def gold(spark: SparkSession, base_dir: Path) -> dict[str, DataFrame]:
    df_gold = spark.read.parquet(str(base_dir / "output" / "silver"))

    winrate_by_color = (
        df_gold
            .groupBy("winner")
            .agg(count("*").alias("wins"))
            .withColumn("winrate", spark_round(col("wins") / df_gold.count() * 100, 2))
    )

    avg_rating_per_opening = (
        df_gold
            .withColumn("avg_game_rating", (col("white_rating") + col("black_rating")) / 2)
            .groupBy("opening_name")
            .agg(spark_round(avg("avg_game_rating"), 0).alias("avg_rating"))
            .orderBy(col("avg_rating").desc())
            .limit(100)
    )

    top_openings = (
        df_gold
            .groupBy("opening_name")
            .agg(count("*").alias("total_games"))
            .orderBy(col("total_games").desc())
            .limit(100)
    )

    return {
        "winrate" : winrate_by_color,
        "avg_rating_by_opening" : avg_rating_per_opening,
        "top_openings" : top_openings
    }


def main():
    spark = SparkSession.builder.master("local[*]").appName("chess").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    application_time = time.time()

    base_dir = Path.cwd()
    output_path = base_dir / "output"

    df_bronze = bronze(spark, base_dir)
    df_bronze.write.mode("overwrite").parquet(str(output_path / "bronze"))
    print(f"bronze time: {time.time() - application_time:.4f}s")

    df_silver = silver(spark, base_dir)
    df_silver.write.mode("overwrite").parquet(str(output_path / "silver"))
    print(f"silver time: {time.time() - application_time:.4f}s")

    df_gold = gold(spark, base_dir)
    for name, df in df_gold.items():
        df.write.mode("overwrite").parquet(str(output_path / "gold" / name))
    print(f"gold time: {time.time() - application_time:.4f}s")

#    with open("explain_silver.txt", "w") as f:
#       f.write(df_silver._jdf.queryExecution().toString())

if __name__ == "__main__":
    main()