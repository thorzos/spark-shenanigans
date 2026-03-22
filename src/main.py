from collections import Counter, defaultdict
from pathlib import Path

import os

from pyspark.sql.functions import col

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession, DataFrame


def valid_game(game):
    return int(game["turns"]) > 5 and game["rated"] == "True"

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

def main():
    spark = SparkSession.builder.master("local[*]").appName("chess").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = Path.cwd()
    output_path = base_dir / "output"

    df_bronze = bronze(spark, base_dir)
    df_bronze.write.mode("overwrite").parquet(str(output_path / "bronze"))
    print("bronze count: %d" % df_bronze.count())

    df_silver = silver(spark, base_dir)
    df_silver.write.mode("overwrite").parquet(str(output_path / "silver"))
    print("silver count: %d" % df_silver.count())

    with open("explain_silver.txt", "w") as f:
        f.write(df_silver._jdf.queryExecution().toString())

    # with open("./lichess-data/games.csv", "r") as csv_input, open("./lichess-data/games_processed.csv", "w", newline="") as csv_output:
    #     games_reader = csv.DictReader(csv_input)
    #
    #     games = list(filter(valid_game, games_reader))
    #
    #     print(win_rate_by_color(games))
    #     print(avg_rating_per_opening_top_25(games))
    #     print(top_10_openings(games))
    #
    #     games_writer = csv.DictWriter(csv_output, fieldnames=["opening_name", "average_rating"])
    #     games_writer.writeheader()
    #     games_writer.writerows({})

if __name__ == "__main__":
    main()