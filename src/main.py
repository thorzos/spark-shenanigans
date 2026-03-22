import csv
from collections import Counter, defaultdict
from pathlib import Path

import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

from pyspark.sql import SparkSession

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

def main():
    spark = SparkSession.builder.master("local[*]").appName("chess").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    base_dir = Path.cwd()
    lichess_data = base_dir / "lichess_data" / "games.csv"
    output_path = base_dir / "output"

    df_bronze = spark.read.csv(str(lichess_data), header=True, inferSchema=True)
    df_bronze.write.mode("overwrite").parquet(str(output_path / "bronze"))

    print(f"Bronze rows: {df_bronze.count()}")
    df_bronze.printSchema()

    with open("./lichess-data/games.csv", "r") as csv_input, open("./lichess-data/games_processed.csv", "w", newline="") as csv_output:
        games_reader = csv.DictReader(csv_input)

        games = list(filter(valid_game, games_reader))

        print(win_rate_by_color(games))
        print(avg_rating_per_opening_top_25(games))
        print(top_10_openings(games))

        games_writer = csv.DictWriter(csv_output, fieldnames=["opening_name", "average_rating"])
        games_writer.writeheader()
        games_writer.writerows({})

if __name__ == "__main__":
    main()

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("chess") \
        .getOrCreate()

    print(spark.version)
    spark.stop()