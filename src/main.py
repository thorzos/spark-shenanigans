import csv

def valid_game(game):
    return int(game['turns']) > 5 and game['rated'] == 'True'

def main():
    with open("./lichess-data/games.csv", "r") as csv_input, open("./lichess-data/games_processed.csv", "w", newline="") as csv_output:
        games_reader = csv.DictReader(csv_input)

        fieldnames = games_reader.fieldnames

        games = list(filter(valid_game, games_reader))
        unique_openings = {game['opening_name'] for game in games}

        games_writer = csv.DictWriter(csv_output, fieldnames=["opening_name"])
        games_writer.writeheader()
        games_writer.writerows({"opening_name": opening} for opening in sorted(unique_openings))

if __name__ == '__main__':
    main()