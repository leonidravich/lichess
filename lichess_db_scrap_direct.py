import requests
import os
import zstandard as zstd
import chess.pgn
import io
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


OUTPUT_PATH = 'D:\\lichess_db'
def is_bullet_game(event):
    """ Determine if the game is a bullet game based on the event description. """
    # Common bullet game descriptions
    bullet_keywords = ["Bullet", "1+0", "2+1"]
    return any(keyword in event for keyword in bullet_keywords)

def get_list():
    list_url = "https://database.lichess.org/standard/list.txt"
    # Get the list of files from the URL
    response = requests.get(list_url)
    if response.status_code == 200:
        # Read the lines in the response
        return response.text.splitlines()
    else:
        print("Failed to retrieve file list.")


def download_and_decompress_on_the_fly(url):
    csv_file_name = url.split('/')[-1].replace('.pgn.zst', '.csv')
    csv_file_name = os.path.join(OUTPUT_PATH, csv_file_name)

    print(f"Processing {url} to {csv_file_name}")
    games_data = []  # List to store game data

    # Start the HTTP session
    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # Check that the request was successful
        # Set up the decompressor
        dctx = zstd.ZstdDecompressor()
        decompressor = dctx.stream_reader(response.raw)
        # Prepare to read streamed data as a text stream
        text_stream = io.TextIOWrapper(decompressor, encoding='utf-8')
        pgn_reader = chess.pgn.read_game(text_stream)

        # Process only the first 10 games
        game_count = 0
        game_skip = 0

        while pgn_reader and game_count < 1000000:
            event = pgn_reader.headers.get("Event", "N/A")
            white_elo = pgn_reader.headers.get("WhiteElo", "Unknown")
            black_elo = pgn_reader.headers.get("BlackElo", "Unknown")
            opening = pgn_reader.headers.get("Opening", "Unknown")
            date = pgn_reader.headers.get("Date", "N/A")

            # Calculate average Elo and convert to label if available
            avg_elo = 0
            try:
                if white_elo != "Unknown" and black_elo != "Unknown":
                    avg_elo = (int(white_elo) + int(black_elo)) / 2
            except ValueError:
                avg_elo = 0

            if event != "N/A" and not is_bullet_game(event) and avg_elo != 0 and opening != "N/A" and date != "N/A":
                # Append game data to list
                games_data.append({
                    "Event": event,
                    "AverageElo": avg_elo,
                    "Opening": opening,
                    "Date": date
                })
                game_skip += 1
            game_count += 1
            pgn_reader = chess.pgn.read_game(text_stream)

    df = pd.DataFrame(games_data)
    df.to_csv(csv_file_name, index=False)
    print(f'{csv_file_name} done total {game_count} skipped {game_skip}')

    del df
    del games_data

    return csv_file_name


src_list = get_list()
src_list.reverse()

if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(download_and_decompress_on_the_fly, url) for url in src_list]
    with tqdm(total=len(futures), desc="Total Progress", unit="file") as pbar:
        for future in as_completed(futures):
            print(f"Completed: {future.result()}")
            pbar.update(1)  # Update progress upon each task's completion



