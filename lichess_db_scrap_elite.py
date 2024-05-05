import requests
import os
import zipfile
from bs4 import BeautifulSoup
from tqdm import tqdm
import chess.pgn
import pandas as pd
import dask.dataframe as dd

def fetch_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    zip_links = [link['href'] for link in links if
                 link['href'].endswith('.zip')]
    return zip_links


def download_files(download_links, download_folder):
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    for link in tqdm(download_links):
        file_name = link.split('/')[-1]
        path = os.path.join(download_folder, file_name)
        response = requests.get(link)
        if response.status_code == 200:
            with open(path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_name}")
        else:
            print(f"Failed to download: {file_name}")


def unzip_files_in_folder(src_dir, dst_dir):
    # List all files in the given directory
    files = os.listdir(src_dir)
    zip_files = [file for file in files if file.endswith('.zip')]

    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    # Process each zip file
    for zip_file in tqdm(zip_files):
        # Full path to the zip file
        full_zip_path = os.path.join(src_dir, zip_file)
        # Directory to extract to (same as zip file name without extension)
        #extract_dir = os.path.join(dst_dir, os.path.splitext(zip_file)[0])

        # Open the zip file and extract all its contents
        with zipfile.ZipFile(full_zip_path, 'r') as zip_ref:
            zip_ref.extractall(dst_dir)
            print(f"Extracted {zip_file} to {dst_dir}")


def is_bullet_game(event):
    """ Determine if the game is a bullet game based on the event description. """
    # Common bullet game descriptions
    bullet_keywords = ["Bullet", "1+0", "2+1"]
    return any(keyword in event for keyword in bullet_keywords)

def extract_headers_from_pgn(file_path):
    headers_list = []

    with open(file_path, encoding='utf-8') as p:
        game_counter = tqdm(desc=f"Processing games in {os.path.basename(file_path)}", leave=False)
        while True:
            headers = chess.pgn.read_headers(p)
            if headers is None:
                break

            event = headers.get("Event", "N/A")
            white_elo = headers.get("WhiteElo", "Unknown")
            black_elo = headers.get("BlackElo", "Unknown")
            opening = headers.get("Opening", "Unknown")
            date = headers.get("Date", "N/A")

            avg_elo = 0
            try:
                if white_elo != "Unknown" and black_elo != "Unknown":
                    avg_elo = (int(white_elo) + int(black_elo)) / 2
            except ValueError:
                avg_elo = 0

            if event != "N/A" and not is_bullet_game(event) and avg_elo != 0 and opening != "N/A" and date != "N/A":
                # Append game data to list
                headers_list.append({
                    "Event": event,
                    "AverageElo": avg_elo,
                    "Opening": opening,
                    "Date": date
                })
                game_counter.update(1)
        game_counter.close()
    return headers_list


def process_pgn_file(file_path):
    headers = extract_headers_from_pgn(file_path)
    pdf = pd.DataFrame(headers)
    pdf['Date'] = pd.to_datetime(pdf['Date'])
    pdf = pdf[~pdf['Opening'].str.contains('\?')]
    pdf['Opening'] = pdf['Opening'].str.split(':').str[0]
    pdf["AverageElo"] = pdf["AverageElo"].astype(int)
    df = dd.from_pandas(pdf, npartitions=1)
    return df


def process_pgn_files(directory):
    pgn_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.pgn')]
    # Use a progress bar to show processing progress
    lazy_results = []
    for file_path in pgn_files:
        result = process_pgn_file(file_path)
        lazy_results.append(result)

    # Concatenate all results into a single Dask DataFrame
    if lazy_results:
        final_df = dd.concat(lazy_results, axis=0)
    else:
        final_df = dd.from_pandas(pd.DataFrame(), npartitions=1)

    return final_df


def main():
    download_dir = "D:\\lichess_db\\zip\\"
    pgn_dir = "D:\\lichess_db\\"
    csv_path = os.path.join(pgn_dir, 'pgn_headers.csv')
    #links = fetch_links("https://database.nikonoel.fr/")
    #download_files(links, download_dir)
    #unzip_files_in_folder(download_dir, pgn_dir)
    final_df = process_pgn_files(pgn_dir)
    result_df = final_df.compute()
    print(result_df)

    result_df.to_parquet(csv_path, index=False)
    print(f' Data saved to {csv_path}')


if __name__ == '__main__':
    main()
