import requests
from bs4 import BeautifulSoup
from lxml import html
import os


def fetch_torrent_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)
    torrent_links = [url + link['href'] for link in links if
                     link['href'].endswith('.torrent') and 'standard' in link['href']]
    return torrent_links


def download_torrent_files(torrent_links, download_folder):
    """
    Downloads all torrent files found in the torrent_links list to the specified download_folder.
    """
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    for link in torrent_links:
        file_name = link.split('/')[-1]
        path = os.path.join(download_folder, file_name)
        response = requests.get(link)
        if response.status_code == 200:
            with open(path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded: {file_name}")
        else:
            print(f"Failed to download: {file_name}")


def utorrent_connect(username, password, ip='127.0.0.1', port='8080'):
    url = f'http://{ip}:{port}/gui/'
    session = requests.Session()
    # Login to uTorrent Web UI
    try:
        # Login to uTorrent Web UI and get a token
        auth_response = session.get(url + 'token.html', auth=(username, password))
        auth_response.raise_for_status()  # Check for connection errors

        xtree = html.fromstring(auth_response.content)
        token = xtree.xpath('//*[@id="token"]/text()')[0]
        guid = auth_response.cookies['GUID']
        cookies = dict(GUID=guid)
    except requests.exceptions.RequestException as e:
        print(f"Error Utorrent connect: - {str(e)}")
        return 0, 0

    return token, cookies


def add_torrent_to_utorrent(token, cookies, torrent_file, username, password, ip='127.0.0.1', port='8080'):
    file = []
    base_url = f'http://{ip}:{port}/gui/'
    url = '%s/?%s&token=%s' % (base_url, 'action=add-file', token)

    files = {'torrent_file': open(torrent_file, 'rb')}

    try:
        if files:
            response = requests.post(url, files=files, auth=(username, password), cookies=cookies)
            if response.status_code == 200:
                file = response.json()
                print('file added')
            else:
                print(response.status_code)
        else:
            print('file not found')

        pass
    except requests.ConnectionError as error:
        print(error)
    except Exception as e:
        print(e)

    return file


def main():
    lichess_url = 'https://database.lichess.org/'
    utorrent_username = 'admin'
    utorrent_password = '123456'
    utorrent_ip = '127.0.0.1'  # Adjust as necessary
    utorrent_port = '8080'  # Adjust as necessary

    # Scrape the torrent links
    torrent_links = fetch_torrent_links(lichess_url)
    if not torrent_links:
        print("No torrents found.")
        return

    torrents_path = "D:\\lichess_db\\torrents"
    #download_torrent_files(torrent_links, torrents_path)

    token, cookies = utorrent_connect(utorrent_username, utorrent_password, utorrent_ip, utorrent_port)
    if token != 0 and cookies != 0:
        print("Connection to utorrent successful")
    # Add torrents to uTorrent
    files = os.listdir(torrents_path)
    for file in files:
        add_torrent_to_utorrent(token, cookies, os.path.join(torrents_path, file), utorrent_username, utorrent_password)


if __name__ == '__main__':
    main()
