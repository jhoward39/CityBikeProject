import logging
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='citybike_data_import.log',
                    filemode='w')
logger = logging.getLogger("IMPORT")


class CityBikeDataImport:
    def __init__(self):
        self.target_base_url = 'https://s3.amazonaws.com/tripdata/'
        self.api_base_url = "http://127.0.0.1:8000/api/"
        logger.info(
            f"Initialized CityBikeDataImport with base URL: {self.target_base_url}")

    def execute(self):
        '''
        1. Get all the file names
        2. Check if we have the file
        3. If we don't have the file, send the file to the view for download
        '''
        logger.info(f"Getting file names from {self.target_base_url}")
        files = self.getFileFromWeb()
        if len(files) == 0:
            logger.error("No files found. Exiting.")
            return

        logger.info("Checking if files are in the database")
        existing_files = self.get_existing_files_in_db()

        logger.info("Filtering out files that are already downloaded")
        files_to_download = list(filter(lambda file: file['filename'] not in existing_files, files))

        logger.info(f"Sending {len(files_to_download)} files to API for download and storage")
        
        self.send_files_to_api_for_download(files_to_download)


    def getFileFromWeb(self):
        '''
        Extract file metadata from the index page.
        Returns a list of dictionaries with file names, last modified dates, and sizes.
        '''
        response = requests.get(self.target_base_url)
        if response.status_code != 200:
            logger.debug("Failed to retrieve data")
            return []

        soup = BeautifulSoup(response.text, 'lxml-xml')

        files = []
        for content in soup.find_all("Contents"):
            file_data = {
                'filename': content.find("Key").text,
                'last_modified': content.find("LastModified").text,
                'size': content.find("Size").text
            }
            if file_data['filename'].endswith('.zip'):
                files.append(file_data)

        logger.info(f"Found {len(files)} file(s)")
        return files
    
    def get_existing_files_in_db(self):
        '''
        Fetches file listings from the Django API and checks against local records.
        '''
        response = requests.get(self.api_base_url+'get_files/')
        if response.status_code == 200:
            existing_files = {file['file_name']: file for file in response.json()}

            logger.info(f"Fetched {len(existing_files)} file record(s) from API.")
            return existing_files
        else:
            logger.error("Failed to fetch file records from API.")
            return {}

    def send_files_to_api_for_download(self, files):
        headers = {'Content-Type': 'application/json'}
        with requests.post(self.api_base_url+'upload_files/', data=json.dumps(files), headers=headers, stream=True) as response:
            if response.status_code == 200:
                logger.info(f"{response}")
            else:
                logger.error(f"{response}")
        return


if __name__ == "__main__":
    Import = CityBikeDataImport()
    Import.execute()
