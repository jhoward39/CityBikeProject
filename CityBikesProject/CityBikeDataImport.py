import logging
import shutil
import os
import tempfile
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import zipfile

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='citybike_data_import.log',
                    filemode='w')
logger = logging.getLogger("IMPORT")

PROCESSING_DIR = "/Users/joeyhoward/Desktop/CityBikeData/Processing"
PROCESSED_DIR = "/Users/joeyhoward/Desktop/CityBikeData/Processed"

##########################################################################
#                                                                        #
#                           CityBikeDataImport                           #
#                                                                        #
#  Description: This script automates the processing of CityBike data    #
#  files. It downloads, maps, and imports data into a sqllite database   #
#                                                                        #
#  Routines:                                                             #          
#  (safe to rerun a routine) {1}, {1,2}, {3,4}                           # 
#                                                                        #
#  Steps:                                                                #
#  1.0 Collect list of files from the target URL                         #
#      1.1 Get all the file names from the target URL ending in .zip     #
#                                                                        #
#  2.0 Extract and organize all files in zip files                       #
#      For each file in the list:                                        #
#      2.1 Unzip the file in a temporary directory                       #
#      2.2 Move all the files to the processing directory                #
#      2.3 Record files in processing dir to be added to db later        #
#      2.4 Clean up the temporary directory                              #
#      2.5 Add Record of processing files to db                          #
#                                                                        #
#  3.0 Filter out files that are already downloaded                      #
#      3.1 Check if files in the processing directory are already in the #
#          database                                                      #
#      3.2 If we have the file (latest version), delete it               #
#                                                                        #
#  4.0 Extract and Load data into the database                           #
#      4.1 Import the data into the database                             #
#      4.2 Move file to the processed directory                          #
#                                                                        #
#  Author: Joseph Howard                                                 #
#  Date: April 12, 2024                                                  #
#                                                                        #
##########################################################################


class CityBikeDataImport:
    def __init__(self):
        self.target_base_url = 'https://s3.amazonaws.com/tripdata/'
        self.api_base_url = "http://127.0.0.1:8000/api/"
        logger.info(
            f"Initialized CityBikeDataImport with base URL: {self.target_base_url}")

    def execute(self):
        # 1.0 "Collect list of files from the target URL"
        try:
            logger.info(f"Starting 1.0 Getting file names from {self.target_base_url}")
            files = self.get_files_from_web()
            if len(files) == 0:
                logger.error("No files found. Exiting.")
                return
            logger.info(f"Ending 1.0 Found {len(files)} file(s)")
        except Exception as e:
            logger.error(f"Failed during 1.0, did not get files from {self.target_base_url}")
            logger.error(e)
            return
        

        # 2.0 Extract and organize all files in zip files
        try:
            logger.info("Starting 2.0 Putting files in the processing directory")
            counter = 0 ## MOD Counter to limit the number of files processed
            for zip_file in files:
                counter+=1  
                if counter % 2 == 0: ## MOD Limit the number of files processed
                    continue
                if counter > 20:
                    break

                logger.info(f"Processing {zip_file['filename']}")
                self.extract_and_organize_files(zip_file['filename'])
            logger.info("Ending 2.0 All files in the processing directory")
        except Exception as e:
            logger.error("Failed to process files")
            logger.error(e)
            return
        
       # 3.0 Filter out files that are already downloaded

       # 4.0 Extract and Load data into the database

    def get_files_from_web(self):
        '''
        Extract file metadata from the index page.

        Returns:
        A list of dictionaries with file names, last modified dates, and sizes.
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
        return files

    def get_existing_files_in_db(self):
        '''
        Fetches file listings from the Django API and checks against local records.
        '''
        response = requests.get(self.api_base_url+'get_files/')
        if response.status_code == 200:
            existing_files = {file['file_name']: file for file in response.json()}

            logger.info(
                f"Fetched {len(existing_files)} file record(s) from API.")
            return existing_files
        else:
            logger.error("Failed to fetch file records from API.")
            return {}

    def extract_and_organize_files(self, zip_file_name):
        """
        Extracts a zip file to a temporary directory, moves all extracted files to the specified processing directory,
        and cleans up the temporary directory.

        Parameters:
        - zip_file_path (str): The file path for the zip file to extract.
        - processing_dir (str): The directory to move extracted files to for further processing.

        Returns:
        - list of str: A list of file paths for the files moved to the processing directory.
        """

        zip_file_url = f"{self.target_base_url}{zip_file_name}"
        local_zip_path = os.path.join(tempfile.gettempdir(), zip_file_name)

        # Download the file
        try:
            with requests.get(zip_file_url, stream=True) as r:
                r.raise_for_status()
                with open(local_zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logging.info(f"Downloaded {zip_file_name} successfully.")
        except requests.RequestException as e:
            logging.error(f"Failed to download the file {zip_file_name}. Error: {str(e)}")
            return []
    
        extracted_files = []
        first_file_found = False  ## MOD Flag to indicate if at least one file has been processed
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                logger.debug(f"Extracting {zip_file_name}")
                zip_ref.extractall(temp_dir)
                for root, _, files in os.walk(temp_dir):
                    files = [f for f in files if not f.startswith('.') and f.endswith('.csv')]
                    if files:
                        logger.debug(
                            f"Found {len(files)} files in {root}")
                        for file in files:
                            if not first_file_found:
                                temp_file_path = os.path.join(root, file)
                                processing_file_path = os.path.join(
                                    PROCESSING_DIR, file)
                                shutil.move(temp_file_path, processing_file_path)
                                extracted_files.append(processing_file_path)
                                first_file_found = True  ## MOD Set the flag after moving the first file
                                break ## MOD Only process the first file in the directory
                    if first_file_found:
                        logging.debug("Stopping after the first CSV file has been moved...")
                        break  ## MODStop after processing the first directory with a CSV file
        
        logger.debug(f"Moved {len(extracted_files)} files to {PROCESSING_DIR}")
        return extracted_files


if __name__ == "__main__":
    Import = CityBikeDataImport()
    Import.execute()
