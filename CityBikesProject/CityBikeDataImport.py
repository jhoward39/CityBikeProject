import logging
import shutil
import os
import tempfile
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import zipfile
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'CityBikesProject.settings')
django.setup()

from CityBikeApp.models import ProcessedFile, ProcessingFile


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
            logger.info(
                f"Starting 1.0 Getting file names from {self.target_base_url}")
            files = self.get_files_from_web()
            if len(files) == 0:
                logger.error("No files found. Exiting.")
                return
            logger.info(f"Ending 1.0 Found {len(files)} file(s)")
        except Exception as e:
            logger.error(
                f"Failed during 1.0, did not get files from {self.target_base_url}")
            logger.error(e)
            return

        # 2.0 Extract and organize all files in zip files
        try:
            logger.info(
                "Starting 2.0 Putting files in the processing directory")
            counter = 0  # MOD Counter to limit the number of files processed
            files_to_process = []
            for zip_file in files:
                counter += 1  # MOD Counter to limit the number of files processed
                if counter % 3 == 0:  # MOD Limit the number of files processed
                    continue  # MOD Counter to limit the number of files processed
                if counter > 16:  # MOD Counter to limit the number of files processed
                    break  # MOD Counter to limit the number of files processed

                logger.info(f"Processing {zip_file['filename']}")
                extracted_files = self.extract_and_organize_files(zip_file)
                files_to_process.extend(extracted_files)
            self.add_files_to_ProcessingFile(files_to_process)
            logger.info("Ending 2.0 All files in the processing directory")
        except Exception as e:
            self.add_files_to_ProcessingFile(files_to_process)
            logger.error("Failed to process files")
            logger.error(e)
            return

       # 3.0 Filter out files that are already downloaded
        try:
            logger.info(
                "Starting 3.0 Filtering out files that are already downloaded")
            files_to_delete = self.get_processed_files()
            if len(files_to_delete) > 0:
                self.delete_files_and_records(files_to_delete)
            logger.info(
                "Ending 3.0 Filtered out files that are already downloaded")
        except Exception as e:
            logger.error(
                "Failed to filter out files that are already downloaded")
            logger.error(e)
            return

       # 4.0 Extract and Load data into the database

        try:
            logger.info("Starting 4.0 Extracting and loading data into the database")
            self.process_files()
            logger.info("Ending 4.0 Extracting and loading data into the database")
        except Exception as e:
            logger.error("Failed to extract and load data into the database")
            logger.error(e)
            return
        
        logger.info("Ending 4.0 Extracting and loading data into the database")
        logger.info("CityBikeDataImport completed successfully!!!!")




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

    def extract_and_organize_files(self, zip_file_attributes):
        """
        Extracts a zip file to a temporary directory, moves all extracted files to the specified processing directory,
        and cleans up the temporary directory.

        Parameters:
        - zip_file_path (str): The file path for the zip file to extract.
        - processing_dir (str): The directory to move extracted files to for further processing.

        Returns:
        - list of str: A list of file paths for the files moved to the processing directory.
        """

        zip_file_url = f"{self.target_base_url}{zip_file_attributes['filename']}"
        local_zip_path = os.path.join(tempfile.gettempdir(), zip_file_attributes['filename'])

        # Download the file
        try:
            with requests.get(zip_file_url, stream=True) as r:
                r.raise_for_status()
                with open(local_zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logging.info(f"Downloaded {zip_file_attributes['filename']} successfully.")
        except requests.RequestException as e:
            logging.error(
                f"Failed to download the file {zip_file_attributes['filename']}. Error: {str(e)}")
            return []

        extracted_files = []
        first_file_found = False  # MOD Flag to indicate if at least one file has been processed
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                logger.debug(f"Extracting {zip_file_attributes['filename']}")
                zip_ref.extractall(temp_dir)
                for root, _, files in os.walk(temp_dir):
                    files = [f for f in files if not f.startswith(
                        '.') and f.endswith('.csv')]
                    if files:
                        logger.debug(
                            f"Found {len(files)} files in {root}")
                        for file in files:
                            if not first_file_found:
                                temp_file_path = os.path.join(root, file)
                                processing_file_path = os.path.join(
                                    PROCESSING_DIR, file)
                                shutil.move(temp_file_path,
                                            processing_file_path)
                                extracted_files.append({'filename':file, 'size':zip_file_attributes['size'], 'last_modified':zip_file_attributes['last_modified']})
                                first_file_found = True  # MOD Set the flag after moving the first file
                                break  # MOD Only process the first file in the directory
                    if first_file_found:
                        logging.debug(
                            "Stopping after the first CSV file has been moved...")
                        break  # MODStop after processing the first directory with a CSV file

        logger.debug(f"Moved {len(extracted_files)} files to {PROCESSING_DIR}")
        return extracted_files

    def add_files_to_ProcessingFile(self, file_details):
        processing_files = []
        for detail in file_details:
            try:
                last_modified = datetime.fromisoformat(
                    detail['last_modified'].replace('Z', '+00:00'))
                processing_file = ProcessingFile(
                    file_name=detail['filename'],
                    file_path=PROCESSING_DIR + detail['filename'],
                    parent_zip_last_modified=last_modified,
                    size=detail['size'],
                    number_of_rows=0
                )
                processing_files.append(processing_file)
            except Exception as e:
                logger.error(
                    f"Error processing file details {detail}: {str(e)}")

        try:
            ProcessingFile.objects.bulk_create(
                processing_files, ignore_conflicts=True)
            logger.info(
                f"Successfully added {len(processing_files)} files to the ProcessingFile model.")
        except Exception as e:
            logger.error(
                "Failed to add files to the database. DELETE FILES IN PROCESSED AND RECORDS IN ProcessingFiles then RERUN.")
            logger.error(e)

    def get_processed_files(self):
        """
        Retrieves a list of filenames from the ProcessingFile table that exist in the ProcessedFile table.
        """
        matched_files = ProcessingFile.objects.filter(
            file_name__in=ProcessedFile.objects.values_list('file_name', flat=True),
            size__in=ProcessedFile.objects.values_list('size', flat=True),
            parent_zip_last_modified__in=ProcessedFile.objects.values_list(
                'parent_zip_last_modified', flat=True)
        ).values_list('file_name', flat=True)

        return list(matched_files)

    def delete_files_and_records(self, files_to_delete):
        """
        Deletes files in the processing directory that are already in the database and their corresponding records.
        """
        for file in files_to_delete:
            file_path = os.path.join(PROCESSING_DIR, file)
            try:
                os.remove(file_path)
                logger.info(f"Deleted {file} from the processing directory.")
            except Exception as e:
                logger.error(
                    f"Failed to delete {file} from the processing directory. DELETE FILES IN PROCESSED AND RECORDS IN ProcessingFiles then RERUN")
                logger.error(e)

        try:
            ProcessingFile.objects.filter(file_name__in=files_to_delete).delete()
            logger.info(
                f"Deleted {len(files_to_delete)} records from the ProcessingFile model.")
        except Exception as e:
            logger.error(
                "Failed to delete records from the ProcessingFile model. DELETE FILES IN PROCESSED AND RECORDS IN ProcessingFiles then RERUN.")
            logger.error(e)

    def process_files(self):
        files = sorted(
            [os.path.join(PROCESSING_DIR, f) for f in os.listdir(PROCESSING_DIR)],
            key=lambda x: os.path.getmtime(x),
            reverse=True  # Youngest files first
        )[:10]  # Only process the first 10 files

        for file_path in files:
            self.process_file(file_path)

    def process_file(self,file_path):
        print(f"Processing {file_path}")
        pass
if __name__ == "__main__":
    Import = CityBikeDataImport()
    Import.execute()
