import logging
import shutil
import os
import tempfile
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
from dateutil import parser
import zipfile
import django
import os
import csv
from datetime import datetime
import django
from django.conf import settings
from django.db.models import Max

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'CityBikesProject.settings')
django.setup()

from CityBikeApp.models import ProcessedFile, ProcessingFile, Station, Bike, Ride


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
#      For every file Processing Dir:                                    #
#      4.1 Pull out the rows                                             #              
#      4.2 Normalize the data in the rows                                #    
#      4.3 Bulk Insert that data into the DB                             #
#      4.4 Move File from Processing to Processed                        #
#      4.5 Create db Record of ProcessedFile                             #
#      4.6 Delete db record of ProcessingFile                            #
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
                if counter > 14:  # MOD Counter to limit the number of files processed
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
        ) 

        for file_path in files:
            file_name = os.path.basename(file_path)
            if file_name.endswith('.csv') and not file_name.startswith('._'):
                self.process_file(file_name)
            

    def process_file(self, file_name): 
        # 4.1 Pull out the rows
        logger.debug(f"Pulling out data from {file_name}")

        file_path = os.path.join(PROCESSING_DIR, file_name)
        logger.debug(f"Opening {file_name} to parse and upload")
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            header = reader.fieldnames
            rows = list(reader)[:2]  ## MOD Read only the first 2 records for processing
            self.normalize_rows(header, rows, file_path)

    def convert_date(self, date_str):
        try:
            # Parse the date string to datetime
            dt = parser.parse(date_str)
            # Return the datetime object
            return dt
        except ValueError:
            print(f"Error parsing date: {date_str}")
            return None

    def parse_row(self, row, is_old_format):
        if is_old_format:
            if row.get("birth year") == "\\N":
                row["birth year"] = 0
            row_data = {
                'ride_id': row.get("ride_id"),
                'start_station_id': row.get("start station sd"),
                'start_station_name': row.get("start station same", "unknown"),
                'start_station_lat': row.get("start station latitude"),
                'start_station_lon': row.get("start station longitude"),
                'end_station_id': row.get("end station id"),
                'end_station_name': row.get("end station name", "unknown"),
                'end_station_lat': row.get("end station latitude"),
                'end_station_lon': row.get("end station longitude"),
                'bike_id': row.get("bikeid"),
                'rider_birth_year': int(float(str(row.get("birth year")))),
                'rider_gender': row.get("gender"),
                'started_at': self.convert_date(row["starttime"]),
                'ended_at': self.convert_date(row["stoptime"]),
                'bike_type': row.get("bike_type", 'unknown'),  # bike_type is not available in old format
                'rider_member_or_casual': row.get("usertype", 'unknown')  # member_type is not available in old format
            }
            
        else:
            row_data = {
                'ride_id': row.get("ride_id"),
                'bike_type': row.get("rideable_type"),
                'started_at': self.convert_date(row["started_at"]),
                'ended_at': self.convert_date(row["ended_at"]),
                'start_station_id': row.get("start_station_id"),
                'start_station_name': row.get("start_station_name"),
                'end_station_id': row.get("end_station_id"),
                'end_station_name': row.get("end_station_name"),
                'start_station_lat': row.get("start_lat"),
                'start_station_lon': row.get("start_lng"),
                'end_station_lat': row.get("end_lat"),
                'end_station_lon': row.get("end_lng"),
                'rider_member_or_casual': row.get("member_casual"),
                'bike_id': row.get("bike_id"),
                'rider_birth_year': 0,
                'rider_gender': 0,
                
            }
        
        return row_data
    

    def normalize_rows(self, header, rows, file_path):
        #4.2 Normalize the data in the rows
        logger.debug(f"Normalizing {len(rows)} records from {file_path}")

        processing_file = ProcessingFile.objects.get(file_name=os.path.basename(file_path))
        
        # Create a processed file record to be a foreign key for the rides
        processed_file, created_processed_file = ProcessedFile.objects.update_or_create(
            file_name=processing_file.file_name,
            defaults={
                'file_path': processing_file.file_path.replace("Processing", "Processed"),
                'parent_zip_last_modified': processing_file.parent_zip_last_modified,
                'size': processing_file.size,
                'number_of_rows': len(rows)
            }
        )
        
        logger.info(f"Created or Updated ProcessedFile record {processed_file}")

        ride_objects = []
        is_old_format = "tripduration" in header
        for row in rows:
            print("raw",row)
            # Dyanmically map the fields based on the header
            parsed_row =  self.parse_row(row, is_old_format)
            print("parsed", parsed_row)

            # Create or get Station and Bike instances
            if parsed_row.get("start_station_id"):
                station_id = int(float(parsed_row.get("start_station_id")))
            else:
                station_id = None
            start_station, _ = Station.objects.get_or_create(
                
                station_id=station_id,
                defaults={'station_name': parsed_row.get('start_station_name'), 'lat': float(row.get('start_lat', 0)), 'lon': float(row.get('start_lng', 0))}
            )

            if parsed_row.get("end_station_id"):
                station_id = int(float(parsed_row.get("end_station_id")))
            else:
                station_id = None

            end_station, _ = Station.objects.get_or_create(
                station_id=station_id,
                defaults={'station_name': parsed_row.get('end_station_name'), 'lat': float(row.get('end_lat', 0)), 'lon': float(row.get('end_lng', 0))}
            )

            bike, _ = Bike.objects.get_or_create(
                bike_id=parsed_row.get('bike_id'),
                bike_type=parsed_row.get('bike_type')
            )

            ride = Ride(
                ride_id=row.get('ride_id'),
                started_at=parsed_row.get('started_at'),
                ended_at=parsed_row.get('ended_at'),
                start_station=start_station,
                end_station=end_station,
                bike=bike,
                rider_birth_year=int(parsed_row.get('rider_birth_year', 0)),
                rider_gender=int(parsed_row.get('rider_gender', 0)),
                rider_member_or_casual=parsed_row.get('user_type', 'unknown'),
                source_file=processed_file
            )
            ride_objects.append(ride)
            

        logger.debug(f"Adding {len(ride_objects)} records to the Ride model")
        Ride.objects.bulk_create(ride_objects, batch_size=998)
        logger.debug(f"Successfully added {len(ride_objects)} records to the Ride model")
        self.move_file_from_processing_to_processed(os.path.basename(file_path))
        logger.debug(f"Deleting {processing_file.file_name} from the ProcessingFile model")
        processing_file.delete()
        return
    
    def move_file_from_processing_to_processed(self,file_name):
        processing_file = os.path.join(PROCESSING_DIR, file_name)
        processed_file = os.path.join(PROCESSED_DIR, file_name)
        try:
            shutil.move(processing_file, processed_file)
            logger.info(f"Moved {file_name} from processing to processed directory")
        except Exception as e:
            logger.error(f"Failed to move {file_name} from processing to processed directory")
            logger.error(e)
            return
    
    
    

if __name__ == "__main__":
    Import = CityBikeDataImport()
    Import.execute()
