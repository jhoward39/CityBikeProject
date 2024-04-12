from django.db import models

class ProcessedFile(models.Model):
    """
    The ProcessedFile model represents a file in the system.
    Each file has a unique name, a path, a last modified timestamp, a size in byes, and number of rows in the db.
    """
    file_id = models.AutoField(primary_key=True)
    file_name = models.CharField(max_length=255, unique=True)
    file_path = models.CharField(max_length=255)
    parent_zip_last_modified = models.DateTimeField()
    size = models.BigIntegerField()
    number_of_rows = models.IntegerField(default=0)

    def __str__(self):
        return f"{self.file_name} ({self.size} MB) Last Modified: {self.parent_zip_last_modified}"
    
class ProcessingFile(models.Model):
    """
    The ProcessingFile model represents a file that is currently being processed.
    Each processing file has a unique name, a path, a last modified timestamp, a size in bytes, and number of rows in the db.
    """
    file_id = models.AutoField(primary_key=True)
    file_name = models.CharField(max_length=255, unique=True)
    file_path = models.CharField(max_length=255)
    parent_zip_last_modified = models.DateTimeField()
    size = models.BigIntegerField()
    number_of_rows = models.IntegerField(default=0)

    def __str__(self):
        return f"{self.file_name} ({self.size} MB) Last Modified: {self.parent_zip_last_modified}"

class Station(models.Model):
    """
    The Station model represents a physical station where bikes are docked.
    Each station has a unique identifier, a name, and a geographical location represented by latitude and longitude.
    """
    station_id = models.AutoField(primary_key=True)
    station_name = models.CharField(max_length=255)
    lat = models.FloatField()
    lon = models.FloatField()

    def __str__(self):
        return f"{self.station_name} (ID: {self.station_id})"

class Bike(models.Model):
    """
    The Bike model represents a bike that can be rented.
    Each bike has a unique identifier and a type (electric, classic, or unknown).
    """
    BIKE_TYPES = (
        ('electric', 'Electric'),
        ('classic', 'Classic'),
        ('unknown', 'Unknown'),
    )
    bike_id = models.AutoField(primary_key=True, editable=True)
    bike_type = models.CharField(max_length=255, choices=BIKE_TYPES, default='unknown')

    def __str__(self):
        return f"{self.bike_type.title()} Bike (ID: {self.bike_id})"

class Ride(models.Model):
    """
    The Ride model represents a single ride taken by a user.
    Each ride has a unique identifier, start and end times, start and end stations, a bike, the birth year of the rider, the gender of the rider, and the membership status of the rider.
    """
    GENDER_CHOICES = (
        (0, 'Unknown'),
        (1, 'Male'),
        (2, 'Female'),
    )
    ride_id = models.AutoField(primary_key=True, editable=True)
    started_at = models.DateTimeField()
    ended_at = models.DateTimeField()
    start_station = models.ForeignKey(Station, on_delete=models.SET_NULL, null=True, related_name='rides_started')
    end_station = models.ForeignKey(Station, on_delete=models.SET_NULL, null=True, related_name='rides_ended')
    bike = models.ForeignKey(Bike, on_delete=models.SET_NULL, null=True, blank=True)
    rider_birth_year = models.IntegerField(null=True, blank=True)
    rider_gender = models.IntegerField(choices=GENDER_CHOICES, default=0, null=True, blank=True)
    rider_member_or_casual = models.CharField(max_length=255, null=True, blank=True)
    source_file = models.ForeignKey(ProcessedFile, on_delete=models.CASCADE,related_name='rides')

    def __str__(self):
        return f"Ride {self.ride_id} from Station {self.start_station_id} to Station {self.end_station_id}"
