from django.contrib import admin
from .models import ProcessedFile, Station, Bike, Ride, ProcessingFile

# Register your models here.
admin.site.register(ProcessedFile)
admin.site.register(Station)
admin.site.register(Bike)
admin.site.register(Ride)
admin.site.register(ProcessingFile)