from django.contrib import admin
from .models import File, Station, Bike, Ride

# Register your models here.
admin.site.register(File)
admin.site.register(Station)
admin.site.register(Bike)
admin.site.register(Ride)