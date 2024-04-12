from django.urls import path
from . import views

urlpatterns = [
    path('get_files/', views.get_files, name="get_files"),
    path('upload_files/', views.stream_for_file_upload, name="stream_for_file_download"),
]