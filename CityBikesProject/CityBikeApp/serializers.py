from rest_framework import serializers
from .models import ProcessedFile

class ProcessedFileSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProcessedFile
        fields = ['file_name', 'file_path', 'last_modified', 'size']