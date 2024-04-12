from django.shortcuts import render
from rest_framework.views import APIView
from django.http import StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import ProcessedFile
from .serializers import ProcessedFileSerializer
