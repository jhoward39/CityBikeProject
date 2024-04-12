from django.shortcuts import render
from rest_framework.views import APIView
from django.http import StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import File
from .serializers import FileSerializer

@csrf_exempt
@api_view(['GET'])
def get_files(request):
    files = File.objects.all()
    serializer = FileSerializer(files, many=True)
    return Response(serializer.data)

# @csrf_exempt
# @api_view(['POST'])
# def stream_for_file_upload(request, **files):
#     response = StreamingHttpResponse(sort_files_for_download(request))
#     return response

# @csrf_exempt
# @api_view(['POST'])
# async def sort_files_for_download(request):
#     print(request.data)
#     for file in request.data:
#         yield f"downloading {file}"
#         await download_file(file)

#     yield "done downloading files"

# def download_file( file_name):
#     pass