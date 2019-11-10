import pandas as pd

from rest_framework import viewsets
from rest_framework.response import Response
from rest_framework import status


class BaseDataFrameView(viewsets.ModelViewSet):

    def create(self, request, *args, **kwargs):
        dataframe = self.get_dataframe_from_request(request=request)
        serializer = self.get_serializer(data=dataframe)
        if serializer.is_valid(raise_exception=True):
            serializer.save()
        return Response(status=status.HTTP_201_CREATED)

    @classmethod
    def get_dataframe_from_request(cls, request):
        raise NotImplementedError


class RecordsDataFrameView(BaseDataFrameView):

    @classmethod
    def get_dataframe_from_request(cls, request):
        return pd.DataFrame.from_records(request.data)
    
    
class CSVDataFrameView(BaseDataFrameView):

    @classmethod
    def get_dataframe_from_request(cls, request):
        file = next(request.FILES.values())
        dataframe = pd.read_csv(file)
        return dataframe


class JSONDataFrameView(BaseDataFrameView):

    @classmethod
    def get_dataframe_from_request(cls, request):
        file = next(request.FILES.values())
        dataframe = pd.read_json(file)
        return dataframe
