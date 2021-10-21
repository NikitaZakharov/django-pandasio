class BaseDataFrameDatabaseSaver(object):

    def __init__(self, connection):
        self._connection = connection

    def save(self, dataframe, model):
        raise NotImplementedError
