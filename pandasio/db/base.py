class BaseDataFrameDatabaseSaver(object):

    def __init__(self, connection):
        self._connection = connection
        self._cursor = connection.cursor()

    def save(self, dataframe, model):
        raise NotImplementedError
