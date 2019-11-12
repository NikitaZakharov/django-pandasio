import io

from pandasio.db.base import BaseDataFrameDatabaseSaver
from pandasio.db.utils import get_unique_field_names, get_upsert_clause_sql, get_pk_column_name, get_insert_values_sql


class DataFrameDatabaseSaver(BaseDataFrameDatabaseSaver):

    def save(self, dataframe, model):
        buffer = io.StringIO()
        dataframe.to_csv(buffer, sep='\t', header=False, index=False)
        buffer.seek(0)
        try:
            self._cursor.copy_from(file=buffer, table=model._meta.db_table, columns=dataframe.columns, null='')
        except Exception as e:
            print(e)
            self.upsert(dataframe=dataframe, model=model)

    def upsert(self, dataframe, model):
        upsert_clause = get_upsert_clause_sql(model)
        do_statement = 'DO UPDATE SET %s ' % upsert_clause if upsert_clause else 'DO NOTHING '
        self._cursor.execute(
            'INSERT INTO %(table)s (%(columns)s) '
            'VALUES %(values)s '
            'ON CONFLICT (%(unique_columns)s) '
            '%(do_statement)s '
            '%(returning_insert_id_sql)s' % {
                'table': model._meta.db_table,
                'columns': ','.join(dataframe.columns),
                'values': get_insert_values_sql(self._cursor, dataframe.to_dict('split')['data']),
                'unique_columns': ', '.join(get_unique_field_names(model)),
                'do_statement': do_statement,
                'returning_insert_id_sql': self._connection.ops.return_insert_id()[0] % get_pk_column_name(model)
            }
        )
