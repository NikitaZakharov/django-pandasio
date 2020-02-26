import io

from pandasio.db.base import BaseDataFrameDatabaseSaver
from pandasio.db.utils import get_unique_field_names, get_upsert_clause_sql, get_insert_values_sql


class DataFrameDatabaseSaver(BaseDataFrameDatabaseSaver):

    def save(self, dataframe, model, returning_columns=None):
        if returning_columns is not None:
            return self.upsert(dataframe=dataframe, model=model, returning_columns=returning_columns)
        buffer = io.StringIO()
        dataframe.to_csv(buffer, sep='\t', header=False, index=False)
        buffer.seek(0)
        try:
            self._cursor.copy_from(file=buffer, table=model._meta.db_table, columns=dataframe.columns, null='')
            self._connection.commit()
        except Exception as e:
            print(e)
            self._connection.rollback()
            self.upsert(dataframe=dataframe, model=model)

    def upsert(self, dataframe, model, returning_columns=None):
        upsert_clause = get_upsert_clause_sql(model)
        do_statement = 'DO UPDATE SET %s ' % upsert_clause if upsert_clause else 'DO NOTHING '
        returning_statement = ('RETURNING %s' % ', '.join(returning_columns)) if returning_columns else ''
        try:
            self._cursor.execute(
                'INSERT INTO %(table)s (%(columns)s) '
                'VALUES %(values)s '
                'ON CONFLICT (%(unique_columns)s) '
                '%(do_statement)s '
                '%(returning_statement)s' % {
                    'table': model._meta.db_table,
                    'columns': ','.join(dataframe.columns),
                    'values': get_insert_values_sql(self._cursor, dataframe.to_dict('split')['data']),
                    'unique_columns': ', '.join(get_unique_field_names(model)),
                    'do_statement': do_statement,
                    'returning_statement': returning_statement
                }
            )
            self._connection.commit()
            if returning_columns:
                return self._cursor.fetchall()
        except Exception as e:
            print(e)
            self._connection.rollback()
            raise e
