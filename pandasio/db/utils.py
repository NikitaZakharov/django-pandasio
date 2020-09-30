def get_name_field_mapping(model):
    return {field.name: field for field in get_model_fields(model)}


def get_field_name(field):
    if field.is_relation:
        return field.name + "_id"
    return field.name if not field.db_column else field.db_column


def get_unique_fields(model):
    unique_fields = model._meta.unique_together
    if not unique_fields:
        return []
    unique_fields = unique_fields if not isinstance(unique_fields[0], (list, tuple)) else unique_fields[0]
    name_field_mapping = get_name_field_mapping(model)
    return [name_field_mapping[field_name] for field_name in unique_fields]


def get_unique_field_names(model, null_field_expr='COALESCE(%s, -1)'):
    return [
        ('%s' if not field.null else null_field_expr) % get_field_name(field)
        for field in get_unique_fields(model)
    ]


def get_model_fields(model):
    return model._meta.fields


def get_model_field_names(model):
    return [get_field_name(field) for field in get_model_fields(model)]


def get_manage_field_names():
    return {'id'}


def get_upsert_clause_sql(model, columns=None):
    columns = columns or get_model_field_names(model)
    upsert_columns = set(columns) - (set(get_unique_field_names(model)) | set(get_manage_field_names()))
    return ', '.join(['"%(col)s" = EXCLUDED."%(col)s"' % {'col': col} for col in upsert_columns])


def get_pk_column(model):
    return model._meta.pk


def get_pk_column_name(model):
    return get_pk_column(model).name


def get_dataframe_saver_backend(django_backend):
    mapping = {
        'django.db.backends.postgresql_psycopg2': 'pandasio.db.postgresql'
    }
    if django_backend not in mapping:
        raise Exception
    return mapping[django_backend]


def get_insert_values_sql(cursor, rows):
    placeholders = '(%s)' % ','.join(['%s'] * len(rows[0]))
    return ','.join(cursor.mogrify(placeholders, v).decode('utf-8') for v in rows)
