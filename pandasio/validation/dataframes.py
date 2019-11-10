from importlib import import_module
from collections import OrderedDict

import pandas as pd

from django.db import connections
from django.core.exceptions import ValidationError as DjangoValidationError

from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from rest_framework.fields import get_error_detail
from rest_framework.settings import api_settings
from rest_framework.utils import representation
from rest_framework.fields import SkipField

from pandasio.db.utils import get_dataframe_saver_backend


ALL_FIELDS = '__all__'


class DataFrameSerializer(serializers.Serializer):

    default_error_messages = {
        'invalid': 'Invalid data. Expected a dataframe, but got {datatype}'
    }

    def to_internal_value(self, data):
        """
        DataFrame of native values <- DataFrame of primitive datatypes.
        """
        if not isinstance(data, pd.DataFrame):
            message = self.error_messages['invalid'].format(
                datatype=type(data).__name__
            )
            raise ValidationError({
                api_settings.NON_FIELD_ERRORS_KEY: [message]
            }, code='invalid')

        ret = pd.DataFrame()
        errors = OrderedDict()
        fields = self._writable_fields

        for field in fields:
            validate_method = getattr(self, 'validate_' + field.field_name, None)
            primitive_value = field.get_value(data)
            try:
                validated_value = field.run_validation(primitive_value)
                if validate_method is not None:
                    validated_value = validate_method(validated_value)
            except ValidationError as exc:
                errors[field.field_name] = exc.detail
            except DjangoValidationError as exc:
                errors[field.field_name] = get_error_detail(exc)
            except SkipField:
                pass
            else:
                if len(field.source_attrs) > 1:
                    raise NotImplemented('Nested `source` is not implemented')
                ret[field.source_attrs[0]] = validated_value
                # set_value(ret, field.source_attrs, validated_value)

        if errors:
            raise ValidationError(errors)

        return ret

    def to_representation(self, instance):
        raise NotImplemented('`to_representation()` not implemented for `DataFrameSerializer`')

    def save(self, using='default'):
        connection = connections[using]
        backend_module = get_dataframe_saver_backend(connection.settings_dict['ENGINE'])
        backend = import_module(backend_module)
        saver = backend.DataFrameDatabaseSaver(connection)
        saver.save(dataframe=self.validated_data, model=self.Meta.model)

    def validate(self, dataframe):
        return dataframe

    def __repr__(self):
        return representation.serializer_repr(self, indent=1)

    def __iter__(self):
        for field in self.fields.values():
            yield self[field.field_name]


# TODO: ModelSerializer
