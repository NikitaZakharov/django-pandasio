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
from pandasio.validation.base import BasePandasValidator


ALL_FIELDS = '__all__'


class DataFrameValidator(BasePandasValidator):

    default_error_messages = {
        'invalid': 'Invalid data. Expected a dataframe, but got {datatype}'
    }

    def run_columns_validation(self, data):
        ret = pd.DataFrame()
        errors = OrderedDict()

        for column in self._columns:
            validate_method = getattr(self, 'validate_' + column.column_name, None)
            column_data = data[column.source_attrs[0]]
            try:
                validated_column_data = column.run_validation(column_data)
                if validate_method is not None:
                    validated_column_data = validate_method(validated_column_data)
            except ValidationError as exc:
                errors[column.column_name] = exc.detail
            except DjangoValidationError as exc:
                errors[column.column_name] = get_error_detail(exc)
            except SkipField:
                pass
            else:
                if len(column.source_attrs) > 1:
                    raise NotImplemented('Nested `source` is not implemented')
                ret[column.source_attrs[0]] = validated_column_data
                # set_value(ret, field.source_attrs, validated_value)

        if errors:
            raise ValidationError(errors)

        return ret

    def is_valid(self, raise_exception=False):
        if not hasattr(self, '_validated_data'):
            try:
                self._validated_data = self.run_validation(self.initial_data)
            except ValidationError as exc:
                self._validated_data = pd.DataFrame()
                self._errors = exc.detail
            else:
                self._errors = {}

        if self._errors and raise_exception:
            raise ValidationError(self.errors)

        return not bool(self._errors)

    def run_validation(self, data):
        (is_empty_value, data) = self.validate_empty_values(data)
        if is_empty_value:
            return data

        data = self.to_internal_value(data)
        try:
            self.run_validators(data)
            data = self.validate(data)
            assert data is not None, '.validate() should return the validated data'
        except (ValidationError, DjangoValidationError) as exc:
            raise ValidationError(detail=serializers.as_serializer_error(exc))

        return data

    def run_validators(self, data):
        errors = []
        for validator in self.validators:
            if hasattr(validator, 'set_context'):
                validator.set_context(self)
            try:
                if getattr(validator, 'requires_context', False):
                    validator(data, self)
                else:
                    validator(data)
            except ValidationError as exc:
                # If the validation error contains a mapping of fields to
                # errors then simply raise it immediately rather than
                # attempting to accumulate a list of errors.
                if isinstance(exc.detail, dict):
                    raise
                errors.extend(exc.detail)
            except DjangoValidationError as exc:
                errors.extend(get_error_detail(exc))
        if errors:
            raise ValidationError(errors)

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
