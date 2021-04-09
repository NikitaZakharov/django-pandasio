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
from pandasio.validation.errors import ValidationTypeError

ALL_FIELDS = '__all__'


class DataFrameSerializer(serializers.Serializer):

    default_error_messages = {
        'invalid': 'Invalid data. Expected a dataframe, but got {datatype}'
    }

    @property
    def failure_cases(self):
        field_cases = [field.get_failure_cases(field.get_value(self.initial_data)) for field in self._writable_fields]
        validator_cases = [validator.get_invalid_data(self.initial_data) for validator in self.validators]
        first_df = pd.concat(field_cases, axis=1)
        second_df = pd.concat(validator_cases, axis=0)
        return first_df.combine_first(second_df).reset_index()

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
            except ValidationTypeError as exc:
                errors[field.field_name] = exc.detail
                raise ValidationTypeError(errors)
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

    def is_valid(self, raise_exception=False):
        assert not hasattr(self, 'restore_object'), (
            'Serializer `%s.%s` has old-style version 2 `.restore_object()` '
            'that is no longer compatible with REST framework 3. '
            'Use the new-style `.create()` and `.update()` methods instead.' %
            (self.__class__.__module__, self.__class__.__name__)
        )

        assert hasattr(self, 'initial_data'), (
            'Cannot call `.is_valid()` as no `data=` keyword argument was '
            'passed when instantiating the serializer instance.'
        )

        if not hasattr(self, '_validated_data'):
            try:
                self._validated_data = self.run_validation(self.initial_data)
            except ValidationTypeError as exc:
                self._validated_data = pd.DataFrame()
                if raise_exception:
                    raise ValidationTypeError(exc.detail)
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
