from importlib import import_module
from collections import defaultdict

import pandas as pd

from django.db import connections
from django.core.exceptions import ValidationError as DjangoValidationError

from rest_framework import serializers
from rest_framework.exceptions import ValidationError
from rest_framework.fields import get_error_detail, SkipField
from rest_framework.settings import api_settings
from rest_framework.utils import representation
from rest_framework.fields import MISSING_ERROR_MESSAGE

from pandasio.db.utils import get_dataframe_saver_backend

ALL_FIELDS = '__all__'


class DataFrameSerializer(serializers.Serializer):

    default_error_messages = {
        'invalid': 'Invalid data. Expected a dataframe, but got {datatype}'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._errors = defaultdict(list)

    def fail(self, key, **kwargs):
        try:
            msg = self.error_messages[key].format(**kwargs)
        except KeyError:
            class_name = self.__class__.__name__
            msg = MISSING_ERROR_MESSAGE.format(class_name=class_name, key=key)
        self._errors[key].append(msg)

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
        fields = self._writable_fields
        df_valid = True

        for field in fields:
            validate_method = getattr(self, 'validate_' + field.field_name, None)
            primitive_value = field.get_value(data)
            try:
                validated_value = field.run_validation(primitive_value)
                if validate_method is not None:
                    validated_value = validate_method(validated_value)

                if validated_value.empty:
                    df_valid = False

                is_not_valid = not (self.initial_data.shape[0] == validated_value.size == ret.shape[0])

                if df_valid and not ret.empty and is_not_valid:
                    ret = ret.join(validated_value.to_frame(field.source_attrs[0]), how='inner')
                else:
                    ret[field.source_attrs[0]] = validated_value

                # if (ret.empty and not self._errors) or ret.shape[0] == validated_value.size:
                #     ret[field.source_attrs[0]] = validated_value
                # else:
                #     ret = ret.join(validated_value.to_frame(field.source_attrs[0]), how='inner')

            except SkipField:
                df_valid = False
                self._errors[field.field_name] = field.errors

            if field.errors:
                self._errors[field.field_name] = field.errors

        if df_valid:
            return ret
        return pd.DataFrame()

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
            except ValidationError as exc:
                self._validated_data = pd.DataFrame()
                self._errors[api_settings.NON_FIELD_ERRORS_KEY] = exc.detail

        if self._errors and raise_exception:
            raise ValidationError(self._errors)

        return not bool(self._errors)

    def run_validation(self, data):
        (is_empty_value, data) = self.validate_empty_values(data)
        if is_empty_value:
            return data

        data = self.to_internal_value(data)
        try:
            data = self.run_validators(data)
            data = self.validate(data)
            assert data is not None, '.validate() should return the validated data'
        except (ValidationError, DjangoValidationError) as exc:
            raise ValidationError(detail=serializers.as_serializer_error(exc))

        return data

    def run_validators(self, data):
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
                data = validator.get_valid_data(data)
                self._errors[api_settings.NON_FIELD_ERRORS_KEY] = exc.detail
            except DjangoValidationError as exc:
                data = validator.get_valid_data(data)
                self._errors[api_settings.NON_FIELD_ERRORS_KEY] = get_error_detail(exc)
        return data

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
