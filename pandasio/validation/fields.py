import warnings
import pandas as pd

from rest_framework import serializers
from rest_framework.fields import MISSING_ERROR_MESSAGE
from rest_framework import (
    RemovedInDRF313Warning
)
from rest_framework.exceptions import ValidationError
from rest_framework.fields import get_error_detail, empty
from django.core.exceptions import ValidationError as DjangoValidationError

from pandasio.validation import validators


__all__ = [
    'Empty', 'Field',
    'IntegerField', 'BooleanField', 'NullBooleanField',
    'FloatField', 'CharField', 'DateField', 'DateTimeField'
]

NOT_ALLOW_NULL_REPLACE_NULL = 'May not set both `allow_null=False` and `replace_null`'

class Empty(object):
    pass


class NotProvided(object):
    pass


class Field(serializers.Field):

    default_error_messages = {
        'required': 'This column is required',
        'null': 'This column cannot contain null values'
    }

    def __init__(self, *args, **kwargs):
        self.replace_null = kwargs.pop('replace_null', None)
        super().__init__(*args, **kwargs)
        assert self.allow_null or self.replace_null is None, NOT_ALLOW_NULL_REPLACE_NULL

        self._errors = set()

    @property
    def errors(self):
        return self._errors

    def fail(self, key, **kwargs):
        try:
            msg = self.error_messages[key].format(**kwargs)
        except KeyError:
            class_name = self.__class__.__name__
            msg = MISSING_ERROR_MESSAGE.format(class_name=class_name, key=key)

        self._errors.add(msg)

    def to_internal_value(self, data):
        return data

    def validate_empty_values(self, column):
        """
        Validate empty values, and either:

        * Raise `ValidationError`, indicating invalid data.
        * Raise `SkipField`, indicating that the field should be ignored.
        * Return (True, data), indicating an empty value that should be
          returned without any further validation being applied.
        * Return (False, data), indicating a non-empty value, that should
          have validation applied as normal.
        """
        if self.read_only:
            return True, self.get_default()

        if column is serializers.empty:
            if self.required:
                self.fail('required')
                return True, pd.Series()
            return True, self.get_default()

        if column.isnull().any():
            if self.replace_null is not None:
                column = column.fillna(self.replace_null)
            elif not self.allow_null:
                self.fail('null')
                return False, column[column.notnull()]
            # Nullable `source='*'` fields should not be skipped when its named
            # field is given a null value. This is because `source='*'` means
            # the field is passed the entire object, which is not null.
            elif self.source == '*':
                return False, column
            return False, column

        return False, column

    def run_validation(self, data=empty):
        """
        Validate a simple representation and return the internal value.

        The provided data may be `empty` if no representation was included
        in the input.

        May raise `SkipField` if the field should not be included in the
        validated data.
        """
        (is_empty_value, data) = self.validate_empty_values(data)
        if is_empty_value:
            return data
        value = self.to_internal_value(data)
        value = self.run_validators(value)
        return value

    def run_validators(self, value):
        """
        Test the given value against all the validators on the field,
        and either raise a `ValidationError` or simply return.
        """
        for validator in self.validators:
            if hasattr(validator, 'set_context'):
                warnings.warn(
                    "Method `set_context` on validators is deprecated and will "
                    "no longer be called starting with 3.13. Instead set "
                    "`requires_context = True` on the class, and accept the "
                    "context as an additional argument.",
                    RemovedInDRF313Warning, stacklevel=2
                )
                validator.set_context(self)

            try:
                if getattr(validator, 'requires_context', False):
                    validator(value, self)
                else:
                    validator(value)
            except ValidationError as exc:
                # If the validation error contains a mapping of fields to
                # errors then simply raise it immediately rather than
                # attempting to accumulate a list of errors.
                if isinstance(exc.detail, dict):
                    print('Raise dict Validation/fields')
                    print(exc.detail)
                    raise
                value = validator.get_valid_data(value)
                self._errors.add(exc.detail)
            except DjangoValidationError as exc:
                value = validator.get_valid_data(value)
                self._errors |= set(get_error_detail(exc))

        return value


class _UnvalidatedField(Field):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.allow_blank = True
        self.allow_null = True

    def to_internal_value(self, data):
        return data

    def to_representation(self, value):
        return value


class IntegerField(Field):

    default_error_messages = {
        'invalid': 'A valid integer values are required',
        'max_value': 'Ensure column values are less than or equal to {max_value}',
        'min_value': 'Ensure column values are greater than or equal to {min_value}',
        'overflow': 'Passed values are too large'
    }

    def __init__(self, **kwargs):
        self.max_value = kwargs.pop('max_value', None)
        self.min_value = kwargs.pop('min_value', None)
        super().__init__(**kwargs)
        if self.max_value is not None:
            message = self.error_messages['max_value'].format(max_value=self.max_value)
            self.validators.append(
                validators.MaxValueValidator(self.max_value, message=message)
            )
        if self.min_value is not None:
            message = self.error_messages['min_value'].format(min_value=self.min_value)
            self.validators.append(
                validators.MinValueValidator(self.min_value, message=message)
            )

    def to_internal_value(self, data):
        if data.dtype == int:
            return data

        try:
            if self.allow_null:
                data = data.apply(lambda x: int(x) if not pd.isnull(x) else None, convert_dtype=False)
            else:
                data = data.astype(int)
        except ValueError:
            self.fail('invalid')

            def is_valid_element(el):
                try:
                    if self.allow_null and pd.isnull(el):
                        return True
                    int(el)
                    return True
                except ValueError:
                    return False
            return data[data.apply(lambda x: is_valid_element(x))].astype(int)
        except OverflowError:
            self.fail('overflow')

        return data

    def to_representation(self, value):
        return value


class BooleanField(Field):

    def to_internal_value(self, data):
        if data.dtype == bool:
            return data
        return data.astype(bool)

    def to_representation(self, value):
        return value


class NullBooleanField(Field):

    def __init__(self, **kwargs):
        assert 'allow_null' not in kwargs, '`allow_null` is not a valid option.'
        kwargs['allow_null'] = True
        super().__init__(**kwargs)

    def to_type(self, data):
        if data.dtype == bool:
            return data
        return data.apply(lambda x: bool(x) if not pd.isnull(x) else None, convert_dtype=False)

    def to_internal_value(self, data):
        if data.dtype == bool:
            return data
        return data.apply(lambda x: bool(x) if not pd.isnull(x) else None, convert_dtype=False)

    def to_representation(self, value):
        return value


class FloatField(IntegerField):

    def to_internal_value(self, data):
        if data.dtype == float and not self.allow_null:
            return data

        try:
            if self.allow_null:
                data = data.apply(lambda x: float(x) if not pd.isnull(x) else None, convert_dtype=False)
            else:
                data = data.astype(float)
        except ValueError:
            self.fail('invalid')

            def is_valid_element(el):
                try:
                    if self.allow_null and pd.isnull(el):
                        return True
                    float(el)
                    return True
                except ValueError:
                    return False
            return data[data.apply(lambda x: is_valid_element(x))].astype(float)

        return data

    def to_representation(self, value):
        return value


class CharField(Field):

    default_error_messages = {
        'invalid': 'Not a valid string',
        'blank': 'This column may not be blank',
        'max_length': 'Ensure column values have no more than {max_length} characters',
        'min_length': 'Ensure column values have at least {min_length} characters',
    }
    initial = ''

    def __init__(self, **kwargs):
        self.allow_blank = kwargs.pop('allow_blank', False)
        self.trim_whitespace = kwargs.pop('trim_whitespace', True)
        self.max_length = kwargs.pop('max_length', None)
        self.min_length = kwargs.pop('min_length', None)
        self.trim_extra = kwargs.pop('trim_extra', False)
        super().__init__(**kwargs)
        if self.max_length is not None and not self.trim_extra:
            message = self.error_messages['max_length'].format(max_length=self.max_length)
            self.validators.append(
                validators.MaxLengthValidator(self.max_length, message=message)
            )
        if self.min_length is not None:
            message = self.error_messages['min_length'].format(min_length=self.min_length)
            self.validators.append(
                validators.MinLengthValidator(self.min_length, message=message)
            )

    def to_internal_value(self, data):
        if data.dtype != object:
            if self.allow_null and data.dtype == float:
                data = data.apply(lambda x: str(int(x)) if x.is_integer() else str(x) if not pd.isnull(x) else None)
            else:
                data = data.astype(str)
        else:
            if self.allow_null:
                data = data.apply(lambda x: str(x) if not pd.isnull(x) else None)
            else:
                data = data.astype(str)
        data = data.str.strip() if self.trim_whitespace else data
        if (data == '').any() and not self.allow_blank:
            self.fail('blank')
            return data[data != '']
        if self.trim_extra and self.max_length is not None:
            data = data.str[:self.max_length]
        return data

    def to_representation(self, value):
        return value


class DateField(Field):

    default_error_messages = {
        'invalid': 'Date values have wrong format. Use one of these formats instead: {format}',
    }

    def __init__(self, **kwargs):
        self.format = kwargs.pop('format', serializers.empty)
        assert self.format is not serializers.empty, '`format` is required for date column'
        super().__init__(**kwargs)

    def to_internal_value(self, data):
        try:
            data = pd.to_datetime(data, format=self.format, errors='coerce' if self.allow_null else 'raise').dt.date
            if self.allow_null:
                data = data.apply(lambda x: x if not pd.isnull(x) else self.replace_null, convert_dtype=False)
        except ValueError:
            self.fail('invalid', format=self.format)

            series = pd.to_datetime(data, format=self.format, errors='coerce').dt.date
            return series[series.notna()]
        return data

    def to_representation(self, value):
        return value.apply(lambda x: x.strftime(self.format) if not pd.isnull(x) else x)


class DateTimeField(Field):

    default_error_messages = {
        'invalid': 'Datetime values have wrong format. Use one of these formats instead: {format}',
    }

    def __init__(self, **kwargs):
        self.format = kwargs.pop('format', serializers.empty)
        assert self.format is not serializers.empty, '`format` is required for datetime column'
        super().__init__(**kwargs)

    def to_internal_value(self, data):
        try:
            data = pd.to_datetime(data, format=self.format, errors='raise')
            if self.allow_null:
                data = data.apply(lambda x: x if not pd.isnull(x) else None, convert_dtype=False)
        except ValueError:
            self.fail('invalid', format=self.format)

            series = pd.to_datetime(data, format=self.format, errors='coerce')
            return series[series.notna()]
        return data

    def to_representation(self, value):
        return value.apply(lambda x: x.strftime(self.format) if not pd.isnull(x) else x)
