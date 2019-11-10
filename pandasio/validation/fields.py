import pandas as pd

from rest_framework import serializers
from rest_framework.utils.formatting import lazy_format

from pandasio.validation import validators

__all__ = [
    'Empty', 'Field',
    'IntegerField', 'BooleanField', 'NullBooleanField',
    'FloatField', 'CharField', 'DateField'
]


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
        super().__init__(*args, **kwargs)

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
            if getattr(self.root, 'partial', False):
                raise serializers.SkipField()
            if self.required:
                self.fail('required')
            return True, self.get_default()

        if column.isnull().any():
            if not self.allow_null:
                self.fail('null')
            # Nullable `source='*'` fields should not be skipped when its named
            # field is given a null value. This is because `source='*'` means
            # the field is passed the entire object, which is not null.
            elif self.source == '*':
                return False, column
            return True, column

        return False, column


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
            message = lazy_format(self.error_messages['max_value'], max_value=self.max_value)
            self.validators.append(
                validators.MaxValueValidator(self.max_value, message=message)
            )
        if self.min_value is not None:
            message = lazy_format(self.error_messages['min_value'], min_value=self.min_value)
            self.validators.append(
                validators.MinValueValidator(self.min_value, message=message)
            )

    def to_internal_value(self, data):
        if data.dtype == int:
            return data

        try:
            data = data.astype(int)
        except ValueError:
            self.fail('invalid')
        except OverflowError:
            self.fail('overflow')

        return data

    def to_representation(self, value):
        return value.astype(int)


class BooleanField(Field):

    def to_internal_value(self, data):
        if data.dtype == bool:
            return data
        return data.astype(bool)

    def to_representation(self, value):
        if value.dtype == bool:
            return value
        return value.astype(bool)


class NullBooleanField(Field):

    def __init__(self, **kwargs):
        assert 'allow_null' not in kwargs, '`allow_null` is not a valid option.'
        kwargs['allow_null'] = True
        super().__init__(**kwargs)

    def to_internal_value(self, data):
        return data.apply(lambda x: bool(x) if not pd.isnull(x) else None)

    def to_representation(self, value):
        return value.apply(lambda x: bool(x) if not pd.isnull(x) else None)


class FloatField(IntegerField):

    def to_internal_value(self, data):
        if data.dtype == float:
            return data

        try:
            data = data.astype(float)
        except ValueError:
            self.fail('invalid')

        return data

    def to_representation(self, value):
        return value.astype(float)


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
        super().__init__(**kwargs)
        if self.max_length is not None:
            message = lazy_format(self.error_messages['max_length'], max_length=self.max_length)
            self.validators.append(
                validators.MaxLengthValidator(self.max_length, message=message)
            )
        if self.min_length is not None:
            message = lazy_format(self.error_messages['min_length'], min_length=self.min_length)
            self.validators.append(
                validators.MinLengthValidator(self.min_length, message=message)
            )

    def to_internal_value(self, data):
        if data.dtype != object:
            data = data.astype(str)
        data = data.str.strip() if self.trim_whitespace else data
        if (data == '').any() and not self.allow_blank:
            self.fail('blank')
        return data.str.strip() if self.trim_whitespace else data

    def to_representation(self, value):
        return value.astype(str)


class DateField(Field):

    default_error_messages = {
        'invalid': 'Date values have wrong format. Use one of these formats instead: {format}',
    }

    def __init__(self, **kwargs):
        self.format = kwargs.pop('format', serializers.empty)
        assert self.format is not serializers.empty, '`format` is required for date column'
        super().__init__(**kwargs)

    def to_internal_value(self, value):
        try:
            value = pd.to_datetime(value, format=self.format, errors='raise').dt.date
        except ValueError:
            self.fail('invalid', format=self.format)
        return value

    def to_representation(self, value):
        return value.apply(lambda x: x.strftime(self.format) if not pd.isnull(x) else x)
