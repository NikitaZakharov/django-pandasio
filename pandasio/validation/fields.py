import copy
import inspect
from collections import OrderedDict

import pandas as pd

from rest_framework import serializers

from pandasio.validation import validators

__all__ = [
    'Empty', 'Field',
    'IntegerField', 'BooleanField', 'NullBooleanField',
    'FloatField', 'CharField', 'DateField', 'DateTimeField',
    'ListField'
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
            if self.replace_null is not None:
                column = column.fillna(self.replace_null)
            elif not self.allow_null:
                self.fail('null')
            # Nullable `source='*'` fields should not be skipped when its named
            # field is given a null value. This is because `source='*'` means
            # the field is passed the entire object, which is not null.
            elif self.source == '*':
                return False, column
            return False, column

        return False, column


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

    def to_internal_value(self, data):
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
        super().__init__(**kwargs)
        if self.max_length is not None:
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
                data = data.apply(lambda x: x if not pd.isnull(x) else None, convert_dtype=False)
        except ValueError:
            self.fail('invalid', format=self.format)
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
        return data

    def to_representation(self, value):
        return value.apply(lambda x: x.strftime(self.format) if not pd.isnull(x) else x)


class ListField(Field):

    child = _UnvalidatedField()

    default_error_messages = {
        'not_a_list': 'Ensure column values are `list` type',
        'empty': 'Column values cannot contain empty lists',
        'min_length': 'Ensure column values have at least {min_length} elements.',
        'max_length': 'Ensure column values have no more than {max_length} elements.'
    }

    def __init__(self, *args, **kwargs):
        self.child = kwargs.pop('child', copy.deepcopy(self.child))
        self.allow_empty = kwargs.pop('allow_empty', True)
        self.max_length = kwargs.pop('max_length', None)
        self.min_length = kwargs.pop('min_length', None)

        assert not inspect.isclass(self.child), '`child` has not been instantiated.'
        assert self.child.source is None, (
            "The `source` argument is not meaningful when applied to a `child=` field. "
            "Remove `source=` from the field declaration."
        )

        super().__init__(*args, **kwargs)
        self.child.bind(field_name='', parent=self)
        if self.max_length is not None:
            message = self.error_messages['max_length'].format(max_length=self.max_length)
            self.validators.append(validators.MaxLengthValidator(self.max_length, message=message))
        if self.min_length is not None:
            message = self.error_messages['min_length'].format(min_length=self.min_length)
            self.validators.append(validators.MinLengthValidator(self.min_length, message=message))

    def to_internal_value(self, data):
        if not self.allow_empty and not data.str.len().all():
            self.fail('empty')
        if not data.apply(lambda x: isinstance(x, list) or (self.allow_null and x is None)).all():
            self.fail('not_a_list')
        return self.run_child_validation(data)

    def to_representation(self, data):
        return pd.Series([self.child.to_representation(lst) if lst is not None else None for lst in data])

    def run_child_validation(self, data):
        errors = OrderedDict()

        def validate(child, i):
            if not isinstance(child, list) and self.allow_null:
                return None
            try:
                i += 1
                return list(self.child.run_validation(pd.Series(child)))
            except serializers.ValidationError as e:
                errors[i] = e.detail

        idx = -1
        data = data.apply(lambda x: validate(x, idx))

        if not errors:
            return data

        raise serializers.ValidationError(errors)

