import copy
import inspect
from collections import OrderedDict

import pandas as pd

from pandasio.validation import validators
from pandasio.validation.base import BasePandasValidator, not_provided
from pandasio.validation.exceptions import ValidationError

__all__ = [
    'Column',
    'IntegerColumn', 'BooleanColumn',
    'FloatColumn', 'StringColumn', 'DateColumn', 'DateTimeColumn',
    'ListColumn'
]


class Column(BasePandasValidator):

    default_error_messages = {
        'required': 'This column is required',
        'null': 'Ensure column values are not null'
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.required = kwargs.pop('required', True)
        self.allow_null = kwargs.pop('allow_null', False)
        self.default = kwargs.pop('default', not_provided)

        self._kwargs = kwargs


class _UnvalidatedColumn(Column):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.allow_blank = True
        self.allow_null = True

    def _validate(self, data):
        return data


class IntegerColumn(Column):

    default_error_messages = {
        'invalid': 'Ensure column values are valid integers',
        'max_value': 'Ensure column values are less than or equal to {max_value}',
        'min_value': 'Ensure column values are greater than or equal to {min_value}',
        'overflow': 'Ensure column values are not very large'
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

    def _validate(self, data):
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


class BooleanColumn(Column):

    def _validate(self, data):
        if data.dtype == bool:
            return data
        if self.allow_null:
            return data.apply(lambda x: bool(x) if not pd.isnull(x) else None, convert_dtype=False)
        return data.astype(bool)


class FloatColumn(IntegerColumn):

    def _validate(self, data):
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


class StringColumn(Column):

    default_error_messages = {
        'invalid': 'Ensure column values are valid strings',
        'blank': 'Ensure column values are not blank',
        'max_length': 'Ensure column values have no more than {max_length} characters',
        'min_length': 'Ensure column values have at least {min_length} characters',
    }

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

    def _validate(self, data):
        if data.dtype != object:
            if self.allow_null:
                data = data.apply(
                    lambda x: str(int(x)) if float.is_integer(x) else str(x) if not pd.isnull(x) else None
                )
            else:
                data = data.astype(str)
        data = data.str.strip() if self.trim_whitespace else data
        if (data == '').any() and not self.allow_blank:
            self.fail('blank')
        return data


class DateColumn(Column):

    default_error_messages = {
        'invalid': 'Ensure column values have valid date format {format}',
    }

    def __init__(self, **kwargs):
        self.format = kwargs.pop('format', None)
        assert self.format is not None, '`format` is required for date column'
        super().__init__(**kwargs)

    def _validate(self, data):
        try:
            data = pd.to_datetime(data, format=self.format, errors='raise').dt.date
            if self.allow_null:
                data = data.apply(lambda x: x if not pd.isnull(x) else None, convert_dtype=False)
        except ValueError:
            self.fail('invalid', format=self.format)
        return data


class DateTimeColumn(DateColumn):

    default_error_messages = {
        'invalid': 'Ensure column values have valid datetime format {format}',
    }

    def _validate(self, data):
        try:
            data = pd.to_datetime(data, format=self.format, errors='raise')
            if self.allow_null:
                data = data.apply(lambda x: x if not pd.isnull(x) else None, convert_dtype=False)
        except ValueError:
            self.fail('invalid', format=self.format)
        return data


class ListColumn(Column):

    child = _UnvalidatedColumn()

    default_error_messages = {
        'not_a_list': 'Ensure column values are `list` type',
        'empty': 'Ensure column values are not empty lists',
        'min_length': 'Ensure column values have at least {min_length} elements.',
        'max_length': 'Ensure column values have no more than {max_length} elements.'
    }

    def __init__(self, **kwargs):
        self.child = kwargs.pop('child', copy.deepcopy(self.child))
        self.allow_empty = kwargs.pop('allow_empty', True)
        self.max_length = kwargs.pop('max_length', None)
        self.min_length = kwargs.pop('min_length', None)

        assert not inspect.isclass(self.child), '`child` has not been instantiated.'

        super().__init__(**kwargs)
        if self.max_length is not None:
            message = self.error_messages['max_length'].format(max_length=self.max_length)
            self.validators.append(validators.MaxLengthValidator(self.max_length, message=message))
        if self.min_length is not None:
            message = self.error_messages['min_length'].format(min_length=self.min_length)
            self.validators.append(validators.MinLengthValidator(self.min_length, message=message))

    def _validate(self, data):
        if not self.allow_empty and (data.str.len() == 0).any():
            self.fail('empty')
        if not data.apply(lambda x: isinstance(x, list) or (self.allow_null and x is None)).all():
            self.fail('not_a_list')
        return self.run_child_validation(data)

    def run_child_validation(self, data):
        errors = OrderedDict()

        def validate(child, i):
            if not isinstance(child, list) and self.allow_null:
                return None
            try:
                i += 1
                return list(self.child.run_validation(pd.Series(child)))
            except ValidationError as e:
                errors[i] = e.detail

        idx = -1
        data = data.apply(lambda x: validate(x, idx))

        if not errors:
            return data

        raise ValidationError(errors)

