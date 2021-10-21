from django.core.validators import deconstructible
from django.core import validators as django_validators


class BaseValidator(django_validators.BaseValidator):

    def get_valid_data(self, data):
        raise NotImplemented


@deconstructible
class MaxValueValidator(BaseValidator):

    message = 'Ensure column values are less than or equal to %(limit_value)s'
    code = 'max_value'

    def get_valid_data(self, data):
        return data[data <= self.limit_value]

    def compare(self, a, b):
        return (a > b).any()


@deconstructible
class MinValueValidator(BaseValidator):

    message = 'Ensure column values are greater than or equal to %(limit_value)s'
    code = 'min_value'

    def get_valid_data(self, data):
        return data[data >= self.limit_value]

    def compare(self, a, b):
        return (a < b).any()


@deconstructible
class MinLengthValidator(BaseValidator):

    message = 'Ensure column values length are greater than or equal to %(limit_value)s'
    code = 'min_length'

    def get_valid_data(self, data):
        return data[self.clean(data) >= self.limit_value]

    def compare(self, a, b):
        return (a < b).any()

    def clean(self, x):
        return x.str.len()


@deconstructible
class MaxLengthValidator(BaseValidator):

    message = 'Ensure column values length are less than or equal to %(limit_value)s'
    code = 'max_length'

    def get_valid_data(self, data):
        return data[self.clean(data) <= self.limit_value]

    def compare(self, a, b):
        return (a > b).any()

    def clean(self, x):
        return x.str.len()


class UniqueTogetherValidator(BaseValidator):

    message = 'Ensure values are not duplicated by %(limit_value)s'
    code = 'duplicated'

    def get_valid_data(self, data):
        return data[~data.duplicated(subset=self.limit_value)]

    def compare(self, a, b):
        return a.duplicated(subset=b).any()
