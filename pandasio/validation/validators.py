from django.core.validators import deconstructible
from django.core import validators as django_validators


@deconstructible
class MaxValueValidator(django_validators.MaxValueValidator):

    message = 'Ensure column values are less than or equal to %(limit_value)s'
    code = 'max_value'

    def compare(self, a, b):
        return (a > b).any()


@deconstructible
class MinValueValidator(django_validators.MaxValueValidator):

    message = 'Ensure column values are greater than or equal to %(limit_value)s'
    code = 'min_value'

    def compare(self, a, b):
        return (a < b).any()


@deconstructible
class MinLengthValidator(django_validators.MinLengthValidator):

    message = 'Ensure column values length are greater than or equal to %(limit_value)s'
    code = 'min_length'

    def compare(self, a, b):
        return (a < b).any()

    def clean(self, x):
        return x.str.len()


@deconstructible
class MaxLengthValidator(django_validators.MaxLengthValidator):

    message = 'Ensure column values length are less than or equal to %(limit_value)s'
    code = 'max_length'

    def compare(self, a, b):
        return (a > b).any()

    def clean(self, x):
        return x.str.len()


class UniqueTogetherValidator(django_validators.BaseValidator):

    message = 'Ensure values are not duplicated by %(limit_value)s'
    code = 'duplicated'

    def compare(self, a, b):
        return a.duplicated(subset=b).any()
