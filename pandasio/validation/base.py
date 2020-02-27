from abc import abstractmethod

from pandasio.validation.exceptions import ValidationError


class Empty(object):
    pass


empty = Empty()


class NotProvided(object):
    pass


not_provided = NotProvided()


class BasePandasValidator(object):

    default_error_messages = {}

    _errors = {}
    _validators = []
    _validated_data = None

    def __init__(self, **kwargs):
        messages = {}
        for cls in reversed(self.__class__.__mro__):
            messages.update(getattr(cls, 'default_error_messages', {}))
        self.error_messages = messages

    @abstractmethod
    def _validate(self, data):
        return data

    @abstractmethod
    def validate(self, data):
        # For custom validate
        return data

    def run_validation(self, data):
        data = self._validate(data)
        data = self.validate(data)
        assert data is not None, '.validate() should return the validated data'
        self.run_validators(data)
        return data

    # .validators is a lazily loaded property, that gets its default
    # value from `get_validators`.
    @property
    def validators(self):
        if not hasattr(self, '_validators'):
            self._validators = self.get_validators()
        return self._validators

    @validators.setter
    def validators(self, validators):
        self._validators = validators

    @abstractmethod
    def get_validators(self):
        return []

    def run_validators(self, data):
        errors = []
        for validator in self.validators:
            if hasattr(validator, 'set_context'):
                validator.set_context(self)
            try:
                validator(data)
            except ValidationError as exc:
                # If the validation error contains a mapping of fields to
                # errors then simply raise it immediately rather than
                # attempting to accumulate a list of errors.
                if isinstance(exc.detail, dict):
                    raise
                errors.extend(exc.detail)

        if errors:
            raise ValidationError(errors)

    @abstractmethod
    def is_valid(self, raise_exception=False):
        pass

    @property
    def errors(self):
        if not hasattr(self, '_errors'):
            msg = 'You must call `.is_valid()` before accessing `.errors`.'
            raise AssertionError(msg)
        return self._errors

    @property
    def validated_data(self):
        if not hasattr(self, '_validated_data'):
            msg = 'You must call `.is_valid()` before accessing `.validated_data`.'
            raise AssertionError(msg)
        return self._validated_data

    def fail(self, key, **kwargs):
        """
        A helper method that simply raises a validation error.
        """
        try:
            msg = self.error_messages[key]
        except KeyError:
            raise AssertionError('Missing error message')
        message_string = msg.format(**kwargs)
        raise ValidationError(message_string, code=key)
