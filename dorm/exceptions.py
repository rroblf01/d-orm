class DoesNotExist(Exception):
    pass


class MultipleObjectsReturned(Exception):
    pass


class FieldDoesNotExist(Exception):
    pass


class ValidationError(Exception):
    def __init__(self, message, code=None):
        self.message = message
        self.code = code
        super().__init__(message)


class DatabaseError(Exception):
    pass


class IntegrityError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class MigrationError(Exception):
    pass


class ImproperlyConfigured(Exception):
    pass


class ObjectDoesNotExist(DoesNotExist):
    pass
