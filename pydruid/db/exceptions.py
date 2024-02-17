class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class CompileError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


# Allow for the support of using `sqlalchemy.exc.CompileError` when using the
# `extra_require` of sqlalchemy - implemented in #243
support_error_child_cls = None

try:
    from sqlalchemy.exc import CompileError

    support_error_child_cls = CompileError
except ImportError:
    support_error_child_cls = DatabaseError


class NotSupportedError(support_error_child_cls):
    pass
