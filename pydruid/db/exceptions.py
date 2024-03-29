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


class NotSupportedError(CompileError):
    pass
