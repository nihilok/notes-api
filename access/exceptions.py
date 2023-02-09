class AccessLayerException(Exception):
    pass


class AccessLayerNotOpenError(AccessLayerException):
    pass


class AccessLayerLockedError(AccessLayerException):
    pass


class DataError(AccessLayerException):
    pass


class ItemNotFoundError(DataError):
    pass


class TransactionAbortedException(AccessLayerException):
    pass
