import os
import shelve
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import Any, Hashable, Iterable, Optional, Union

from access.exceptions import (
    AccessLayerLockedError,
    AccessLayerNotOpenError,
    ItemNotFoundError,
    TransactionAbortedException,
)

FILE_SUFFIX = ".db"


def operation(f):
    @wraps(f)
    def wrapper(self: "AccessLayer", *args, **kw):
        if self.db is None:
            raise AccessLayerNotOpenError
        return f(self, *args, **kw)

    return wrapper


def locked(f):
    @wraps(f)
    def wrapper(self: "AccessLayer", *args, **kw):
        if self.db is None:
            return f(self, *args, **kw)
        raise AccessLayerLockedError

    return wrapper


class AccessLayer(ABC):
    @abstractmethod
    def __init__(self, path: Path):
        self.path = str(path)
        self.db = None

    def __enter__(self):
        raise NotImplemented

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplemented

    def open(self) -> "AccessLayer":
        raise NotImplemented

    def close(self):
        raise NotImplemented

    def set_item(self, key: Hashable, item: Any):
        raise NotImplemented

    def get_by_key(self, key: Hashable) -> Any:
        raise NotImplemented

    def get_by_attribute(
        self,
        name: str,
        attribute: Any,
        subset: Iterable[Any],
        multiple: bool,
        sort: Union[str, bool],
        reverse: bool,
    ) -> Any:
        raise NotImplemented

    def get_by_condition(
        self,
        condition,
        subset: Iterable[Any],
        multiple: bool,
        sort: Union[str, bool],
        reverse: bool,
    ) -> Any:
        raise NotImplemented

    def del_by_key(self, key):
        raise NotImplemented

    def flush(self):
        raise NotImplemented

    def abort(self):
        raise NotImplemented

    def purge_data(self):
        raise NotImplemented


class ShelfAccessLayer(AccessLayer):
    def __init__(self, path: Path):
        self.path = str(path)
        self.db: Optional[shelve.Shelf] = None
        self.create_db(path)
        self._aborted = False
        self.transaction_cache = {}

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def create_db(path: Path):
        _dir = path.parent if not path.is_dir() else path
        names = [
            os.path.splitext(name)[0]
            for (dirname, _, filenames) in os.walk(_dir)
            if dirname == str(_dir)
            for name in filenames
        ]
        if path.name in names:
            return
        db = shelve.open(str(path), "n")
        db.close()

    @locked
    def open(self):
        self.db = shelve.open(self.path, "w")
        return self

    @operation
    def close(self):
        if self._aborted:
            self._aborted = False
            self.db = None
            self.transaction_cache = {}
            return
        self.flush()
        self.db.close()
        self.db = None

    @operation
    def get_by_key(self, key: str):
        try:
            return self.db[key]
        except KeyError as e:
            raise ItemNotFoundError(e)

    @operation
    def get_by_attribute(
        self, name, attribute, subset=None, multiple=False, sort=False, reverse=False
    ):
        subset = subset or self.db.values()
        if sort is True:
            sort = name
        found = []
        for v in subset:
            if getattr(v, name, None) == attribute:
                if not multiple:
                    return v
                found.append(v)
        if sort:
            found.sort(key=lambda item: getattr(item, sort, None), reverse=reverse)
        return found

    @operation
    def get_by_condition(
        self,
        condition,
        subset=None,
        multiple=False,
        sort: Union[str, bool] = False,
        reverse=False,
    ):
        subset = subset or self.db.values()
        found = []
        for v in subset:
            if condition(v):
                if not multiple:
                    return v
                found.append(v)
        if sort:
            found.sort(
                key=lambda item: getattr(item, sort, None)
                if sort is not True
                else None,
                reverse=reverse,
            )
        return found

    @operation
    def del_by_key(self, key):
        del self.db[key]

    @operation
    def flush(self):
        self.db.update(**self.transaction_cache)
        self.transaction_cache = {}

    def abort(self):
        self._aborted = True
        raise TransactionAbortedException

    def purge_data(self):
        os.remove(self.path + FILE_SUFFIX)

    @operation
    def set_item(self, key, item):
        self.transaction_cache[key] = item
