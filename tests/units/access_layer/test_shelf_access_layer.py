import os
import shelve
from unittest import mock

import pytest

from access import ShelfAccessLayer
from access.exceptions import (
    AccessLayerNotOpenError,
    AccessLayerLockedError,
    ItemNotFoundError,
    TransactionAbortedException,
)
from tests.units.access_layer import remove_persistence_file


class TestObject:
    def __init__(self, name):
        self.attr = 42
        self.name = name

    def __eq__(self, other):
        return self.name == other.name


def test_open_close_access_layer(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    assert al.db is None
    al.open()
    assert isinstance(al.db, shelve.Shelf)
    al.close()
    assert al.db is None
    al.purge_data()


def test_purge(shelf_path, default_suffix):
    al = ShelfAccessLayer(shelf_path)
    full_path = f"{str(shelf_path)}{default_suffix}"
    assert os.path.exists(full_path)
    al.purge_data()
    assert not os.path.exists(full_path)


def test_data_persists_on_close(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    test_object = 42
    with ShelfAccessLayer(shelf_path) as db:
        db.set_item("key", test_object)
    assert al.db is None
    with ShelfAccessLayer(shelf_path) as db:
        assert db.get_by_key("key") == test_object
    al.purge_data()


def test_operation_on_closed_access_layer(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    with pytest.raises(AccessLayerNotOpenError):
        al.set_item("key", "item")
    al.purge_data()


def test_open_locked_access_layer(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    with al as _:
        with pytest.raises(AccessLayerLockedError):
            al.open()
    al.purge_data()


def test_get_by_attribute(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    test_object = TestObject("name")
    with ShelfAccessLayer(shelf_path) as db:
        db.set_item("key", test_object)
    assert al.db is None
    with ShelfAccessLayer(shelf_path) as db:
        assert db.get_by_attribute("attr", 42) == test_object
    al.purge_data()


def test_get_multiple_by_attribute(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    test_object_1 = TestObject("1")
    test_object_2 = TestObject("2")
    test_object_3 = TestObject("3")
    with ShelfAccessLayer(shelf_path) as db:
        db.set_item("1", test_object_1)
        db.set_item("2", test_object_2)
        db.set_item("3", test_object_3)
    assert al.db is None
    with ShelfAccessLayer(shelf_path) as db:
        result = db.get_by_attribute("attr", 42, multiple=True)
    assert test_object_1 in result
    assert test_object_2 in result
    assert test_object_3 in result
    al.purge_data()


def test_sorted_results(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    test_object_1 = TestObject("1")
    test_object_2 = TestObject("2")
    test_object_3 = TestObject("3")
    with ShelfAccessLayer(shelf_path) as db:
        db.set_item("3", test_object_3)
        db.set_item("2", test_object_2)
        db.set_item("1", test_object_1)
    assert al.db is None
    with ShelfAccessLayer(shelf_path) as db:
        result = db.get_by_attribute("attr", 42, multiple=True, sort="name")
    assert result[0] == test_object_1
    assert result[1] == test_object_2
    assert result[2] == test_object_3
    al.purge_data()


def test_reverse_sorted_results(shelf_path):
    al = ShelfAccessLayer(shelf_path)
    test_object_1 = TestObject("1")
    test_object_2 = TestObject("2")
    test_object_3 = TestObject("3")
    with ShelfAccessLayer(shelf_path) as db:
        db.set_item("3", test_object_3)
        db.set_item("2", test_object_2)
        db.set_item("1", test_object_1)
    assert al.db is None
    with ShelfAccessLayer(shelf_path) as db:
        result = db.get_by_attribute(
            "attr", 42, multiple=True, sort="name", reverse=True
        )
    assert result[2] == test_object_1
    assert result[1] == test_object_2
    assert result[0] == test_object_3
    al.purge_data()


def test_item_not_found(shelf_path):
    with ShelfAccessLayer(shelf_path) as db:
        with pytest.raises(ItemNotFoundError):
            _ = db.get_by_key("key")
    remove_persistence_file(shelf_path)


def test_data_not_persist_on_abort(shelf_path):
    with pytest.raises(TransactionAbortedException):
        with ShelfAccessLayer(shelf_path) as db:
            db.set_item("key", "value")
            db.abort()
    with ShelfAccessLayer(shelf_path) as db:
        with pytest.raises(ItemNotFoundError):
            _ = db.get_by_key("key")
    remove_persistence_file(shelf_path)


def test_early_flush_persists_on_abort(shelf_path):
    with pytest.raises(TransactionAbortedException):
        with ShelfAccessLayer(shelf_path) as db:
            db.set_item("key", "value")
            db.flush()
            db.abort()
    with ShelfAccessLayer(shelf_path) as db:
        assert db.get_by_key("key") == "value"
    remove_persistence_file(shelf_path)


def test_create_db_creates_file(shelf_path, default_suffix):
    full_path = f"{str(shelf_path)}{default_suffix}"
    assert not os.path.exists(full_path)
    ShelfAccessLayer.create_db(shelf_path)
    assert os.path.exists(full_path)
    remove_persistence_file(shelf_path)
    assert not os.path.exists(full_path)


@mock.patch("access.access.ShelfAccessLayer.close")
def test_context_manager_calls_close(mock_close, shelf_path):
    with ShelfAccessLayer(shelf_path) as _:
        pass
    assert mock_close.called


@mock.patch("shelve.Shelf.close")
def test_context_manager_calls_shelf_close(mock_close, shelf_path):
    with ShelfAccessLayer(shelf_path) as _:
        pass
    assert mock_close.called


@mock.patch("shelve.open")
def test_context_manager_calls_shelf_open(mock_open, shelf_path):
    with ShelfAccessLayer(shelf_path) as _:
        assert mock_open.called


def test_get_by_condition(shelf_path):
    test_str = "GET THIS"
    ob1 = TestObject(f"name{test_str}")
    ob2 = TestObject(f"another name {test_str}")
    ob3 = TestObject("not this")
    with ShelfAccessLayer(shelf_path) as db:
        db.set_item(ob1.name, ob1)
        db.set_item(ob2.name, ob2)
        db.set_item(ob3.name, ob3)
    with ShelfAccessLayer(shelf_path) as db:
        items = db.get_by_condition(lambda item: test_str in item.name, multiple=True)
        assert ob1 in items
        assert ob2 in items
        assert ob3 not in items
