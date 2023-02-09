from unittest import mock

from access import ShelfAccessLayer
from models.note import BaseNote, SimpleTextNote


def test_note_model_equal_to_shelved_version(shelf_path):
    note = BaseNote()
    with ShelfAccessLayer(shelf_path) as db:
        db.set_item(note._id, note)
    with ShelfAccessLayer(shelf_path) as db:
        assert db.get_by_key(note._id) == note
    al = ShelfAccessLayer(shelf_path)
    al.purge_data()


@mock.patch("access.ShelfAccessLayer.close")
def test_close_called_on_save(mock_close, shelf_path):
    note = SimpleTextNote("this is the note", access_layer=ShelfAccessLayer(shelf_path))
    note.save()
    assert mock_close.called


def test_save_simple_text_note(shelf_path):
    note = SimpleTextNote("this is the note", access_layer=ShelfAccessLayer(shelf_path))
    note.save()
    with note.access_layer as db:
        assert (db_note := db.get_by_key(note._id)) == note
        assert db_note.body == "this is the note"
    note.access_layer.purge_data()
