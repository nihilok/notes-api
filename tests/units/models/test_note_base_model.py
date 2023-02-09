from unittest import mock

from access import ShelfAccessLayer
from models.note import BaseNote


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
    note = BaseNote(access_layer=ShelfAccessLayer(shelf_path))
    note.save()
    assert mock_close.called
    note.access_layer.purge_data()
