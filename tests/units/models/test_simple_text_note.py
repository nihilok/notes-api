from access import ShelfAccessLayer
from models.note import SimpleTextNote


def test_save_simple_text_note(shelf_path):
    note = SimpleTextNote("this is the note", access_layer=ShelfAccessLayer(shelf_path))
    note.save()
    with note.access_layer as db:
        assert (db_note := db.get_by_key(note._id)) == note
        assert db_note.body == "this is the note"
    note.access_layer.purge_data()
