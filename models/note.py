import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from access import ShelfAccessLayer
from access.access import AccessLayer
from models.constants import DB_PATH


@dataclass
class BaseNote:
    _id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    access_layer: AccessLayer = ShelfAccessLayer(DB_PATH)
    title: Optional[str] = None
    body: Optional[str] = None
    tags: Optional[list[str]] = None

    def __eq__(self, other):
        return all(
            (
                getattr(self, attr) == getattr(other, attr)
                for attr in ("_id", "created_at", "updated_at")
            )
        )

    def save(self):
        with self.access_layer as db:
            db.set_item(self._id, self)

    def __getstate__(self):
        return (
            self._id,
            self.created_at,
            self.updated_at,
            self.title,
            self.body,
            self.tags,
            self.access_layer.__class__,
            Path(self.access_layer.path),
        )

    def __setstate__(self, state):
        (
            self._id,
            self.created_at,
            self.updated_at,
            self.title,
            self.body,
            self.tags,
            _class,
            path,
        ) = state
        self.access_layer = _class(path)


class SimpleTextNote(BaseNote):
    def __init__(self, text: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.body = text
