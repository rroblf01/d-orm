"""Tests for Tier-4.6: MigrationAutodetector RenameModel/RenameField detection."""
from __future__ import annotations

import dorm
from dorm.migrations.autodetector import MigrationAutodetector
from dorm.migrations.operations import (
    AddField,
    CreateModel,
    DeleteModel,
    RemoveField,
    RenameField,
    RenameModel,
)
from dorm.migrations.state import ProjectState


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_state(models: dict) -> ProjectState:
    """Build a ProjectState from a dict of {key: {name, fields, options}}."""
    state = ProjectState()
    state.models = dict(models)
    return state


def _model(name: str, fields: dict, app: str = "myapp") -> tuple[str, dict]:
    key = f"{app}.{name.lower()}"
    return key, {"name": name, "fields": fields, "options": {}}


def _char_field():
    return dorm.CharField(max_length=100)


def _int_field():
    return dorm.IntegerField()


# ── RenameModel via explicit hints ────────────────────────────────────────────

def test_rename_model_hint():
    k1, v1 = _model("OldName", {"id": dorm.AutoField(primary_key=True), "name": _char_field()})
    k2, v2 = _model("NewName", {"id": dorm.AutoField(primary_key=True), "name": _char_field()})

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(
        from_state,
        to_state,
        rename_hints={"models": {"myapp": {"OldName": "NewName"}}},
    )
    changes = detector.changes("myapp")

    ops = changes.get("myapp", [])
    assert len(ops) == 1
    assert isinstance(ops[0], RenameModel)
    assert ops[0].old_name == "OldName"
    assert ops[0].new_name == "NewName"


def test_rename_model_hint_no_extra_create_delete():
    k1, v1 = _model("Foo", {"id": dorm.AutoField(primary_key=True)})
    k2, v2 = _model("Bar", {"id": dorm.AutoField(primary_key=True)})

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(
        from_state,
        to_state,
        rename_hints={"models": {"myapp": {"Foo": "Bar"}}},
    )
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])
    assert not any(isinstance(op, (DeleteModel, CreateModel)) for op in ops)


# ── RenameModel via heuristic ─────────────────────────────────────────────────

def test_detect_renames_model_identical_fields():
    k1, v1 = _model("OldModel", {"id": dorm.AutoField(primary_key=True), "title": _char_field()})
    k2, v2 = _model("NewModel", {"id": dorm.AutoField(primary_key=True), "title": _char_field()})

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(from_state, to_state, detect_renames=True)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    renames = [op for op in ops if isinstance(op, RenameModel)]
    assert len(renames) == 1
    assert renames[0].old_name == "OldModel"
    assert renames[0].new_name == "NewModel"


def test_detect_renames_model_different_fields_no_rename():
    k1, v1 = _model("Foo", {"id": dorm.AutoField(primary_key=True), "x": _char_field()})
    k2, v2 = _model("Bar", {"id": dorm.AutoField(primary_key=True), "y": _int_field()})

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(from_state, to_state, detect_renames=True)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    # Fields differ → should be delete+create, not rename
    assert not any(isinstance(op, RenameModel) for op in ops)
    assert any(isinstance(op, DeleteModel) for op in ops)
    assert any(isinstance(op, CreateModel) for op in ops)


# ── RenameField via explicit hints ────────────────────────────────────────────

def test_rename_field_hint():
    k1, v1 = _model("Article", {
        "id": dorm.AutoField(primary_key=True),
        "headline": _char_field(),
    })
    k2, v2 = _model("Article", {
        "id": dorm.AutoField(primary_key=True),
        "title": _char_field(),
    })

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(
        from_state,
        to_state,
        rename_hints={"fields": {"myapp.Article": {"headline": "title"}}},
    )
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    renames = [op for op in ops if isinstance(op, RenameField)]
    assert len(renames) == 1
    assert renames[0].old_name == "headline"
    assert renames[0].new_name == "title"
    assert renames[0].model_name == "Article"


def test_rename_field_hint_no_remove_add():
    k1, v1 = _model("Foo", {"id": dorm.AutoField(primary_key=True), "old": _char_field()})
    k2, v2 = _model("Foo", {"id": dorm.AutoField(primary_key=True), "new": _char_field()})

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(
        from_state,
        to_state,
        rename_hints={"fields": {"myapp.Foo": {"old": "new"}}},
    )
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    # No RemoveField or AddField for the renamed field
    assert not any(isinstance(op, (RemoveField, AddField)) for op in ops)


# ── RenameField via heuristic ─────────────────────────────────────────────────

def test_detect_renames_field_same_type():
    k1, v1 = _model("Post", {
        "id": dorm.AutoField(primary_key=True),
        "body": _char_field(),
    })
    k2, v2 = _model("Post", {
        "id": dorm.AutoField(primary_key=True),
        "content": _char_field(),
    })

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(from_state, to_state, detect_renames=True)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    renames = [op for op in ops if isinstance(op, RenameField)]
    assert len(renames) == 1
    assert renames[0].old_name == "body"
    assert renames[0].new_name == "content"


def test_detect_renames_field_different_types_no_rename():
    k1, v1 = _model("Tag", {
        "id": dorm.AutoField(primary_key=True),
        "count": _int_field(),
    })
    k2, v2 = _model("Tag", {
        "id": dorm.AutoField(primary_key=True),
        "label": _char_field(),  # different type
    })

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(from_state, to_state, detect_renames=True)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    # Types differ → should be remove+add, not rename
    assert not any(isinstance(op, RenameField) for op in ops)
    assert any(isinstance(op, RemoveField) for op in ops)
    assert any(isinstance(op, AddField) for op in ops)


def test_detect_renames_field_ambiguous_no_rename():
    """Two fields removed, two added — too ambiguous, no rename emitted."""
    k1, v1 = _model("Item", {
        "id": dorm.AutoField(primary_key=True),
        "a": _char_field(),
        "b": _char_field(),
    })
    k2, v2 = _model("Item", {
        "id": dorm.AutoField(primary_key=True),
        "c": _char_field(),
        "d": _char_field(),
    })

    from_state = _make_state({k1: v1})
    to_state = _make_state({k2: v2})

    detector = MigrationAutodetector(from_state, to_state, detect_renames=True)
    changes = detector.changes("myapp")
    ops = changes.get("myapp", [])

    # Ambiguous — no renames, just remove+add
    assert not any(isinstance(op, RenameField) for op in ops)


# ── No changes ────────────────────────────────────────────────────────────────

def test_no_changes():
    k1, v1 = _model("Widget", {"id": dorm.AutoField(primary_key=True), "name": _char_field()})
    from_state = _make_state({k1: v1})
    to_state = _make_state({k1: v1})

    detector = MigrationAutodetector(from_state, to_state)
    changes = detector.changes("myapp")
    assert changes == {}
