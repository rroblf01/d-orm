"""Generic relations: a polymorphic FK that can point at any registered model.

Mirrors Django's :mod:`django.contrib.contenttypes`. A
:class:`~dorm.contrib.contenttypes.models.ContentType` row identifies a
model class by (``app_label``, ``model``); a
:class:`~dorm.contrib.contenttypes.fields.GenericForeignKey` ties together
a ``content_type`` FK and an ``object_id`` column to point at *any* row
of *any* model. Use :class:`~dorm.contrib.contenttypes.fields.GenericRelation`
on the target side to walk the reverse direction.

Add ``"dorm.contrib.contenttypes"`` to ``INSTALLED_APPS`` and run
``dorm makemigrations`` / ``dorm migrate`` to create the
``django_content_type`` table before using these features.
"""

from .fields import GenericForeignKey, GenericRelation
from .models import ContentType

__all__ = [
    "ContentType",
    "GenericForeignKey",
    "GenericRelation",
]
