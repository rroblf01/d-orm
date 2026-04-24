"""
djanorm: A Django-like ORM with synchronous and asynchronous support.

Quick start:

    import dorm

    dorm.configure(
        DATABASES={
            'default': {
                'ENGINE': 'sqlite',
                'NAME': 'db.sqlite3',
            }
        },
        INSTALLED_APPS=['myapp'],
    )

    class Author(dorm.Model):
        name = dorm.CharField(max_length=100)
        age  = dorm.IntegerField()

    # Sync
    author = Author.objects.create(name='Alice', age=30)
    authors = Author.objects.filter(age__gte=18).order_by('name')

    # Async
    async def main():
        author = await Author.objects.acreate(name='Bob', age=25)
        author = await Author.objects.aget(name='Bob')
"""

from .conf import configure, settings
from . import signals, transaction
from .signals import Signal, post_delete, post_save, pre_delete, pre_save
from .exceptions import (
    DatabaseError,
    DoesNotExist,
    FieldDoesNotExist,
    ImproperlyConfigured,
    IntegrityError,
    MigrationError,
    MultipleObjectsReturned,
    OperationalError,
    ProtectedError,
    ValidationError,
)
from .expressions import F, Q, Value
from .aggregates import Avg, Count, Max, Min, StdDev, Sum, Variance
from .functions import Abs, Case, Cast, Coalesce, Concat, Length, Lower, Now, Upper, When
from .fields import (
    AutoField,
    BigAutoField,
    BigIntegerField,
    BinaryField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    EmailField,
    FloatField,
    ForeignKey,
    GenericIPAddressField,
    IPAddressField,
    IntegerField,
    JSONField,
    ManyToManyField,
    NullBooleanField,
    OneToOneField,
    PositiveIntegerField,
    PositiveSmallIntegerField,
    SlugField,
    SmallAutoField,
    SmallIntegerField,
    TextField,
    TimeField,
    URLField,
    UUIDField,
    CASCADE,
    PROTECT,
    SET_NULL,
    SET_DEFAULT,
    DO_NOTHING,
    RESTRICT,
)
from .models import Model
from .manager import Manager
from .queryset import CombinedQuerySet, QuerySet

__version__ = "0.1.0"
__all__ = [
    # Config
    "configure",
    "settings",
    "transaction",
    "signals",
    # Signals
    "Signal",
    "pre_save",
    "post_save",
    "pre_delete",
    "post_delete",
    # Base
    "Model",
    "Manager",
    "QuerySet",
    "CombinedQuerySet",
    # Fields
    "AutoField",
    "BigAutoField",
    "SmallAutoField",
    "CharField",
    "TextField",
    "IntegerField",
    "SmallIntegerField",
    "BigIntegerField",
    "PositiveIntegerField",
    "PositiveSmallIntegerField",
    "FloatField",
    "DecimalField",
    "BooleanField",
    "NullBooleanField",
    "DateField",
    "TimeField",
    "DateTimeField",
    "EmailField",
    "URLField",
    "SlugField",
    "UUIDField",
    "IPAddressField",
    "GenericIPAddressField",
    "JSONField",
    "BinaryField",
    "ForeignKey",
    "OneToOneField",
    "ManyToManyField",
    # on_delete constants
    "CASCADE",
    "PROTECT",
    "SET_NULL",
    "SET_DEFAULT",
    "DO_NOTHING",
    "RESTRICT",
    # Expressions
    "Q",
    "F",
    "Value",
    # Aggregates
    "Count",
    "Sum",
    "Avg",
    "Max",
    "Min",
    "StdDev",
    "Variance",
    # Functions
    "Case",
    "When",
    "Coalesce",
    "Now",
    "Concat",
    "Cast",
    "Upper",
    "Lower",
    "Length",
    "Abs",
    # Exceptions
    "DoesNotExist",
    "MultipleObjectsReturned",
    "FieldDoesNotExist",
    "ValidationError",
    "DatabaseError",
    "IntegrityError",
    "ProtectedError",
    "OperationalError",
    "MigrationError",
    "ImproperlyConfigured",
]
