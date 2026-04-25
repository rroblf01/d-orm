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
from .signals import (
    Signal,
    post_delete,
    post_query,
    post_save,
    pre_delete,
    pre_query,
    pre_save,
)
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
from .functions import (
    Abs,
    Case,
    Cast,
    Coalesce,
    Concat,
    Length,
    Lower,
    Now,
    Upper,
    When,
)
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
from .indexes import Index
from .queryset import CombinedQuerySet, QuerySet, RawQuerySet
from .validators import (
    EmailValidator,
    MaxLengthValidator,
    MaxValueValidator,
    MinLengthValidator,
    MinValueValidator,
    RegexValidator,
    validate_email,
)

__version__ = "2.0.0"
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
    "pre_query",
    "post_query",
    # Base
    "Model",
    "Manager",
    "QuerySet",
    "CombinedQuerySet",
    "RawQuerySet",
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
    # Index
    "Index",
    # Validators
    "MinValueValidator",
    "MaxValueValidator",
    "MinLengthValidator",
    "MaxLengthValidator",
    "RegexValidator",
    "EmailValidator",
    "validate_email",
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
