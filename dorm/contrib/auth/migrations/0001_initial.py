"""
Auto-generated migration.
Generated: 2026-05-03T07:01:43.371149+00:00
"""
from dorm.migrations.operations import (
    CreateModel,
)
from dorm.fields import BigAutoField
from dorm.fields import BooleanField
from dorm.fields import CharField
from dorm.fields import DateTimeField
from dorm.fields import EmailField
from dorm.fields import ManyToManyField

dependencies = []

operations = [
    CreateModel(
        name='User',
        fields=[('id', BigAutoField(primary_key=True)), ('username', CharField(max_length=150, unique=True)), ('email', EmailField(max_length=254, unique=True)), ('password', CharField(max_length=128)), ('is_active', BooleanField(default=True)), ('is_staff', BooleanField(default=False)), ('is_superuser', BooleanField(default=False)), ('date_joined', DateTimeField()), ('last_login', DateTimeField(null=True, blank=True)), ('groups', ManyToManyField('Group', null=True, blank=True)), ('user_permissions', ManyToManyField('Permission', null=True, blank=True))],
        options={'db_table': 'auth_user', 'ordering': ['email'], 'unique_together': [], 'indexes': [], 'constraints': []},
    ),
    CreateModel(
        name='Permission',
        fields=[('id', BigAutoField(primary_key=True)), ('name', CharField(max_length=255)), ('codename', CharField(max_length=100, unique=True))],
        options={'db_table': 'auth_permission', 'ordering': ['codename'], 'unique_together': [], 'indexes': [], 'constraints': []},
    ),
    CreateModel(
        name='Group',
        fields=[('id', BigAutoField(primary_key=True)), ('name', CharField(max_length=150, unique=True)), ('permissions', ManyToManyField('Permission', null=True, blank=True))],
        options={'db_table': 'auth_group', 'ordering': ['name'], 'unique_together': [], 'indexes': [], 'constraints': []},
    ),
]
