# `dorm.contrib.auth`

Optional `User` / `Group` / `Permission` models, framework-agnostic.
Provides only the data model + password-hashing helpers — login
views, sessions, permission decorators and middleware are the
framework's job.

Install with `INSTALLED_APPS = ["dorm.contrib.auth", ...]`. No extra
dependency: password hashing uses stdlib `hashlib.pbkdf2_hmac`.

## Quick start

```python
from dorm.contrib.auth.models import User, Group, Permission

user = User.objects.create_user(email="alice@example.com", password="hunter2")
user.check_password("hunter2")    # True
user.set_password("new-password")
user.save()

# Permissions: explicit per user, or via Group membership.
publish = Permission.objects.create(name="Publish", codename="articles.publish")
editors = Group.objects.create(name="editors")
editors.permissions.add(publish)
user.groups.add(editors)
user.has_perm("articles.publish")  # True
```

## Models

::: dorm.contrib.auth.models.User
::: dorm.contrib.auth.models.Group
::: dorm.contrib.auth.models.Permission
::: dorm.contrib.auth.models.UserManager

## Password hashing

Stdlib PBKDF2-SHA256 by default, format
``pbkdf2_sha256$<iterations>$<salt>$<hash>`` — same shape Django
emits, so passwords migrate cleanly between the two ORMs.

::: dorm.contrib.auth.password.make_password
::: dorm.contrib.auth.password.check_password
::: dorm.contrib.auth.password.is_password_usable

### Argon2id (3.1+, opt-in)

Install the optional extra:

```bash
pip install "djanorm[auth-argon2]"
```

Then use `make_password_argon2` in place of `make_password`. The
output is prefixed with `argon2$` so `check_password` dispatches
by algorithm tag — both PBKDF2 and Argon2 hashes verify through
the same call:

```python
from dorm.contrib.auth.password import (
    make_password_argon2, check_password,
)

h = make_password_argon2("secret-pw")  # "argon2$$argon2id$..."
assert check_password("secret-pw", h)
```

Argon2id is the current state of the art for password hashing
(memory-hard, resistant to GPU / ASIC bruteforce). PBKDF2 stays
the default because it ships in stdlib without a C extension.

## Reset / verification tokens

Stateless HMAC-signed tokens for password-reset / email-verification
flows. The signature binds to the user's `last_login` / `password`
/ `email`, so a single use of the token (which changes the password)
invalidates every outstanding URL.

```python
from dorm.contrib.auth.tokens import default_token_generator

token = default_token_generator.make_token(user)
# … embed in reset email URL …

if default_token_generator.check_token(user, posted_token):
    user.set_password(new_password)
    user.save()  # Salt rolls — token invalidates automatically.
```

::: dorm.contrib.auth.tokens.PasswordResetTokenGenerator
::: dorm.contrib.auth.tokens.default_token_generator
::: dorm.contrib.auth.tokens.generate_short_lived_token

## `Meta.permissions` sync

Custom permissions declared on a model via ``Meta.permissions``
materialise into ``auth_permission`` rows when you call
:func:`sync_permissions`. Default verbs (``add_x``, ``change_x``,
``delete_x``, ``view_x``) get auto-emitted per concrete model.
Idempotent — safe to call every deploy.

```python
class Article(dorm.Model):
    class Meta:
        permissions = [
            ("articles.publish", "Can publish articles"),
            ("articles.archive", "Can archive articles"),
        ]

# Run once after migrate (or wire into a deploy hook):
from dorm.contrib.auth.management import sync_permissions
sync_permissions()  # → ints, count of new rows created
```

::: dorm.contrib.auth.management.sync_permissions
