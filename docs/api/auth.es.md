# `dorm.contrib.auth`

Modelos opcionales `User` / `Group` / `Permission`, framework-agnósticos.
Provee solo el modelo de datos + helpers de hash de password — vistas
de login, sesiones, decorators de permisos y middleware son trabajo
del framework.

Instala con `INSTALLED_APPS = ["dorm.contrib.auth", ...]`. Sin dependencia
extra: el hash de password usa stdlib `hashlib.pbkdf2_hmac`.

## Quick start

```python
from dorm.contrib.auth.models import User, Group, Permission

user = User.objects.create_user(email="alice@example.com", password="hunter2")
user.check_password("hunter2")    # True
user.set_password("new-password")
user.save()

# Permisos: explícitos por usuario, o vía Group.
publish = Permission.objects.create(name="Publish", codename="articles.publish")
editors = Group.objects.create(name="editors")
editors.permissions.add(publish)
user.groups.add(editors)
user.has_perm("articles.publish")  # True
```

## Modelos


## Hash de password

PBKDF2-SHA256 stdlib por defecto, formato
``pbkdf2_sha256$<iterations>$<salt>$<hash>`` — misma forma que
emite Django, así los passwords migran limpio entre ambos ORMs.

### Argon2id (3.1+, opt-in)

Instala el extra opcional:

```bash
pip install "djanorm[auth-argon2]"
```

Luego usa `make_password_argon2` en lugar de `make_password`. El
output lleva prefijo `argon2$` para que `check_password` despache
por tag de algoritmo — hashes PBKDF2 y Argon2 verifican por la
misma llamada:

```python
from dorm.contrib.auth.password import (
    make_password_argon2, check_password,
)

h = make_password_argon2("secret-pw")  # "argon2$$argon2id$..."
assert check_password("secret-pw", h)
```

Argon2id es el estado del arte actual en hash de passwords
(memory-hard, resistente a bruteforce GPU / ASIC). PBKDF2 queda
como default porque viene en stdlib sin extensión C.


## Tokens de reset / verificación

Tokens stateless firmados con HMAC para flujos password-reset /
email-verification. La firma se ata a `last_login` / `password` /
`email` del usuario, así un solo uso del token (que cambia el
password) invalida todas las URLs pendientes.

```python
from dorm.contrib.auth.tokens import default_token_generator

token = default_token_generator.make_token(user)
# … embed en URL del email de reset …

if default_token_generator.check_token(user, posted_token):
    user.set_password(new_password)
    user.save()  # Salt rota — token se invalida automáticamente.
```


## Sync de `Meta.permissions`

Permisos custom declarados en el modelo via ``Meta.permissions``
se materializan como filas en ``auth_permission`` al llamar
:func:`sync_permissions`. Los verbos default (``add_x``, ``change_x``,
``delete_x``, ``view_x``) se auto-emiten por modelo concrete.
Idempotente — seguro llamar en cada deploy.

```python
class Article(dorm.Model):
    class Meta:
        permissions = [
            ("articles.publish", "Can publish articles"),
            ("articles.archive", "Can archive articles"),
        ]

# Ejecutar una vez tras migrate (o desde hook de deploy):
from dorm.contrib.auth.management import sync_permissions
sync_permissions()  # → int, cuenta de filas nuevas creadas
```

