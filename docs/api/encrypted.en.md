# `dorm.contrib.encrypted`

Application-level field encryption — stores ciphertext on disk and
decrypts transparently on read. Backed by AES-GCM via the optional
`cryptography` package (``pip install 'djanorm[encrypted]'``).

## Threat model

- **In scope**: a database snapshot leak — stolen backup, hot
  replica handed to an analyst, misconfigured object-storage ACL.
  Without the key the column reads as random bytes.
- **NOT in scope**: a process that has both the running app and
  the key in memory. Encryption is at rest, not at runtime.

## Quick start

```python
import dorm
from dorm.contrib.encrypted import EncryptedCharField, EncryptedTextField

class Patient(dorm.Model):
    # Equality lookup works (deterministic mode by default).
    ssn = EncryptedCharField(max_length=64)
    # Random nonce — equality lookup stops working but
    # indistinguishability is restored.
    notes = EncryptedTextField(deterministic=False)
```

Generate a key:

```bash
python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
```

Configure once:

```python
dorm.configure(FIELD_ENCRYPTION_KEY="<base64 32-byte key>")
```

## Key rotation

Set `FIELD_ENCRYPTION_KEYS` to a list (newest first). Encryption
uses index 0; decryption tries each in order. After enough writes
roll over (or after a manual re-encrypt pass), retire the old key
from the list.

```python
dorm.configure(FIELD_ENCRYPTION_KEYS=[
    "<new key>",
    "<old key>",  # kept until every row has been re-encrypted
])
```

## Field types

::: dorm.contrib.encrypted.EncryptedCharField
::: dorm.contrib.encrypted.EncryptedTextField
::: dorm.contrib.encrypted.EncryptedFieldMixin

## When to pick which mode

| Mode                    | Equality lookup | Indistinguishability | Use case                       |
|-------------------------|-----------------|----------------------|--------------------------------|
| `deterministic=True`    | ✅ works         | ❌ same plaintext → same ciphertext | filter-by-value (email, SSN)    |
| `deterministic=False`   | ❌ broken        | ✅ random nonce per write | bulk text where lookups don't matter |

Range / substring / sort lookups will NEVER work — the ciphertext
doesn't preserve those orderings on either mode. Use a separate
plaintext search-helper column when you need them.

## Tampering protection

Every blob carries the AES-GCM authentication tag. Tampered
ciphertext raises `ValueError("could not decrypt")` rather than
silently returning `None` — better to surface the bug than mask
it. A blob written under a key not in the active rotation list
fails the same way.
