# `dorm.contrib.encrypted`

Cifrado de campo a nivel aplicación — guarda ciphertext en disco
y descifra transparente al leer. Backed by AES-GCM via el paquete
opcional `cryptography` (``pip install 'djanorm[encrypted]'``).

## Threat model

- **En alcance**: leak de snapshot de DB — backup robado, hot
  replica entregada a analista, ACL mal configurada en object
  storage. Sin la clave la columna se lee como bytes random.
- **Fuera de alcance**: un proceso que tiene la app corriendo Y
  la clave en memoria. Cifrado at rest, no en runtime.

## Quick start

```python
import dorm
from dorm.contrib.encrypted import EncryptedCharField, EncryptedTextField

class Patient(dorm.Model):
    # Lookup por igualdad funciona (modo determinista por default).
    ssn = EncryptedCharField(max_length=64)
    # Nonce random — lookup por igualdad deja de funcionar pero
    # se recupera indistinguishability.
    notes = EncryptedTextField(deterministic=False)
```

Genera una clave:

```bash
python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
```

Configura una vez:

```python
dorm.configure(FIELD_ENCRYPTION_KEY="<clave base64 de 32 bytes>")
```

## Rotación de claves

Pon `FIELD_ENCRYPTION_KEYS` a lista (más nueva primero). El cifrado
usa índice 0; el descifrado prueba cada clave en orden. Tras
suficientes writes que roten (o tras un re-cifrado manual), retira
la clave vieja de la lista.

```python
dorm.configure(FIELD_ENCRYPTION_KEYS=[
    "<clave nueva>",
    "<clave vieja>",  # se mantiene hasta re-cifrar cada fila
])
```

## Tipos de field

::: dorm.contrib.encrypted.EncryptedCharField
::: dorm.contrib.encrypted.EncryptedTextField
::: dorm.contrib.encrypted.EncryptedFieldMixin

## Qué modo elegir

| Modo                    | Lookup igualdad | Indistinguishability | Caso de uso                    |
|-------------------------|-----------------|----------------------|--------------------------------|
| `deterministic=True`    | ✅ funciona      | ❌ mismo plaintext → mismo ciphertext | filter-by-value (email, SSN)   |
| `deterministic=False`   | ❌ roto          | ✅ nonce random por write | texto bulk sin lookups        |

Range / substring / sort lookups NUNCA funcionan — el ciphertext
no preserva esos ordenamientos en ningún modo. Usa una columna
de búsqueda paralela en texto plano si los necesitas.

## Protección anti-tampering

Cada blob carga el tag de autenticación AES-GCM. Ciphertext alterado
raisea `ValueError("could not decrypt")` en vez de devolver `None`
silencioso — mejor surface el bug que ocultarlo. Un blob escrito
bajo una clave que no está en la lista de rotación activa falla
igual.
