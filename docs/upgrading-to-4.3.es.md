# Subir de 4.2 a 4.3

Sin cambios de código necesarios. Toda novedad de 4.3 es opt-in.

## Compatibilidad

- Python 3.11+ (sin cambios).
- Paquetes hermanos ``djanorm-mypy`` / ``pytest-djanorm`` mantienen
  sus pins mínimos.
- Migraciones existentes se reaplican igual.

## Cambios de comportamiento por bug fixes

Estos fueron correcciones release-blocker entre el corte y el tag;
código que dependía del bug puede cambiar:

- **``Manager.upsert(returning=True)``** antes fallaba en
  ``bulk_create`` por pasar objetos Field en lugar de nombres.
  Ahora funciona. Código que dependía del fallo (catch de
  "not yet implemented") debe adaptarse.
- **``Saga.run()`` dentro de ``atomic()``** ahora loguea WARNING.
  Auditar lugares que anidan sagas a propósito — semántica
  documentada no cambia, solo el diagnóstico.
- **``two_phase_commit``** ahora lanza ``RuntimeError`` cuando se
  invoca dentro de un bloque ``atomic()``. Restructura para salir
  del atomic antes de entrar en 2PC (siempre fue inseguro; ahora
  es error explícito).

## Orden de adopción recomendado

1. **`dorm doctor`** — recoge warnings v4.3.
2. **`dorm.contrib.permissions`** — sustituye checks ad-hoc.
3. **`dorm.contrib.concurrency.named_lock`** — sustituye helpers
   advisory-lock hand-rolled.
4. **`UUIDField(version=7)`** en tablas nuevas — PKs time-ordered
   mejoran cache locality vs UUIDv4 random.
5. **`SerializableSnapshot`** donde había loops de retry manuales.
6. **TaskQueue cron + priorities** — migrar scripts cron al
   ``@task(cron="...")`` para reutilizar outbox+retry.
7. **`dorm.contrib.slow_tx`** — staging primero; el detector
   instrumenta TODO `atomic()`, observa baseline de WARNINGs
   antes de prod.
8. **Migration linter v2** — añade DORM-M005 / DORM-M006.

## Dependencias opcionales nuevas

Ninguna obligatoria. ``stream_msgpack`` requiere ``pip install msgpack``
opt-in, sin nuevo extra dorm.

## Eliminado / deprecado

Nada eliminado. Nada deprecado.

---

Ver [Novedades en 4.3](v4_3.md) para catálogo completo de features.
