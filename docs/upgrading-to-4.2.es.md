# Subir de 4.1 a 4.2

Sin cambios de código necesarios. Toda novedad de 4.2 es opt-in o
gratis cuando no se usa.

## Compatibilidad

- Python 3.11+ (sin cambios).
- Paquetes hermanos ``djanorm-mypy`` / ``pytest-djanorm`` siguen
  funcionando sin actualizar (pin mínimo ``djanorm >= 4.0``).
- Migraciones existentes se reaplican igual — toda nueva
  :class:`Operation` (``MakeTableAppendOnly``,
  ``AlterColumnTypeOnline``) es clase nueva; nada renombrado ni
  eliminado.

## Cambios de comportamiento a tener en cuenta

No son rupturas de API — misma firma, mismo tipo de retorno — pero
los valores por defecto recorren un camino distinto al de 4.1:

- **`dorm migrate --dry-run <target>`** antes salía con exit 1; en
  4.2 imprime el SQL del rollback. Actualiza tests CI que esperaban
  el error.
- **Métricas Prometheus** incluyen
  ``dorm_pool_saturation{alias}`` y (al llamar a
  :func:`querystats.collector().enable()`) líneas
  ``dorm_template_*``. Dashboards que comprueban métricas ausentes
  verán claves nuevas.
- **`dorm.configure(DEBUG_NPLUSONE=True)`** auto-instala el
  detector global N+1. Si ya llamabas a :func:`install_debug_global`
  manualmente, la segunda llamada es no-op — déjala por
  compatibilidad.

## Dependencias opcionales nuevas

Ninguna. v4.2 conserva la matriz de instalación de v4.1
(`[postgresql]`, `[litestar]`, `[parquet]`, …).

## Orden recomendado de adopción

Las features son independientes — usa solo las que necesites. Un
rollout típico de stack productivo:

1. **`dorm doctor`** — ejecuta contra config actual y triage los
   nuevos warnings antes de adoptar otra feature.
2. **`dorm.contrib.querystats`** — habilita collector, observa
   distribución de queries durante una semana antes de decidir qué
   indexar.
3. **Gauge de saturación de pool** — se activa automáticamente al
   exponer `prometheus.metrics_response()`.
4. **`SLOW_QUERY_EXPLAIN=True`** — primero staging; coste duplicado
   sobre slow queries no suele ser problema pero verifica logs.
5. **`MakeTableAppendOnly`** sobre tablas `<Model>_history` que no
   necesiten updates.
6. **`sql_allowlist`** — canary capture, curado, enforcement.
7. **PG advisory locks** donde haya leader-election manual o cron
   singleton.
8. **`Broadcaster`** + **`DataLoader`** en stacks async (GraphQL,
   FastAPI con fan-out).

## Eliminado / deprecado

Nada eliminado. Nada deprecado.

---

Ver [Novedades en 4.2](v4_2.md) para catálogo completo de features.
