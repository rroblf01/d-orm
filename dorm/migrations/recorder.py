from __future__ import annotations

MIGRATIONS_TABLE = "dorm_migrations"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS "{MIGRATIONS_TABLE}" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "app" VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "applied_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE ("app", "name")
)
"""

CREATE_TABLE_SQL_PG = f"""
CREATE TABLE IF NOT EXISTS "{MIGRATIONS_TABLE}" (
    "id" SERIAL PRIMARY KEY,
    "app" VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "applied_at" TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE ("app", "name")
)
"""


class MigrationRecorder:
    """Records which migrations have been applied in the database."""

    def __init__(self, connection):
        self.connection = connection

    def ensure_table(self):
        if not self.connection.table_exists(MIGRATIONS_TABLE):
            vendor = getattr(self.connection, "vendor", "sqlite")
            sql = CREATE_TABLE_SQL_PG if vendor == "postgresql" else CREATE_TABLE_SQL
            self.connection.execute_script(sql)

    def applied_migrations(self) -> set[tuple[str, str]]:
        self.ensure_table()
        rows = self.connection.execute(
            f'SELECT "app", "name" FROM "{MIGRATIONS_TABLE}"'
        )
        return {(r[0] if not hasattr(r, "keys") else r["app"],
                 r[1] if not hasattr(r, "keys") else r["name"])
                for r in rows}

    def record_applied(self, app_label: str, name: str):
        self.ensure_table()
        vendor = getattr(self.connection, "vendor", "sqlite")
        if vendor == "postgresql":
            sql = f'INSERT INTO "{MIGRATIONS_TABLE}" ("app", "name") VALUES (%s, %s)'
        else:
            sql = f'INSERT INTO "{MIGRATIONS_TABLE}" ("app", "name") VALUES (?, ?)'
        self.connection.execute_write(sql, [app_label, name])

    def record_unapplied(self, app_label: str, name: str):
        self.ensure_table()
        vendor = getattr(self.connection, "vendor", "sqlite")
        ph = "%s" if vendor == "postgresql" else "?"
        sql = f'DELETE FROM "{MIGRATIONS_TABLE}" WHERE "app" = {ph} AND "name" = {ph}'
        self.connection.execute_write(sql, [app_label, name])
