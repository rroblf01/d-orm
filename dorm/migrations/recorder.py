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

# MySQL / MariaDB rejects ``INTEGER PRIMARY KEY AUTOINCREMENT``
# (sqlite syntax) — the corresponding form is
# ``BIGINT AUTO_INCREMENT PRIMARY KEY``. Single quotes around the
# default keep the DDL portable across MySQL 5.7 / 8 + MariaDB.
CREATE_TABLE_SQL_MYSQL = f"""
CREATE TABLE IF NOT EXISTS "{MIGRATIONS_TABLE}" (
    "id" BIGINT AUTO_INCREMENT PRIMARY KEY,
    "app" VARCHAR(255) NOT NULL,
    "name" VARCHAR(255) NOT NULL,
    "applied_at" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
            if vendor == "postgresql":
                sql = CREATE_TABLE_SQL_PG
            elif vendor == "mysql":
                sql = CREATE_TABLE_SQL_MYSQL
            else:
                sql = CREATE_TABLE_SQL
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
        # ``%s`` is dorm's canonical placeholder; the SQLite backend
        # rewrites to ``?`` on its own.
        sql = (
            f'INSERT INTO "{MIGRATIONS_TABLE}" ("app", "name") VALUES (%s, %s)'
        )
        self.connection.execute_write(sql, [app_label, name])

    def record_unapplied(self, app_label: str, name: str):
        self.ensure_table()
        sql = (
            f'DELETE FROM "{MIGRATIONS_TABLE}" '
            'WHERE "app" = %s AND "name" = %s'
        )
        self.connection.execute_write(sql, [app_label, name])
