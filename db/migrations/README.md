# Database Migrations

This folder contains all schema migrations, applied in order by `migrate.py`.

## 🧠 Guidelines

- Name migrations using the format: `NNN_description.sql`
  - e.g., `001_initial_schema.sql`, `002_add_active_column.sql`
- Each migration file should contain **pure SQLite SQL**
  - Use `CREATE TABLE`, `ALTER TABLE`, `CREATE INDEX`, etc.
  - Avoid app-specific logic or PRAGMA statements
- Migrations are applied in order, exactly once.
- A tracking table `__migrations_applied__` is created automatically in the database.

## 🛠️ Running Migrations

From the project root:

```bash
python db/migrate.py
```

This will:

1. Apply any unapplied migrations in `db/migrations/`
2. Regenerate `db/schema.sql` using `sqlite3.exe` in the `db/` folder

> ⚠️ If you don't have `sqlite3.exe`, download it from https://sqlite.org/download.html and place it in the `db/` folder. This is only needed to regenerate `schema.sql`.

## 📌 Tip

- If you're starting from an existing database, manually insert the first applied migration:
  ```sql
  INSERT INTO __migrations_applied__ (filename)
  VALUES ('001_initial_schema.sql');
  ```
