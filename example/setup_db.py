"""
Creates database tables and loads initial seed data.
Run once before the demos.

    # From the example/ directory:
    python setup_db.py

    # From the project root:
    uv run python example/setup_db.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql
from example.models import Author, Book, Genre, Review


# ── Output helpers ─────────────────────────────────────────────────────────────

RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[32m"
CYAN = "\033[36m"


def header(text: str):
    print(f"\n{BOLD}{CYAN}{'─' * 50}{RESET}")
    print(f"{BOLD}{CYAN}  {text}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 50}{RESET}")


def ok(text: str):
    print(f"  {GREEN}✓{RESET}  {text}")


# ── Create tables ──────────────────────────────────────────────────────────────


def create_tables():
    header("Creating tables")
    conn = get_connection()

    for model in [Genre, Author, Book, Review]:
        table = model._meta.db_table
        conn.execute_script(f'DROP TABLE IF EXISTS "{table}"')
        cols = [
            _field_to_column_sql(f.name, f, conn)
            for f in model._meta.fields
            if f.db_type(conn)
        ]
        conn.execute_script(
            f'CREATE TABLE IF NOT EXISTS "{table}" (\n  '
            + ",\n  ".join(filter(None, cols))
            + "\n)"
        )
        ok(f"Table '{table}' created")


# ── Seed data ──────────────────────────────────────────────────────────────────


def seed_data():
    header("Loading seed data")

    # Genres
    fiction, _ = Genre.objects.get_or_create(
        name="Fiction", defaults={"description": "Imaginative narrative"}
    )
    scifi, _ = Genre.objects.get_or_create(
        name="Science Fiction", defaults={"description": "Scientific speculation"}
    )
    mystery, _ = Genre.objects.get_or_create(
        name="Mystery", defaults={"description": "Suspense and investigation"}
    )
    nonfiction, _ = Genre.objects.get_or_create(
        name="Non-Fiction", defaults={"description": "Essays and popular science"}
    )
    ok("4 genres created")

    # Authors
    orwell = Author.objects.create(
        name="George Orwell",
        email="orwell@example.com",
        birth_year=1903,
        nationality="British",
        bio="Novelist and essayist known for his critique of totalitarianism.",
    )
    asimov = Author.objects.create(
        name="Isaac Asimov",
        email="asimov@example.com",
        birth_year=1920,
        nationality="American",
        bio="Prolific science fiction writer and science communicator.",
    )
    christie = Author.objects.create(
        name="Agatha Christie",
        email="christie@example.com",
        birth_year=1890,
        nationality="British",
        bio="Queen of crime fiction, creator of Hercule Poirot and Miss Marple.",
    )
    sagan = Author.objects.create(
        name="Carl Sagan",
        email="sagan@example.com",
        birth_year=1934,
        nationality="American",
        bio="Astronomer and popular science communicator.",
        active=False,
    )
    ok("4 authors created")

    # Books
    books_data = [
        ("Nineteen Eighty-Four", orwell,   fiction,    "9780451524935", 328,  9.99, 1949, 15),
        ("Animal Farm",          orwell,   fiction,    "9780451526342", 112,  7.50, 1945, 20),
        ("Foundation",           asimov,   scifi,      "9780553293357", 255, 11.99, 1951, 10),
        ("I, Robot",             asimov,   scifi,      "9780553294385", 253, 10.50, 1950,  8),
        ("The Bicentennial Man", asimov,   scifi,      "9780385420785", 137,  8.99, 1976,  5),
        ("Murder on the Orient Express", christie, mystery, "9780062693662", 256, 12.99, 1934, 18),
        ("And Then There Were None",     christie, mystery, "9780062073488", 264, 11.50, 1939, 12),
        ("Cosmos",               sagan,    nonfiction, "9780345539434", 365, 15.99, 1980,  3),
    ]

    for title, author, genre, isbn, pages, price, year, stock in books_data:
        Book.objects.create(
            title=title,
            author_id=author.pk,
            genre_id=genre.pk,
            isbn=isbn,
            pages=pages,
            price=price,
            published_year=year,
            stock=stock,
        )
    ok("8 books created")

    # Reviews
    reviews_data = [
        (1, "Maria Lopez",   5, "A masterpiece, absolutely essential."),
        (1, "Carlos Garcia", 4, "Disturbing and brilliant."),
        (2, "Ana Martinez",  5, "Simple but profoundly moving."),
        (3, "Peter Ruiz",    5, "The best science fiction saga ever written."),
        (3, "Laura Sanchez", 4, "An epic start to the series."),
        (4, "James Torres",  4, "Perfect short stories."),
        (6, "Sofia Ramirez", 5, "Christie at her very best."),
        (7, "Mike Flores",   5, "The greatest whodunit ever written."),
        (8, "Elena Moreno",  5, "Changed my view of the universe."),
    ]

    all_books = {b.pk: b for b in Book.objects.all()}
    for book_idx, reviewer, rating, comment in reviews_data:
        book_pk = list(all_books.keys())[book_idx - 1]
        Review.objects.create(
            book_id=book_pk,
            reviewer=reviewer,
            rating=rating,
            comment=comment,
        )
    ok("9 reviews created")


if __name__ == "__main__":
    create_tables()
    seed_data()
    print(f"\n{BOLD}{GREEN}Database ready at example/library.db{RESET}\n")
