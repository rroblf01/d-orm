"""
Synchronous operations demo for djanorm.
Run after setup_db.py.

    uv run python example/demo_sync.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dorm import Q, F, Count, Sum, Avg, Max, Min
from example.models import Author, Book, Genre, Review

# ── Output helpers ─────────────────────────────────────────────────────────────

RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
BLUE = "\033[34m"


def section(title: str):
    print(f"\n{BOLD}{BLUE}━━━  {title}  ━━━{RESET}")


def show(label: str, value):
    print(f"  {YELLOW}{label:<35}{RESET} {value}")


def item(text: str):
    print(f"  {GREEN}•{RESET}  {text}")


# ══════════════════════════════════════════════════════════════════════════════

print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════╗{RESET}")
print(f"{BOLD}{CYAN}║    djanorm  ·  Sync Demo               ║{RESET}")
print(f"{BOLD}{CYAN}╚══════════════════════════════════════╝{RESET}")


# ── 1. Basic queries ───────────────────────────────────────────────────────────

section("1. Basic queries")

show("Total books:", Book.objects.count())
show("Total authors:", Author.objects.count())
show("Total reviews:", Review.objects.count())


# ── 2. filter / exclude / order_by ────────────────────────────────────────────

section("2. filter · exclude · order_by")

classics = Book.objects.filter(published_year__lt=1960).order_by("published_year")
print("\n  Classics (before 1960):")
for b in classics:
    item(f"{b.title}  ({b.published_year})")

active_authors = Author.objects.exclude(active=False).order_by("name")
print("\n  Active authors:")
for a in active_authors:
    item(f"{a.name}  —  {a.nationality}")

by_price = Book.objects.order_by("-price")[:3]
print("\n  Top 3 most expensive books:")
for b in by_price:
    item(f"{b.title}  →  ${b.price}")


# ── 3. Lookups ─────────────────────────────────────────────────────────────────

section("3. Lookups")

print("\n  Titles containing 'the' (icontains):")
for b in Book.objects.filter(title__icontains="the"):
    item(b.title)

print("\n  Authors filtered with __in:")
for a in Author.objects.filter(name__in=["Isaac Asimov", "Carl Sagan"]):
    item(a.name)

print("\n  Books published between 1940 and 1960 (__range):")
for b in Book.objects.filter(published_year__range=(1940, 1960)).order_by(
    "published_year"
):
    item(f"{b.title}  ({b.published_year})")

show("\n  Authors with bio:", Author.objects.filter(bio__isnull=False).count())


# ── 4. Q objects ───────────────────────────────────────────────────────────────

section("4. Q objects (complex queries)")

scifi_genre = Genre.objects.get(name="Science Fiction")
result = Book.objects.filter(
    Q(genre_id=scifi_genre.pk) | Q(published_year__lt=1945)
).order_by("published_year")
print("\n  Science Fiction OR published before 1945:")
for b in result:
    item(f"{b.title}  ({b.published_year})")

orwell = Author.objects.get(name="George Orwell")
not_orwell = Book.objects.filter(~Q(author_id=orwell.pk)).order_by("title")
print(f"\n  Books NOT by Orwell ({not_orwell.count()} total):")
for b in not_orwell:
    item(b.title)

result = Author.objects.filter(Q(active=True) & Q(birth_year__lte=1920))
print("\n  Active authors born in 1920 or earlier:")
for a in result:
    item(f"{a.name}  ({a.birth_year})")


# ── 5. Aggregations ────────────────────────────────────────────────────────────

section("5. Aggregations")

stats = Book.objects.aggregate(
    total=Count("id"),
    avg_price=Avg("price"),
    max_price=Max("price"),
    min_price=Min("price"),
    total_pages=Sum("pages"),
)
show("Number of books:", stats["total"])
show("Average price:", f"${stats['avg_price']:.2f}")
show("Highest price:", f"${stats['max_price']}")
show("Lowest price:", f"${stats['min_price']}")
show("Total pages in catalog:", stats["total_pages"])

in_stock = Book.objects.filter(stock__gt=0).aggregate(
    total=Count("id"),
    avg_stock=Avg("stock"),
)
show("\n  Books with available stock:", in_stock["total"])
show("  Average stock:", f"{in_stock['avg_stock']:.1f}")

review_stats = Review.objects.aggregate(
    total=Count("id"),
    avg_rating=Avg("rating"),
    top_rating=Max("rating"),
)
show("\n  Total reviews:", review_stats["total"])
show("  Average rating:", f"{review_stats['avg_rating']:.2f} / 5")
show("  Highest rating:", review_stats["top_rating"])


# ── 6. values / values_list ────────────────────────────────────────────────────

section("6. values · values_list")

print("\n  Authors (name + email) via values():")
for row in Author.objects.values("name", "email").order_by("name"):
    item(f"{row['name']}  <{row['email']}>")

titles = list(Book.objects.values_list("title", flat=True).order_by("title"))
print("\n  All titles via values_list(flat=True):")
for t in titles:
    item(t)


# ── 7. get / first / last / exists ────────────────────────────────────────────

section("7. get · first · last · exists")

try:
    book = Book.objects.get(isbn="9780451524935")
    show("get(isbn='1984'):", book.title)
except Book.DoesNotExist:
    show("get():", "not found")

cheapest = Book.objects.order_by("price").first()
assert cheapest is not None
show("Cheapest book:", f"{cheapest.title}  (${cheapest.price})")

newest = Book.objects.order_by("-published_year").first()
assert newest is not None
show("Most recent book:", f"{newest.title}  ({newest.published_year})")

show("Does 'Cosmos' exist?", Book.objects.filter(title="Cosmos").exists())
show("Does 'Harry Potter' exist?", Book.objects.filter(title="Harry Potter").exists())


# ── 8. F expressions ──────────────────────────────────────────────────────────

section("8. F expressions (column-level operations)")

before = Book.objects.filter(title="Nineteen Eighty-Four").values_list(
    "stock", flat=True
)[0]
show("Stock of '1984' before:", before)

Book.objects.filter(title="Nineteen Eighty-Four").update(stock=F("stock") + 5)
after = Book.objects.filter(title="Nineteen Eighty-Four").values_list(
    "stock", flat=True
)[0]
show("Stock of '1984' after (+5):", after)

long_books = Book.objects.filter(pages__gt=300)
show("Books with more than 300 pages:", long_books.count())


# ── 9. create / update / delete / save ────────────────────────────────────────

section("9. create · save · update · delete")

new_author = Author.objects.create(
    name="J.R.R. Tolkien",
    email="tolkien@example.com",
    birth_year=1892,
    nationality="British",
)
show("Author created:", f"{new_author.name}  (pk={new_author.pk})")

new_author.bio = "Creator of Middle-earth and the legendarium."
new_author.save()
new_author.refresh_from_db()
show("Updated bio (refresh_from_db):", new_author.bio[:42] + "…")

n = Author.objects.filter(nationality="British").update(nationality="British")
show("Authors updated (no-op update):", n)

fantasy, created = Genre.objects.get_or_create(
    name="Fantasy",
    defaults={"description": "Magical and imaginary worlds"},
)
show("Genre 'Fantasy' created:", created)

lotr, created = Book.objects.update_or_create(
    isbn="9780618640157",
    defaults={
        "title": "The Lord of the Rings",
        "author_id": new_author.pk,
        "genre_id": fantasy.pk,
        "pages": 1178,
        "price": 24.99,
        "published_year": 1954,
        "stock": 6,
    },
)
show("'The Lord of the Rings' created:", created)

companions = [
    Author(name=f"Demo Author {i}", email=f"demo{i}@example.com", birth_year=1970 + i)
    for i in range(3)
]
Author.objects.bulk_create(companions)
show("Demo authors created (bulk_create):", 3)

deleted, _ = Author.objects.filter(name__startswith="Demo Author").delete()
show("Demo authors deleted:", deleted)

lotr.delete()
new_author.delete()
show("Tolkien and his book deleted:", True)


# ── 10. Slicing ────────────────────────────────────────────────────────────────

section("10. Slicing (pagination)")

page_size = 3
page_1 = list(Book.objects.order_by("title")[:page_size])
page_2 = list(Book.objects.order_by("title")[page_size : page_size * 2])

print(f"\n  Page 1 (first {page_size}):")
for b in page_1:
    item(b.title)

print(f"\n  Page 2 (next {page_size}):")
for b in page_2:
    item(b.title)


# ── Done ────────────────────────────────────────────────────────────────────────

print(f"\n{BOLD}{GREEN}✅  Sync demo complete{RESET}\n")
