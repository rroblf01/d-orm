"""
Asynchronous operations demo for d-orm.
Run after setup_db.py.

    uv run python example/demo_async.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dorm import Q, Count, Avg, Max
from example.models import Author, Book, Genre, Review

# ── Output helpers ─────────────────────────────────────────────────────────────

RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"


def section(title: str):
    print(f"\n{BOLD}{MAGENTA}━━━  {title}  ━━━{RESET}")


def show(label: str, value):
    print(f"  {YELLOW}{label:<38}{RESET} {value}")


def item(text: str):
    print(f"  {GREEN}•{RESET}  {text}")


# ══════════════════════════════════════════════════════════════════════════════


async def main():
    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║    d-orm  ·  Async Demo              ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════╝{RESET}")

    # ── 1. acreate / aget ─────────────────────────────────────────────────────

    section("1. acreate · aget")

    new_author = await Author.objects.acreate(
        name="Philip K. Dick",
        email="pkdick@example.com",
        birth_year=1928,
        nationality="American",
        bio="Master of speculative science fiction.",
    )
    show("Author created (acreate):", f"{new_author.name}  (pk={new_author.pk})")

    found = await Author.objects.aget(email="pkdick@example.com")
    show("Author retrieved (aget):", found.name)

    try:
        await Author.objects.aget(name="Nobody")
    except Author.DoesNotExist:
        show("DoesNotExist caught:", True)

    # ── 2. Async iteration ────────────────────────────────────────────────────

    section("2. Async iteration (async for)")

    print("\n  Active authors in alphabetical order:")
    async for author in Author.objects.exclude(active=False).order_by("name"):
        item(f"{author.name}  ({author.nationality or '—'})")

    # ── 3. acount · aexists ───────────────────────────────────────────────────

    section("3. acount · aexists")

    show("Total books:", await Book.objects.acount())
    show("Books with stock > 5:", await Book.objects.filter(stock__gt=5).acount())
    show("Does 'Cosmos' exist?", await Book.objects.filter(title="Cosmos").aexists())
    show("Does 'Dune' exist?", await Book.objects.filter(title="Dune").aexists())

    # ── 4. afirst · alast ─────────────────────────────────────────────────────

    section("4. afirst · alast")

    oldest = await Book.objects.order_by("published_year").afirst()
    newest = await Book.objects.order_by("-published_year").afirst()
    cheapest = await Book.objects.order_by("price").afirst()
    empty = await Book.objects.filter(title="Unknown").afirst()

    assert oldest is not None and newest is not None and cheapest is not None
    show("Oldest book:", f"{oldest.title}  ({oldest.published_year})")
    show("Author of oldest book:", oldest.author.name)
    show("Newest book:", f"{newest.title}  ({newest.published_year})")
    show("Cheapest book:", f"{cheapest.title}  (${cheapest.price})")
    show("afirst() on empty queryset:", empty)

    # ── 5. aupdate ────────────────────────────────────────────────────────────

    section("5. aupdate")

    n = await Book.objects.filter(stock=0).aupdate(available=False)
    show("Books without stock marked unavailable:", n)

    scifi = await Genre.objects.aget(name="Science Fiction")
    await Book.objects.acreate(
        title="Do Androids Dream of Electric Sheep?",
        author_id=new_author.pk,
        genre_id=scifi.pk,
        isbn="9780345404473",
        pages=244,
        price=13.50,
        published_year=1968,
        stock=7,
    )
    pkd_book = await Book.objects.aget(isbn="9780345404473")
    show("PKD book created:", pkd_book.title)

    # ── 6. aaggregate ─────────────────────────────────────────────────────────

    section("6. aaggregate")

    stats = await Book.objects.aaggregate(
        total=Count("id"),
        avg_price=Avg("price"),
        max_price=Max("price"),
        max_pages=Max("pages"),
    )
    show("Total books:", stats["total"])
    show("Average price:", f"${stats['avg_price']:.2f}")
    show("Highest price:", f"${stats['max_price']}")
    show("Most pages:", stats["max_pages"])

    review_stats = await Review.objects.aaggregate(
        total=Count("id"),
        avg_rating=Avg("rating"),
    )
    show("\n  Total reviews:", review_stats["total"])
    show("  Average rating:", f"{review_stats['avg_rating']:.2f} / 5")

    # ── 7. Q objects async ────────────────────────────────────────────────────

    section("7. Q objects in async")

    print("\n  Science Fiction OR very cheap books (< $9):")
    async for book in Book.objects.filter(
        Q(genre_id=scifi.pk) | Q(price__lt=9)
    ).order_by("price"):
        item(f"{book.title}  →  ${book.price}")

    # ── 8. aget_or_create · aupdate_or_create ────────────────────────────────

    section("8. aget_or_create · aupdate_or_create")

    dystopia, created = await Genre.objects.aget_or_create(
        name="Dystopia",
        defaults={"description": "Oppressive futuristic societies"},
    )
    show("Genre 'Dystopia' created:", created)

    _, created2 = await Genre.objects.aget_or_create(
        name="Dystopia",
        defaults={"description": "Should not be created again"},
    )
    show("Second call creates again:", created2)

    book, updated = await Book.objects.aupdate_or_create(
        isbn="9780345404473",
        defaults={"stock": 10, "price": 12.99},
    )
    show("PKD book stock updated to 10:", book.stock)

    # ── 9. asave · adelete · arefresh_from_db ────────────────────────────────

    section("9. asave · adelete · arefresh_from_db")

    pkd_book.price = 14.99
    await pkd_book.asave()

    await pkd_book.arefresh_from_db()
    show("Price after asave + arefresh:", f"${pkd_book.price}")

    await pkd_book.adelete()
    show(
        "PKD book deleted (adelete):",
        not await Book.objects.filter(isbn="9780345404473").aexists(),
    )

    # ── 10. abulk_create · ain_bulk ──────────────────────────────────────────

    section("10. abulk_create · ain_bulk")

    temp_authors = [
        Author(
            name=f"Temp Author {i}", email=f"temp{i}@example.com", birth_year=1980 + i
        )
        for i in range(4)
    ]
    await Author.objects.abulk_create(temp_authors)
    show("Temp authors created (abulk_create):", 4)

    all_temp = list(Author.objects.filter(name__startswith="Temp"))
    ids = [a.pk for a in all_temp]
    bulk_result = await Author.objects.ain_bulk(ids)
    show("ain_bulk returns:", f"{len(bulk_result)} records")

    deleted, _ = await Author.objects.filter(name__startswith="Temp").adelete()
    show("Temp authors deleted:", deleted)

    # ── 11. Cleanup ───────────────────────────────────────────────────────────

    section("11. Cleanup")

    await new_author.adelete()
    show("Author Philip K. Dick deleted:", True)

    print(f"\n{BOLD}{GREEN}✅  Async demo complete{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
