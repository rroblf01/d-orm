# djanorm

A Django-inspired ORM for Python with full **synchronous and asynchronous** support. The same API you know from Django, without depending on the full framework.

Works with **SQLite** and **PostgreSQL**, and ships with a migration system, atomic transactions, signals, validation, relationship loading (`select_related` / `prefetch_related`), aggregations, DB functions and much more — all with real static typing (`Field[T]`).

## Installation

```bash
# SQLite
pip install "djanorm[sqlite]"

# PostgreSQL
pip install "djanorm[postgresql]"
```

## Documentation

The full documentation, tutorials and API reference are published at:

**https://rroblf01.github.io/d-orm/**

You will find the getting-started guide, complete examples, the API reference and production deployment notes there.

## Contributing

Everyone is welcome to get involved! If you want to suggest changes, propose improvements or discuss the direction of the project, open an issue or a pull request on this repository. Discussions, ideas and critiques are very welcome.

## License

See [LICENSE](LICENSE).
