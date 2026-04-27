"""Optional storage backends.

The default :class:`dorm.storage.FileSystemStorage` lives in core and
needs no extras. Backends in this package are gated behind optional
dependencies — install them via the matching pip extra:

- :class:`s3.S3Storage` — ``pip install 'djanorm[s3]'`` (boto3).

Each module imports its underlying SDK lazily so importing
``dorm.contrib.storage`` itself doesn't require any of them.
"""
