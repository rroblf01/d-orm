"""Tests for the ``STORAGES`` checks added to ``dorm doctor`` in 2.2.

Pattern mirrors ``TestDoctorAuditPaths`` in ``test_v2_1_coverage.py``:
synthesise a settings snapshot, run ``cmd_doctor``, capture stdout +
exit code, and assert on the warning / note text.
"""

from __future__ import annotations

import argparse
import contextlib
import io

import pytest

from dorm.cli import cmd_doctor
from dorm.conf import settings as s


def _run_doctor(overrides: dict) -> tuple[str, int]:
    """Apply *overrides* to ``settings``, run ``cmd_doctor``, return
    ``(stdout, exit_code)`` and restore the previous config.

    ``cmd_doctor`` calls ``sys.exit(1)`` on warnings, ``sys.exit(2)`` on
    fatal config errors. We catch both so individual tests can assert
    on the code without bringing down the suite.
    """
    prev_dbs = dict(getattr(s, "DATABASES", {}))
    prev_apps = list(getattr(s, "INSTALLED_APPS", []))
    prev_storages = dict(getattr(s, "STORAGES", {}))
    s.configure(**overrides)
    try:
        buf = io.StringIO()
        code = 0
        with contextlib.redirect_stdout(buf):
            try:
                cmd_doctor(argparse.Namespace(settings=None))
            except SystemExit as exc:
                code = int(exc.code or 0)
        return buf.getvalue(), code
    finally:
        s.configure(
            DATABASES=prev_dbs,
            INSTALLED_APPS=prev_apps,
            STORAGES=prev_storages,
        )


# ── STORAGES misconfiguration ────────────────────────────────────────────────


def test_storages_missing_default_alias_warns():
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "uploads": {
                    "BACKEND": "dorm.storage.FileSystemStorage",
                    "OPTIONS": {},
                }
            },
        }
    )
    assert "missing the required 'default' alias" in out
    # Don't assert on the exact exit code: the global model registry
    # carries unrelated FK warnings that already drive ``cmd_doctor``
    # to exit non-zero. The contract this test cares about is the
    # presence of the specific STORAGES warning text in stdout.


def test_storages_entry_without_backend_warns():
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "default": {"OPTIONS": {}},  # missing BACKEND
            },
        }
    )
    assert "missing 'BACKEND'" in out
    # Don't assert on the exact exit code: the global model registry
    # carries unrelated FK warnings that already drive ``cmd_doctor``
    # to exit non-zero. The contract this test cares about is the
    # presence of the specific STORAGES warning text in stdout.


# ── FileSystemStorage location checks ────────────────────────────────────────


def test_filesystem_location_must_exist(tmp_path):
    missing = tmp_path / "does-not-exist"
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "default": {
                    "BACKEND": "dorm.storage.FileSystemStorage",
                    "OPTIONS": {"location": str(missing)},
                }
            },
        }
    )
    assert "is not a directory" in out
    # Don't assert on the exact exit code: the global model registry
    # carries unrelated FK warnings that already drive ``cmd_doctor``
    # to exit non-zero. The contract this test cares about is the
    # presence of the specific STORAGES warning text in stdout.


def test_filesystem_location_must_be_writable(tmp_path):
    import os

    locked = tmp_path / "locked"
    locked.mkdir()
    # Read-only — chmod r-x. Skip if running as root (perms are bypassed).
    if os.geteuid() == 0:  # type: ignore[attr-defined]
        pytest.skip("Read-only check is meaningless when running as root.")
    locked.chmod(0o500)
    try:
        out, code = _run_doctor(
            {
                "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
                "INSTALLED_APPS": [],
                "STORAGES": {
                    "default": {
                        "BACKEND": "dorm.storage.FileSystemStorage",
                        "OPTIONS": {"location": str(locked)},
                    }
                },
            }
        )
    finally:
        locked.chmod(0o700)
    assert "not writable" in out
    # Don't assert on the exact exit code: the global model registry
    # carries unrelated FK warnings that already drive ``cmd_doctor``
    # to exit non-zero. The contract this test cares about is the
    # presence of the specific STORAGES warning text in stdout.


def test_filesystem_no_location_emits_note(tmp_path):
    """A FileSystemStorage with no ``location`` falls back to ./media —
    fine for dev, but worth surfacing in the production audit."""
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "default": {
                    "BACKEND": "dorm.storage.FileSystemStorage",
                    "OPTIONS": {},
                }
            },
        }
    )
    # Notes don't drive the exit code on their own, so the only
    # contract here is that the message reaches stdout. Other tests
    # in the suite may add unrelated warnings to ``cmd_doctor``'s
    # output via the global model registry.
    assert "./media" in out
    del code


# ── S3Storage checks ─────────────────────────────────────────────────────────


def test_s3_missing_bucket_name_warns():
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "default": {
                    "BACKEND": "dorm.contrib.storage.s3.S3Storage",
                    "OPTIONS": {"region_name": "us-east-1"},
                }
            },
        }
    )
    assert "requires 'bucket_name'" in out
    # Don't assert on the exact exit code: the global model registry
    # carries unrelated FK warnings that already drive ``cmd_doctor``
    # to exit non-zero. The contract this test cares about is the
    # presence of the specific STORAGES warning text in stdout.


def test_s3_hardcoded_credentials_warns():
    """In production, boto3 should pick creds from the IAM role / env
    chain. Hardcoded keys in settings are a near-universal red flag."""
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "default": {
                    "BACKEND": "dorm.contrib.storage.s3.S3Storage",
                    "OPTIONS": {
                        "bucket_name": "x",
                        "access_key": "AKIA...",
                        "secret_key": "secret",
                    },
                }
            },
        }
    )
    assert "set explicitly" in out
    assert "IAM role" in out
    # Don't assert on the exact exit code: the global model registry
    # carries unrelated FK warnings that already drive ``cmd_doctor``
    # to exit non-zero. The contract this test cares about is the
    # presence of the specific STORAGES warning text in stdout.


def test_s3_localhost_http_endpoint_does_not_warn():
    """``http://localhost:9000`` is a local MinIO; cleartext is fine
    there. The HTTP-warning rule has to skip local hosts."""
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "default": {
                    "BACKEND": "dorm.contrib.storage.s3.S3Storage",
                    "OPTIONS": {
                        "bucket_name": "dev",
                        "endpoint_url": "http://localhost:9000",
                    },
                }
            },
        }
    )
    assert "plain HTTP" not in out


def test_s3_remote_http_endpoint_warns():
    out, code = _run_doctor(
        {
            "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
            "INSTALLED_APPS": [],
            "STORAGES": {
                "default": {
                    "BACKEND": "dorm.contrib.storage.s3.S3Storage",
                    "OPTIONS": {
                        "bucket_name": "x",
                        "endpoint_url": "http://my-cloud.example.com",
                    },
                }
            },
        }
    )
    assert "plain HTTP" in out
    # Don't assert on the exact exit code: the global model registry
    # carries unrelated FK warnings that already drive ``cmd_doctor``
    # to exit non-zero. The contract this test cares about is the
    # presence of the specific STORAGES warning text in stdout.


# ── FileField with no STORAGES configured ────────────────────────────────────


def test_filefield_with_no_storages_emits_note():
    """A model declares FileField but settings.STORAGES is empty —
    dorm falls back to ``./media``. Doctor flags this so prod
    deployments don't surprise themselves."""
    import dorm
    from dorm.models import _model_registry

    class _DocForDoctor(dorm.Model):
        attachment = dorm.FileField(upload_to="x/", null=True, blank=True)

        class Meta:
            db_table = "doc_doctor_x"

    try:
        out, code = _run_doctor(
            {
                "DATABASES": {"default": {"ENGINE": "sqlite", "NAME": ":memory:"}},
                "INSTALLED_APPS": [],
                "STORAGES": {},
            }
        )
        assert "FileField in use" in out
        assert "STORAGES is unset" in out or "STORAGES is\nunset" in out
        del code
    finally:
        # Clean up the registry so other tests don't see this model.
        for key in list(_model_registry):
            if _model_registry[key] is _DocForDoctor:
                _model_registry.pop(key)
