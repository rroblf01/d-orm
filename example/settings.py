import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": os.path.join(BASE_DIR, "library.db"),
    }
}

INSTALLED_APPS = ["example", "example.sales", "dorm.contrib.auth"]

SECRET_KEY = "example-only-secret-not-for-production-use-please"
