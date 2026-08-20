from urllib.parse import urlencode

from django.conf import settings

JSON_HEADERS = {"Accept": "application/json", "user-agent": settings.USER_AGENT}


def build_url(base_url, params=None):
    if not params:
        return base_url
    return f"{base_url}?{urlencode(params, doseq=True)}"
