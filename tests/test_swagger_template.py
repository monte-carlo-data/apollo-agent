import re
from html.parser import HTMLParser
from pathlib import Path
from unittest import TestCase

TEMPLATE = Path(__file__).parent.parent / "apollo/interfaces/generic/templates/swagger.html"

# Hosts whose content we do not control: anything loaded from them must be
# version-pinned and carry a Subresource Integrity hash.
THIRD_PARTY_CDN_HOSTS = ("unpkg.com", "cdn.jsdelivr.net", "cdnjs.cloudflare.com")


class _ExternalResourceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.resources = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if tag == "script" and attributes.get("src"):
            self.resources.append((tag, attributes))
        elif tag == "link" and attributes.get("href"):
            self.resources.append((tag, attributes))


class SwaggerTemplateSriTests(TestCase):
    """The /swagger/ docs page must not run unverified third-party CDN code."""

    def _cdn_resources(self):
        parser = _ExternalResourceParser()
        parser.feed(TEMPLATE.read_text())
        return [
            (tag, attrs)
            for tag, attrs in parser.resources
            if any(
                host in (attrs.get("src") or attrs.get("href"))
                for host in THIRD_PARTY_CDN_HOSTS
            )
        ]

    def test_template_loads_swagger_ui_from_a_cdn(self):
        # guards against this test passing vacuously if the template is rewritten
        self.assertTrue(self._cdn_resources())

    def test_cdn_resources_are_version_pinned(self):
        for _tag, attrs in self._cdn_resources():
            url = attrs.get("src") or attrs.get("href")
            self.assertRegex(url, r"@\d+\.\d+\.\d+/", url)

    def test_cdn_resources_carry_sri_and_anonymous_cors(self):
        for _tag, attrs in self._cdn_resources():
            url = attrs.get("src") or attrs.get("href")
            self.assertTrue(
                re.fullmatch(r"sha384-[A-Za-z0-9+/]{64}", attrs.get("integrity", "")),
                f"{url} has no sha384 integrity attribute",
            )
            self.assertEqual("anonymous", attrs.get("crossorigin"), url)
