"""Self-hosted credentials schema framework.

Each integration declares a raw cerberus schema dict next to the code that
consumes the credentials — either on its CtpConfig (for CTP-enrolled
connectors) or as a class attribute on the proxy client (for non-CTP).
The validator and registry then just route a dict through cerberus; there
is no wrapper type because cerberus alone can express everything we need
(including ``oneof_schema`` for multi-auth-mode connectors).

The validator is invoked by ``POST /api/v1/self-hosted-credentials/validate/<connection_type>``
after the secret has been fetched from the customer's secret store.
Fetch-time errors (permission denied, secret not found, JSON parse failure)
are surfaced by the existing CredentialsFactory path; this module's
contract starts once the decoded credentials dict is in hand.
"""

from apollo.credentials.schema.registry import get_credentials_schema
from apollo.credentials.schema.validator import validate

__all__ = ["get_credentials_schema", "validate"]
