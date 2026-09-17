from unittest import TestCase
from unittest.mock import patch

from apollo.integrations.azure_blob.utils import AzureUtils


class GetDefaultCredentialTests(TestCase):
    @patch("apollo.integrations.azure_blob.utils.DefaultAzureCredential")
    def test_no_credentials_are_excluded_from_the_chain(self, mock_credential_type):
        # Asserting no arguments at all, so excluding any credential from the chain fails here.
        credential = AzureUtils.get_default_credential()

        mock_credential_type.assert_called_once_with()
        self.assertEqual(mock_credential_type.return_value, credential)
