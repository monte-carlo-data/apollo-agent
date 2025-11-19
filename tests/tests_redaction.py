from unittest import TestCase

from apollo.agent.constants import ATTRIBUTE_VALUE_REDACTED, LOG_ATTRIBUTE_TRACE_ID
from apollo.agent.redact import AgentRedactUtilities


class RedactionTests(TestCase):
    """Unit tests for AgentRedactUtilities class"""

    def test_standard_redact_with_dict_containing_password(self):
        """Test standard_redact redacts dictionary keys containing 'password'"""
        input_data = {"password": "secret123", "username": "john"}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["password"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["username"], ATTRIBUTE_VALUE_REDACTED)

    def test_standard_redact_with_dict_containing_token(self):
        """Test standard_redact redacts dictionary keys containing 'token'"""
        input_data = {"api_token": "abc123xyz", "name": "test"}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["api_token"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["name"], "test")

    def test_standard_redact_with_dict_containing_secret(self):
        """Test standard_redact redacts dictionary keys containing 'secret'"""
        input_data = {"client_secret": "xyz789", "data": "public"}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["client_secret"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["data"], "public")

    def test_standard_redact_with_dict_containing_key(self):
        """Test standard_redact redacts dictionary keys containing 'key'"""
        input_data = {"api_key": "key123", "value": "test"}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["api_key"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["value"], "test")

    def test_standard_redact_with_dict_containing_auth(self):
        """Test standard_redact redacts dictionary keys containing 'auth'"""
        input_data = {"authorization": "Bearer token", "id": "123"}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["authorization"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["id"], "123")

    def test_standard_redact_with_dict_containing_credential(self):
        """Test standard_redact redacts dictionary keys containing 'credential'"""
        input_data = {
            "credentials": "user:pass",
            "status": "active",
            LOG_ATTRIBUTE_TRACE_ID: "abcdefghij1234567890",
            LOG_ATTRIBUTE_TRACE_ID + "_test": "abcdefghij1234567890",
        }
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["credentials"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["status"], "active")
        # trace id is not redacted, even when matching the token value pattern
        self.assertEqual(result[LOG_ATTRIBUTE_TRACE_ID], "abcdefghij1234567890")
        # attributes containing trace_id are redacted
        self.assertEqual(
            result[LOG_ATTRIBUTE_TRACE_ID + "_test"], ATTRIBUTE_VALUE_REDACTED
        )

    def test_standard_redact_with_dict_containing_user(self):
        """Test standard_redact redacts dictionary keys containing 'user'"""
        input_data = {"username": "admin", "count": 5}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["username"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["count"], 5)

    def test_standard_redact_with_dict_containing_client(self):
        """Test standard_redact redacts dictionary keys containing 'client'"""
        input_data = {"client_id": "12345", "server": "localhost"}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["client_id"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["server"], "localhost")

    def test_standard_redact_case_insensitive(self):
        """Test standard_redact is case-insensitive for attribute names"""
        input_data = {"PASSWORD": "secret", "Token": "abc", "API_KEY": "xyz"}
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["PASSWORD"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["Token"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["API_KEY"], ATTRIBUTE_VALUE_REDACTED)

    def test_standard_redact_with_nested_dict(self):
        """Test standard_redact handles nested dictionaries"""
        input_data = {
            "config": {"api_key": "secret123", "endpoint": "https://api.example.com"},
            "name": "test",
        }
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["config"]["api_key"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["config"]["endpoint"], "https://api.example.com")
        self.assertEqual(result["name"], "test")

    def test_standard_redact_with_list(self):
        """Test standard_redact handles lists"""
        input_data = [
            {"password": "secret1", "id": 1},
            {"password": "secret2", "id": 2},
        ]
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result[0]["password"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[1]["password"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result[1]["id"], 2)

    def test_standard_redact_with_mixed_nested_structures(self):
        """Test standard_redact handles complex nested structures"""
        input_data = {
            "users": [
                {"username": "alice", "email": "alice@example.com"},
                {"username": "bob", "email": "bob@example.com"},
            ],
            "config": {
                "api_token": "xyz123",
                "settings": {"client_secret": "abc789", "timeout": 30},
            },
        }
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["users"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["config"]["api_token"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(
            result["config"]["settings"]["client_secret"], ATTRIBUTE_VALUE_REDACTED
        )
        self.assertEqual(result["config"]["settings"]["timeout"], 30)

    def test_standard_redact_with_non_dict_non_list(self):
        """Test standard_redact handles non-dict, non-list values"""
        self.assertEqual(AgentRedactUtilities.standard_redact(123), 123)
        self.assertEqual(AgentRedactUtilities.standard_redact(45.67), 45.67)
        self.assertEqual(AgentRedactUtilities.standard_redact(True), True)
        self.assertEqual(AgentRedactUtilities.standard_redact(None), None)

    def test_standard_redact_with_string_value(self):
        """Test standard_redact handles string values"""
        # String values go through _redact_string method
        result = AgentRedactUtilities.standard_redact("some string")
        # Based on current implementation, strings are returned as-is
        # unless they match certain patterns
        self.assertEqual(result, "some string")

    def test_redact_attributes_with_custom_attributes(self):
        """Test redact_attributes with custom attribute list"""
        input_data = {
            "custom_field": "value1",
            "normal_field": "value2",
            "another_custom": "value3",
        }
        custom_attrs = ["custom"]
        result = AgentRedactUtilities.redact_attributes(input_data, custom_attrs)
        self.assertEqual(result["custom_field"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["normal_field"], "value2")
        self.assertEqual(result["another_custom"], ATTRIBUTE_VALUE_REDACTED)

    def test_redact_attributes_with_empty_attribute_list(self):
        """Test redact_attributes with empty attribute list"""
        input_data = {"password": "my secret value", "token": "abc"}
        result = AgentRedactUtilities.redact_attributes(input_data, [])
        # value containing "secret" should be redacted
        self.assertEqual(result["password"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["token"], "abc")

    def test_redact_attributes_with_empty_dict(self):
        """Test redact_attributes with empty dictionary"""
        result = AgentRedactUtilities.redact_attributes({}, ["password"])
        self.assertEqual(result, {})

    def test_redact_attributes_with_empty_list(self):
        """Test redact_attributes with empty list"""
        result = AgentRedactUtilities.redact_attributes([], ["password"])
        self.assertEqual(result, [])

    def test_is_redacted_attribute_matches_substring(self):
        """Test _is_redacted_attribute matches substrings"""
        self.assertTrue(
            AgentRedactUtilities._is_attribute_included("my_password", ["pass"])
        )
        self.assertTrue(
            AgentRedactUtilities._is_attribute_included("api_token_value", ["token"])
        )
        self.assertTrue(
            AgentRedactUtilities._is_attribute_included("SECRET_KEY", ["secret"])
        )

    def test_is_redacted_attribute_case_insensitive(self):
        """Test _is_redacted_attribute is case-insensitive"""
        self.assertTrue(
            AgentRedactUtilities._is_attribute_included("PASSWORD", ["pass"])
        )
        self.assertTrue(AgentRedactUtilities._is_attribute_included("Token", ["token"]))
        self.assertTrue(AgentRedactUtilities._is_attribute_included("API_KEY", ["key"]))

    def test_is_redacted_attribute_no_match(self):
        """Test _is_redacted_attribute returns False when no match"""
        self.assertFalse(
            AgentRedactUtilities._is_attribute_included("username", ["pass"])
        )
        self.assertFalse(
            AgentRedactUtilities._is_attribute_included("data", ["token", "key"])
        )

    def test_is_redacted_attribute_multiple_attributes(self):
        """Test _is_redacted_attribute with multiple attributes"""
        attributes = ["pass", "token", "key"]
        self.assertTrue(
            AgentRedactUtilities._is_attribute_included("password", attributes)
        )
        self.assertTrue(
            AgentRedactUtilities._is_attribute_included("api_token", attributes)
        )
        self.assertTrue(
            AgentRedactUtilities._is_attribute_included("secret_key", attributes)
        )
        self.assertFalse(
            AgentRedactUtilities._is_attribute_included("username", attributes)
        )

    def test_redact_string_with_normal_string(self):
        """Test _redact_string with normal strings"""
        result = AgentRedactUtilities._redact_string("hello world")
        self.assertEqual(result, "hello world")

    def test_redact_string_with_short_string(self):
        """Test _redact_string with short strings"""
        result = AgentRedactUtilities._redact_string("abc")
        self.assertEqual(result, "abc")

    def test_redact_string_with_long_alphanumeric_token(self):
        """Test _redact_string with long alphanumeric strings (20-64 chars) like tokens"""
        # 20 character token - should be redacted
        token_20 = "abcdefghij1234567890"
        result = AgentRedactUtilities._redact_string(token_20)
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

        # 40 character token - should be redacted
        token_40 = "abcdefghijklmnopqrstuvwxyz12345678901234"
        result = AgentRedactUtilities._redact_string(token_40)
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

        # 64 character token - should be redacted
        token_64 = "abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ12"
        result = AgentRedactUtilities._redact_string(token_64)
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_token_too_short(self):
        """Test _redact_string with alphanumeric string shorter than 20 chars"""
        # 19 character token - should NOT be redacted
        token_19 = "abcdefghij123456789"
        result = AgentRedactUtilities._redact_string(token_19)
        self.assertEqual(result, token_19)

    def test_redact_string_with_token_too_long(self):
        """Test _redact_string with alphanumeric string longer than 64 chars"""
        # 70 character token - includes a 64 chars token, should be redacted
        token_65 = (
            "abcdefghijk-lmnopqrstuvwxyz1-234567890ABC-DEFGHIJKLMNOP-QRSTUVWXYZ-123"
        )
        result = AgentRedactUtilities._redact_string(token_65)
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_oauth_token_request_body(self):
        """Test _redact_string with OAuth token request body containing sensitive data"""
        oauth_body = "grant_type=client_credentials&client_id=abc123&client_secret=xyz789&scope=read"
        result = AgentRedactUtilities._redact_string(oauth_body)
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)
        oauth_body_new_lines = "\ngrant_type=client_credentials\n&client_id=abc123&client_secret=xyz789&scope=read\n"
        result = AgentRedactUtilities._redact_string(oauth_body_new_lines)
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_json_containing_password(self):
        """Test _redact_string with JSON string containing password field"""
        json_str = '{"username": "admin", "password": "secret123"}'
        result = AgentRedactUtilities._redact_string(json_str)
        # Should match pattern .*password.*"
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_json_containing_token(self):
        """Test _redact_string with JSON string containing token field"""
        json_str = '{"access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"}'
        result = AgentRedactUtilities._redact_string(json_str)
        # Should match pattern .*token.*"
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_json_containing_secret(self):
        """Test _redact_string with JSON string containing secret field"""
        json_str = '{"api_secret": "super_secret_value_123"}'
        result = AgentRedactUtilities._redact_string(json_str)
        # Should match pattern .*secret.*"
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_json_containing_key(self):
        """Test _redact_string with JSON string containing key field"""
        json_str = '{"api_key": "1234567890abcdef"}'
        result = AgentRedactUtilities._redact_string(json_str)
        # Should match pattern .*key.*"
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_json_containing_auth(self):
        """Test _redact_string with JSON string containing auth field"""
        json_str = '{"authorization": "Bearer token123"}'
        result = AgentRedactUtilities._redact_string(json_str)
        # Should match pattern .*auth.*"
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_string_with_json_containing_credential(self):
        """Test _redact_string with JSON string containing credential field"""
        json_str = '{"credentials": "user:password"}'
        result = AgentRedactUtilities._redact_string(json_str)
        # Should match pattern .*credential.*"
        self.assertEqual(result, ATTRIBUTE_VALUE_REDACTED)

    def test_redact_attributes_preserves_structure(self):
        """Test that redact_attributes preserves the original structure"""
        input_data = {"level1": {"level2": {"password": "secret", "data": "value"}}}
        result = AgentRedactUtilities.redact_attributes(input_data, ["pass"])
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result["level1"], dict)
        self.assertIsInstance(result["level1"]["level2"], dict)
        self.assertEqual(
            result["level1"]["level2"]["password"], ATTRIBUTE_VALUE_REDACTED
        )
        self.assertEqual(result["level1"]["level2"]["data"], "value")

    def test_redact_attributes_with_list_of_primitives(self):
        """Test redact_attributes with list of primitive values"""
        input_data = [1, 2, 3, "test", True, None]
        result = AgentRedactUtilities.redact_attributes(input_data, ["pass"])
        self.assertEqual(result, [1, 2, 3, "test", True, None])

    def test_redact_tuple(self):
        """Test redact_attributes with tuple"""
        input_data = (1, 2, 3, "test", True, None)
        result = AgentRedactUtilities.redact_attributes(input_data, ["pass"])
        self.assertEqual(result, (1, 2, 3, "test", True, None))

    def test_redact_attributes_with_mixed_list(self):
        """Test redact_attributes with mixed list of dicts and primitives"""
        input_data = [
            {"password": "secret1"},
            "plain string",
            123,
            {"token": "abc", "id": 1},
        ]
        result = AgentRedactUtilities.redact_attributes(input_data, ["pass", "token"])
        self.assertEqual(result[0]["password"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result[1], "plain string")
        self.assertEqual(result[2], 123)
        self.assertEqual(result[3]["token"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result[3]["id"], 1)

    def test_redact_attributes_with_mixed_tuple(self):
        """Test redact_attributes with mixed tuple of dicts and primitives"""
        input_data = (
            {"password": "secret1"},
            "plain string",
            123,
            {"token": "abc", "id": 1},
        )
        result = AgentRedactUtilities.redact_attributes(input_data, ["pass", "token"])
        self.assertIsInstance(result, tuple)
        self.assertEqual(result[0]["password"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result[1], "plain string")
        self.assertEqual(result[2], 123)
        self.assertEqual(result[3]["token"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result[3]["id"], 1)

    def test_standard_redact_all_standard_attributes(self):
        """Test that all standard attributes are redacted"""
        input_data = {
            "password": "val1",
            "secret": "val2",
            "client": "val3",
            "token": "val4",
            "user": "val5",
            "auth": "val6",
            "credential": "val7",
            "key": "val8",
            "safe_field": "val9",
        }
        result = AgentRedactUtilities.standard_redact(input_data)
        self.assertEqual(result["password"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["secret"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["client"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["token"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["user"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["auth"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["credential"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["key"], ATTRIBUTE_VALUE_REDACTED)
        self.assertEqual(result["safe_field"], "val9")
