import datetime
from unittest import TestCase
from unittest.mock import patch, create_autospec

from looker_sdk.sdk.api40.methods import Looker40SDK
from looker_sdk.sdk.api40.models import (
    Dashboard,
    Look,
    LookmlModelExplore,
    LookmlModelExploreFieldset,
    LookmlModelExploreField,
    Category,
)

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_TYPE,
    ATTRIBUTE_VALUE_TYPE_DATETIME,
    ATTRIBUTE_NAME_DATA,
    ATTRIBUTE_VALUE_TYPE_LOOKER_CATEGORY,
)
from apollo.agent.logging_utils import LoggingUtils


class LookerTests(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())
        self._mock_client = create_autospec(Looker40SDK)

    @patch("apollo.integrations.looker.looker_proxy_client.init40")
    def test_all_dashboards(self, mock_looker_init):
        mock_looker_init.return_value = self._mock_client
        dashboards = [
            Dashboard(id="1", title="A"),
            Dashboard(id="2", title="B"),
        ]
        self._mock_client.all_dashboards.return_value = dashboards
        result = self._agent.execute_operation(
            "looker",
            "all_dashboards",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "all_dashboards", "kwargs": {"fields": "id"}}],
            },
            credentials={"user": "test"},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual([{"id": dashboard.id} for dashboard in dashboards], response)

    @patch("apollo.integrations.looker.looker_proxy_client.init40")
    def test_all_looks(self, mock_looker_init):
        mock_looker_init.return_value = self._mock_client
        looks = [
            Look(id="1", title="A"),
            Look(id="2", title="B"),
        ]
        self._mock_client.all_looks.return_value = looks
        result = self._agent.execute_operation(
            "looker",
            "all_looks",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "all_looks", "kwargs": {"fields": "id"}}],
            },
            credentials={"user": "test"},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual([{"id": look.id} for look in looks], response)

    @patch("apollo.integrations.looker.looker_proxy_client.init40")
    def test_dashboard(self, mock_looker_init):
        mock_looker_init.return_value = self._mock_client
        dashboard = Dashboard(id="1", title="A", updated_at=datetime.datetime.now())
        self._mock_client.dashboard.return_value = dashboard
        result = self._agent.execute_operation(
            "looker",
            "dashboard",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [{"method": "dashboard", "args": [dashboard.id]}],
            },
            credentials={"user": "test"},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(dashboard.id, response["id"])
        self.assertEqual(dashboard.title, response["title"])
        self.assertEqual(
            {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_DATETIME,
                ATTRIBUTE_NAME_DATA: dashboard.updated_at.isoformat(),
            },
            response["updated_at"],
        )

    @patch("apollo.integrations.looker.looker_proxy_client.init40")
    def test_explore(self, mock_looker_init):
        mock_looker_init.return_value = self._mock_client
        explore = LookmlModelExplore(
            id="1",
            title="A",
            fields=LookmlModelExploreFieldset(
                dimensions=[
                    LookmlModelExploreField(
                        category=Category.dimension, name="test_dimension"
                    )
                ]
            ),
        )
        self._mock_client.lookml_model_explore.return_value = explore
        result = self._agent.execute_operation(
            "looker",
            "dashboard",
            {
                "trace_id": "1234",
                "skip_cache": True,
                "commands": [
                    {
                        "method": "lookml_model_explore",
                        "args": ["look_id", "explore_id"],
                    }
                ],
            },
            credentials={"user": "test"},
        )
        self.assertIsNone(result.result.get(ATTRIBUTE_NAME_ERROR))

        response = result.result[ATTRIBUTE_NAME_RESULT]
        self.assertEqual(explore.id, response["id"])
        self.assertEqual(explore.title, response["title"])
        self.assertEqual(
            {
                ATTRIBUTE_NAME_TYPE: ATTRIBUTE_VALUE_TYPE_LOOKER_CATEGORY,
                ATTRIBUTE_NAME_DATA: explore.fields.dimensions[0].category.name,
            },
            response["fields"]["dimensions"][0]["category"],
        )
