# This files contains your custom actions which can be used to run
# custom Python code.
#
# See this guide on how to implement these action:
# https://rasa.com/docs/rasa/custom-actions


# This is a simple example for a custom action which utters "Hello World!"

from typing import Any, Text, Dict, List

from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from rasa_sdk.events import SlotSet
from graph_database import KnowledgeGraph
import logging

logger = logging.getLogger(__name__)


def get_entity_type(tracker: Tracker) -> Text:
    """
    Get the entity type mentioned by the user. As the user may speak of an
    entity type in plural, we need to map the mentioned entity type to the
    type used in the knowledge base.
    :param tracker: tracker
    :return: entity type (same type as used in the knowledge base)
    """
    graph_database = KnowledgeGraph("bolt://localhost:7687", "neo4j", "jimin")
    entity_type = tracker.get_slot("entity_type")
    return graph_database.map("entity-type-mapping", entity_type)


class ActionQueryAttribute(Action):
    def name(self) -> Text:
        return "action_query_attribute"

    def run(
        self,
        dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any],
    ) -> List[Dict[Text, Any]]:

        # name = get_entity_type(tracker)

        q = KnowledgeGraph("bolt://localhost:7687", "neo4j", "jimin")
        # print(tracker.latest_message)
        entity_type = tracker.latest_message["entities"][0]["entity"]
        name = q.map("entity-type-mapping", entity_type)
        value = q.get_entities("n4sch__Class", {"n4sch__name": name})
        print(value)
        if value is not None and len(value) == 1:
            dispatcher.utter_message(f"'{value[0]['n4sch__comment']}'.")
        else:
            dispatcher.utter_message(
                f"Did not found a valid value for attribute for entity '{entity_type}'."
            )

        return []
