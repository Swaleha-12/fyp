import logging
from typing import List, Dict, Any, Optional, Text

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class KnowledgeBase(object):
    def get_entities(
        self,
        entity_type: Text,
        attributes: Optional[List[Dict[Text, Text]]] = None,
        limit: int = 5,
    ) -> List[Dict[Text, Any]]:

        raise NotImplementedError("Method is not implemented.")

    def get_attribute_of(
        self, entity_type: Text, key_attribute: Text, entity: Text, attribute: Text
    ) -> List[Any]:

        raise NotImplementedError("Method is not implemented.")

    def validate_entity(
        self, entity_type, entity, key_attribute, attributes
    ) -> Optional[Dict[Text, Any]]:

        raise NotImplementedError("Method is not implemented.")

    def map(self, mapping_type: Text, mapping_key: Text) -> Text:

        raise NotImplementedError("Method is not implemented.")


class KnowledgeGraph(KnowledgeBase):
    """
    GraphDatabase uses a grakn graph database to encode your domain knowledege. Make
    sure to have the graph database set up and the grakn server running.
    """

    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="jimin"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.uri = uri
        self.user = user
        self.password = password
        self.attribute_mapping = {
            "what is": "n4sch__comment",
            "What is": "n4sch__comment",
        }
        self.entity_type_mapping = {"business model": "BusinessModel"}

    def close(self):
        self.driver.close()

    def _thing_to_dict(self, thing):
        """
        Converts a thing (a grakn object) to a dict for easy retrieval of the thing's
        attributes.
        """
        """
        entity = {"id": thing.id, "type": thing.type().label()}
        for each in thing.attributes():
            entity[each.type().label()] = each.value()
        return entity
        """
        type_ = ""
        ##        if str(type(thing)) == "<class 'abc.n4sch__DOMAIN'>":
        ##            type_ = "Domain"
        ##        elif str(type(thing)) == "<class 'abc.n4sch__RANGE'>":
        ##            type_ = "Range"
        if "n4sch__Class" in thing.labels:
            type_ = "Class"
        elif "n4sch__Relationship" in thing.labels:
            type_ = "Relationship"
        elif "n4sch__SubClass" in thing.labels:
            type_ = "SubClass"
        elif "n4sch__Individual" in thing.labels:
            type_ = "Individual"

        entity = {"id": thing.id, "type": type_}
        for prop, val in thing.items():
            entity[prop] = val
        return entity

    def _execute_entity_query(self, query: Text) -> List[Dict[Text, Any]]:
        """
        Executes a query that returns a list of entities with all their attributes.
        """
        with self.driver.session() as session:
            print("Executing Cypher Query: " + query)
            result_iter = session.run(query)
            # concepts = result_iter.collect_concepts()
            entities = []
            for record in result_iter:
                entities.append(self._thing_to_dict(record["n"]))
            return entities

    def _execute_attribute_query(self, query: Text) -> List[Any]:
        """
        Executes a query that returns the value(s) an entity has for a specific
        attribute.
        """

        # this function converts new lines to \n we have to keep this in mind while parsing
        with self.driver.session() as session:
            print("Executing Cypher Query: " + query)
            result_iter = session.run(query)
            return list(result_iter.single())

    def execute_relation_query(
        self, query: Text, relation_name: Text
    ) -> List[Dict[Text, Any]]:
        """
        Execute a query that queries for a relation. All attributes of the relation and
        all entities participating in the relation are part of the result.
        """
        with self.driver.session() as session:
            print("Executing Cypher Query: " + query)
            result_iter = session.run(query)

            relations = []
            relationships = []

            for concept in result_iter:
                relationships.append(concept["r"])

            relation_entity = relationships[0].start_node
            relation = self._thing_to_dict(relation_entity)
            relation["type"] = relation_name
            relation["start"] = self._thing_to_dict(relationships[0].end_node)
            relation["end"] = self._thing_to_dict(relationships[1].end_node)
            relations.append(relation)

            return relations

    def get_attribute_of(self, entity: Text, attribute: Text) -> List[Any]:
        """
        Get the value of the given attribute for the provided entity.
        :param entity_type: entity type
        :param key_attribute: key attribute of entity
        :param entity: name of the entity
        :param attribute: attribute of interest
        :return: the value of the attribute
        """
        # me_clause = self._get_me_clause(entity_type)

        return self._execute_attribute_query(
            f"""
              match (n {{n4sch__name: "{entity}"}})
              return n.{attribute}
            """
        )

    def get_direct_relation_of(self, entity_type: Text, entity: Text, rel_type: Text):
        """
        Get the value of the given attribute for the provided entity.
        :param entity_type: entity type of the class (can be any type in case of individuals)
        :param entity: name of the entity
        :param rel_type: name of the relationship (isA in case of individuals, and subclassof in case of subclass)
        :return: the value of the attribute
        """
        return self._execute_entity_query(
            f"""
              match (a:{entity_type} {{n4sch__name: "{entity}"}})<-[r:{rel_type}]-(n)
              return n
            """
        )

    def get_entities(
        self, entity_type: Text, attributes: Optional[Dict[Text, Text]] = None
    ):
        attr = ""
        if attributes:
            attr = "{ "
            for key, value in attributes.items():
                attr = f"{attr} {key}: '{value}'"
            attr = attr + " }"
        print(attr)
        return self._execute_entity_query(
            f"""
             match (n:{entity_type} {attr} ) return n 
            """
        )

    # def get_payment_methods(self, attributes: [str] ):

    #     temp = self._execute_entity_query(
    #         f"""
    #           match (n:n4sch__class {{n4sch__name: "PaymentMethod"}}) return n
    #         """
    #     )

    def map(self, mapping_type: Text, mapping_key: Text) -> Text:
        """
        Query the given mapping table for the provided key.
        :param mapping_type: the name of the mapping table
        :param mapping_key: the mapping key
        :return: the mapping value
        """

        if (
            mapping_type == "attribute-mapping"
            and mapping_key in self.attribute_mapping
        ):
            return self.attribute_mapping[mapping_key]

        if (
            mapping_type == "entity-type-mapping"
            and mapping_key in self.entity_type_mapping
        ):
            return self.entity_type_mapping[mapping_key]


# if __name__ == "__main__":
#     q = KnowledgeGraph("bolt://localhost:7687", "neo4j", "jimin")
#     print(q.map("attribute-mapping", "what is"))
#     y = q.get_entities("n4sch__Class", {"n4sch__name": "KeyResource"})
#     print(y[0]["n4sch__comment"])
#     q.close()