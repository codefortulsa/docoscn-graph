import csv

from decouple import config
from neo4j.v1 import GraphDatabase, basic_auth


NEO4J_BOLT_URL = config('NEO4J_BOLT_URL', default='')
NEO4J_USER = config('NEO4J_USER', default='')
NEO4J_PASSWORD = config('NEO4J_PASSWORD', default='')


driver = GraphDatabase.driver(
    NEO4J_BOLT_URL,
    auth=basic_auth(
        NEO4J_USER, NEO4J_PASSWORD
    )
)
session = driver.session()

# TODO: Figure out a way to not delete the whole graph db every time
session.run("MATCH (n) DETACH DELETE n")


def neo4j_merge_person(person, person_node_template, session):
    # neo4j statement requires a name parameter; set if empty
    # person.setdefault('name', '')
    statement = "MERGE %s" % person_node_template
    session.run(statement, person)
    session.sync()


with open('Offender.csv', 'rb') as csvfile:
    personreader = csv.reader(csvfile, delimiter=',')
    for row in personreader:
        if row[0] == 'DocNum':
            person_node_fields = []
            for field in row[1:]:
                person_node_fields.append(field)
            person_node_template = "(p:Person {"
            for field in person_node_fields:
                person_node_template += "%s: {%s}, " % (field, field)
            # after all the fields, take off the trailing ', ' characters
            person_node_template = person_node_template[:-2]
            person_node_template += "})"
        else:
            person = {
                'LastName': row[1],
                'FirstName': row[2],
                'MiddleInit': row[3],
                'Suffix': row[4],
                'Race': row[5],
                'Gender': row[6],
                'HairColor': row[7],
                'EyeColor': row[8],
                'Height': row[9],
                'Weight': row[10],
                'DOB': row[11],
                'ReceptionDate': row[12],
                'CurrentFacility': row[13],
                'Status': row[14]
            }
            neo4j_merge_person(person, person_node_template, session)

session.close()
