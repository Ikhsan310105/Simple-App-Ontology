import os
import uuid
import json
import re
from pathlib import Path

from rdflib import RDF, Graph, Literal, URIRef, Namespace
from rdflib.namespace import XSD
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

TALENT_NAMESPACE = Namespace(
    "http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/"
)

def _static_path(*names):
    base_dir = Path(__file__).resolve().parents[1]
    return base_dir / "static" / Path(*names)

def load_base_ontology():
    """Load the base ontology structure"""
    ontology_path = _static_path("ontology.ttl")
    base_graph = Graph()
    base_graph.parse(str(ontology_path), format="turtle")
    base_graph.bind("talent", TALENT_NAMESPACE)
    return base_graph

def import_all_jobs_to_ontology(graph):
    """
    Import all jobs into the ontology graph and map missing skills.
    """
    JSON_PATH = _static_path("jobs.json")
    with open(JSON_PATH, "r", encoding="utf-8") as file:
        jobs_data = json.load(file)

    if isinstance(jobs_data, dict) and "result" in jobs_data:
        jobs_iter = jobs_data["result"]
    else:
        jobs_iter = jobs_data

    all_skills_query = """
    SELECT ?skill
    WHERE {
        ?skill rdf:type <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/Skills> .
    }
    """

    ontology_skills = {}
    for row in graph.query(all_skills_query):
        skill_uri = row[0]
        skill_name = str(skill_uri).split("/")[-1].replace("_", " ")
        ontology_skills[skill_name.lower()] = (skill_name, skill_uri)

    special_cases = {
        "c#": "cs",
        "ci/cd": "ci_cd",
        "pl/sql": "pl_sql",
    }

    jobs_processed = 0
    jobs_with_skills = 0
    missing_skills_map = {}

    for job in jobs_iter:
        job_url = job.get("job_url") or job.get("jobUrl")
        if not job_url:
            continue

        job_id = job_url.split("/")[-1]
        if "?" in job_id:
            job_id = job_id.split("?")[0]

        job_uri = TALENT_NAMESPACE[f"Job_{job_id}"]
        graph.add((job_uri, RDF.type, TALENT_NAMESPACE["Job"]))
        graph.add((job_uri, TALENT_NAMESPACE["job_url"], Literal(job_url, datatype=XSD.string)))

        skills_added = 0
        missing_skills = []
        if "required_skills" in job and job["required_skills"]:
            for skill in job["required_skills"]:
                skill_lower = str(skill).lower()

                if skill_lower in special_cases:
                    mapped_skill = special_cases[skill_lower]
                    if mapped_skill in ontology_skills:
                        _, original_uri = ontology_skills[mapped_skill]
                        graph.add((job_uri, TALENT_NAMESPACE["REQUIRED_SKILL"], original_uri))
                        skills_added += 1
                        continue

                if skill_lower in ontology_skills:
                    _, original_uri = ontology_skills[skill_lower]
                    graph.add((job_uri, TALENT_NAMESPACE["REQUIRED_SKILL"], original_uri))
                    skills_added += 1
                else:
                    missing_skills.append(skill)

        if skills_added > 0:
            jobs_with_skills += 1

        if missing_skills:
            missing_skills_map[job_url] = missing_skills

        jobs_processed += 1

    return graph, missing_skills_map

def _get_neo4j_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    pwd = os.getenv("NEO4J_PASSWORD")
    if not (uri and user and pwd):
        raise RuntimeError("NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD are not set")
    return GraphDatabase.driver(uri, auth=(user, pwd))

def _fetch_users_from_neo4j():
    driver = _get_neo4j_driver()
    cypher = """
    MATCH (u:User)
    OPTIONAL MATCH (u)-[:HAS_SKILL]->(s:Skills)
    WITH u, collect(DISTINCT CASE
        WHEN s.name IS NOT NULL THEN s.name
        WHEN s.uri  IS NOT NULL THEN last(split(s.uri,'/'))
        ELSE NULL
    END) AS rawSkills
    RETURN coalesce(u.email, u.user_email) AS email,
           [sk IN rawSkills WHERE sk IS NOT NULL] AS skills
    """
    users = []
    with driver.session() as session:
        for rec in session.run(cypher):
            email = rec["email"]
            if not email:
                continue
            skills = rec["skills"] or []
            users.append({"email": email, "skills": skills})
    driver.close()
    return users

def import_all_users_to_ontology(graph):
    users_data = _fetch_users_from_neo4j()
    if not users_data:
        return graph

    all_skills_query = """
    SELECT ?skill
    WHERE {
        ?skill rdf:type <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/Skills> .
    }
    """

    ontology_skills = {}
    for row in graph.query(all_skills_query):
        skill_uri = row[0]
        skill_name = str(skill_uri).split("/")[-1].replace("_", " ")
        ontology_skills[skill_name.lower()] = (skill_name, skill_uri)

    special_cases = {
        "c#": "cs",
        "ci/cd": "ci_cd",
        "pl/sql": "pl_sql",
    }

    def _sanitize_id(text: str) -> str:
        return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_") or "User"

    for user in users_data:
        user_id = _sanitize_id(user["email"])
        user_uri = TALENT_NAMESPACE[f"User_{user_id}"]
        graph.add((user_uri, RDF.type, TALENT_NAMESPACE["User"]))
        graph.add((user_uri, TALENT_NAMESPACE["user_email"], Literal(user["email"], datatype=XSD.string)))

        for skill in user.get("skills", []):
            skill_lower = str(skill).lower()

            if skill_lower in special_cases:
                mapped_skill = special_cases[skill_lower]
                if mapped_skill in ontology_skills:
                    _, original_uri = ontology_skills[mapped_skill]
                    graph.add((user_uri, TALENT_NAMESPACE["HAS_SKILL"], original_uri))
                    continue

            if skill_lower in ontology_skills:
                _, original_uri = ontology_skills[skill_lower]
                graph.add((user_uri, TALENT_NAMESPACE["HAS_SKILL"], original_uri))

    return graph

def add_user_job_matches_to_ontology(graph, match_results):
    for match in match_results:
        user_uri_str = str(match["user"])
        job_uri_str = str(match["job"])
        
        user_id = user_uri_str.split("/")[-1]
        
        job_id = job_uri_str.split("/")[-1]
        
        match_id = f"{user_id}_AND_{job_id}"
        match_uri = TALENT_NAMESPACE[f"UserJobMatch_{match_id}"]

        graph.add((match_uri, RDF.type, TALENT_NAMESPACE["UserJobMatch"]))
        graph.add((match_uri, TALENT_NAMESPACE["USER_MATCH"], URIRef(match["user"])))
        graph.add((match_uri, TALENT_NAMESPACE["JOB_MATCH"], URIRef(match["job"])))
        graph.add(
            (
                match_uri,
                TALENT_NAMESPACE["similarityScore"],
                Literal(match["similarity"], datatype=XSD.float),
            )
        )

    return graph