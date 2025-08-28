import os
import json
from pathlib import Path
from rdflib import Graph
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

def _get_neo4j_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    pwd = os.getenv("NEO4J_PASSWORD")
    if not (uri and user and pwd):
        raise RuntimeError("NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD are not set")
    return GraphDatabase.driver(uri, auth=(user, pwd))

def _drop_all_constraints_and_indexes(session):
    for rec in session.run("SHOW CONSTRAINTS"):
        name = rec.get("name")
        if name:
            session.run(f"DROP CONSTRAINT {name} IF EXISTS")
    for rec in session.run("SHOW INDEXES"):
        name = rec.get("name")
        idx_type = (rec.get("type") or "").upper()
        if name and idx_type != "LOOKUP":
            session.run(f"DROP INDEX {name} IF EXISTS")

def reset_neo4j_database(drop_n10s_config: bool = False):
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    pwd = os.getenv("NEO4J_PASSWORD")
    if not (uri and user and pwd):
        raise RuntimeError("NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD are not set")

    driver = GraphDatabase.driver(uri, auth=(user, pwd))
    try:
        with driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
            if drop_n10s_config:
                try:
                    s.run("CALL n10s.graphconfig.drop()")
                except Exception:
                    pass
    finally:
        driver.close()

def _post_import_cleanup(session):
    base_uri = "http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/"

    session.run("""
        WITH $base AS base
        MATCH (s:Skills)
        WHERE s.uri IS NOT NULL AND s.uri STARTS WITH base
        SET s.name = replace(s.uri, base, '')
    """, {"base": base_uri})
    session.run("CREATE INDEX skills_name IF NOT EXISTS FOR (s:Skills) ON (s.name)")

    session.run("""
        MATCH (m:UserJobMatch)
        WHERE m.similarityScore IS NOT NULL
        WITH m, toFloat(m.similarityScore) AS s
        SET m.MatchType =
          CASE
            WHEN s >= 0.65 THEN 'Strong Match'
            WHEN s > 0.35 AND s < 0.65 THEN 'Mid Match'
            ELSE 'Weak Match'
          END
    """)
    session.run("MATCH (m:UserJobMatch:Weak_Match)  REMOVE m:Weak_Match")
    session.run("MATCH (m:UserJobMatch:Mid_Match)   REMOVE m:Mid_Match")
    session.run("MATCH (m:UserJobMatch:Strong_Match) REMOVE m:Strong_Match")

    session.run("""
        MATCH (u:User)
        WHERE u.user_email IS NOT NULL
        SET u.email = coalesce(u.email, u.user_email),
            u.user_email = null
    """)
    session.run("CREATE INDEX user_email IF NOT EXISTS FOR (u:User) ON (u.email)")

    for lbl in ["Class", "Datatype", "DatatypeProperty", "ObjectProperty", "Ontology", "Restriction"]:
        session.run(f"MATCH (n:`{lbl}`) DETACH DELETE n")

    for rel in ["domain", "equivalentClass", "first", "intersectionOf",
                "onDatatype", "onProperty", "range", "rest",
                "someValuesFrom", "subClassOf", "withRestrictions"]:
        session.run(f"MATCH ()-[r:`{rel}`]->() DELETE r")

def _update_jobs_from_json(session):
    session.run("""
        MATCH (j:Job)
        WHERE j.job_url IS NOT NULL AND (j.jobUrl IS NULL OR j.jobUrl <> j.job_url)
        SET j.jobUrl = j.job_url
        REMOVE j.job_url
    """)

    jobs_path = Path(__file__).resolve().parents[1] / "static" / "jobs.json"
    with jobs_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    jobs_iter = data["result"] if isinstance(data, dict) and "result" in data else data

    rows = []
    for job in jobs_iter:
        job_url = job.get("jobUrl") or job.get("job_url")
        if not job_url:
            continue

        sanitized = {k: v for k, v in job.items() if k != "required_skills"}
        sanitized["jobUrl"] = job_url
        rows.append(sanitized)

    if rows:
        session.run("""
            UNWIND $rows AS row
            MATCH (j:Job {jobUrl: row.jobUrl})
            SET j += row
        """, {"rows": rows})

def _create_additional_skills(session, missing_skills: dict | None):
    if not missing_skills:
        return

    rows = []
    for job_url, skills in missing_skills.items():
        if not skills:
            continue
        for s in skills:
            if s:
                rows.append({"jobUrl": job_url, "name": str(s)})

    if rows:
        session.run("""
            UNWIND $rows AS row
            MATCH (j:Job {jobUrl: row.jobUrl})
            MERGE (a:AdditionalSkill {name: row.name})
            MERGE (j)-[:REQUIRED_SKILL]->(a)
        """, {"rows": rows})

def import_graph_to_neo4j_with_n10s(graph: Graph, missing_skills: dict | None = None):
    reset_neo4j_database(drop_n10s_config=True)

    driver = _get_neo4j_driver()
    ttl_text = graph.serialize(format="turtle")
    try:
        with driver.session() as session:
            session.run("""
                CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
                FOR (r:Resource) REQUIRE r.uri IS UNIQUE
            """)
            session.run("""
                CALL n10s.graphconfig.init($config)
            """, {"config": {
                "handleVocabUris": "IGNORE",
                "handleRDFTypes": "LABELS",
                "keepLangTag": False,
                "applyNeo4jNaming": False,
                "keepCustomDataTypes": True
            }})
            session.run(
                "CALL n10s.rdf.import.inline($payload, 'Turtle')",
                {"payload": ttl_text},
            )

            _post_import_cleanup(session)
            _update_jobs_from_json(session)
            _create_additional_skills(session, missing_skills)
    finally:
        driver.close()