from typing import List, Optional, Dict, Any
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

from services.import_service import (
    load_base_ontology,
    import_all_jobs_to_ontology,
    import_all_users_to_ontology,
    add_user_job_matches_to_ontology,
)
from services.matching_service import calculate_all_user_job_similarities
from services.reasoning_service import apply_reasoning
from services.neo4j_service import import_graph_to_neo4j_with_n10s

load_dotenv()

def _get_neo4j_driver():
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    pwd = os.getenv("NEO4J_PASSWORD")
    if not (uri and user and pwd):
        raise RuntimeError("NEO4J_URI/NEO4J_USERNAME/NEO4J_PASSWORD are not set")
    return GraphDatabase.driver(uri, auth=(user, pwd))

def init_user_indexes():
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            s.run("CREATE INDEX user_email IF NOT EXISTS FOR (u:User) ON (u.email)")
            s.run("CREATE INDEX skills_name IF NOT EXISTS FOR (s:Skills) ON (s.name)")
    finally:
        driver.close()

def _ensure_skill(session, skill_name: str) -> None:
    rec = session.run(
        """
        MATCH (s:Skills)
        WHERE toLower(s.name) = toLower($name)
        RETURN s LIMIT 1
        """,
        {"name": skill_name},
    ).single()

    if not rec:
        session.run("CREATE (:Skills {name: $name})", {"name": skill_name})

def create_user(email: str, skills: Optional[List[str]] = None) -> None:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            s.run(
                """
                MERGE (u:User {email: $email})
                SET u.email = $email
                """,
                {"email": email},
            )
            if skills:
                for sk in skills:
                    if not sk:
                        continue
                    _ensure_skill(s, sk)
                    s.run(
                        """
                        MATCH (u:User {email: $email})
                        MATCH (s:Skills) WHERE toLower(s.name) = toLower($sk)
                        MERGE (u)-[:HAS_SKILL]->(s)
                        """,
                        {"email": email, "sk": sk},
                    )
    finally:
        driver.close()

def get_user(email: str) -> Optional[Dict[str, Any]]:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            rec = s.run(
                """
                MATCH (u:User {email: $email})
                OPTIONAL MATCH (u)-[:HAS_SKILL]->(s:Skills)
                RETURN u.email AS email, collect(distinct s.name) AS skills
                """,
                {"email": email},
            ).single()
            if not rec:
                return None
            return {"email": rec["email"], "skills": [x for x in rec["skills"] if x]}
    finally:
        driver.close()

def list_users(limit: int = 50, skip: int = 0) -> List[Dict[str, Any]]:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            rows = s.run(
                """
                MATCH (u:User)
                OPTIONAL MATCH (u)-[:HAS_SKILL]->(s:Skills)
                WITH u, collect(distinct s.name) AS skills
                RETURN u.email AS email, skills
                SKIP $skip LIMIT $limit
                """,
                {"skip": skip, "limit": limit},
            ).data()
            return [{"email": r["email"], "skills": [x for x in r["skills"] if x]} for r in rows]
    finally:
        driver.close()

def update_user_email(old_email: str, new_email: str) -> bool:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            summary = s.run(
                """
                MATCH (u:User {email: $old})
                SET u.email = $new
                RETURN count(u) AS c
                """,
                {"old": old_email, "new": new_email},
            ).single()
            return (summary and summary["c"] > 0)
    finally:
        driver.close()

def add_user_skills(email: str, skills: List[str]) -> None:
    if not skills:
        return
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            for sk in skills:
                if not sk:
                    continue
                _ensure_skill(s, sk)
                s.run(
                    """
                    MATCH (u:User {email: $email})
                    MATCH (s:Skills) WHERE toLower(s.name) = toLower($sk)
                    MERGE (u)-[:HAS_SKILL]->(s)
                    """,
                    {"email": email, "sk": sk},
                )
    finally:
        driver.close()

def remove_user_skills(email: str, skills: List[str]) -> None:
    if not skills:
        return
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            s.run(
                """
                UNWIND $skills AS sk
                MATCH (u:User {email: $email})-[r:HAS_SKILL]->(s:Skills)
                WHERE toLower(s.name) = toLower(sk)
                DELETE r
                """,
                {"email": email, "skills": skills},
            )
    finally:
        driver.close()

def replace_user_skills(email: str, skills: List[str]) -> None:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            s.run("MATCH (u:User {email: $email})-[r:HAS_SKILL]->() DELETE r", {"email": email})
            for sk in skills or []:
                if not sk:
                    continue
                _ensure_skill(s, sk)
                s.run(
                    """
                    MATCH (u:User {email: $email})
                    MATCH (s:Skills) WHERE toLower(s.name) = toLower($sk)
                    MERGE (u)-[:HAS_SKILL]->(s)
                    """,
                    {"email": email, "sk": sk},
                )
    finally:
        driver.close()

def delete_user(email: str) -> bool:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            c = s.run(
                "MATCH (u:User {email: $email}) DETACH DELETE u RETURN 1 AS ok",
                {"email": email},
            ).single()
            return bool(c and c["ok"] == 1)
    finally:
        driver.close()

def list_skills(limit: int = 1000) -> List[str]:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            rows = s.run(
                """
                MATCH (s:Skills)
                WHERE s.name IS NOT NULL
                RETURN s.name AS name
                ORDER BY toLower(name)
                LIMIT $limit
                """,
                {"limit": limit},
            ).data()
            return [r["name"] for r in rows]
    finally:
        driver.close()

def list_user_matches(email: str) -> List[Dict[str, Any]]:
    driver = _get_neo4j_driver()
    try:
        with driver.session() as s:
            rows = s.run(
                """
                MATCH (u:User {email: $email})
                MATCH (m:UserJobMatch)-[:USER_MATCH]->(u)
                MATCH (m)-[:JOB_MATCH]->(j:Job)
                RETURN j.jobTitle AS jobTitle,
                       j.companyName AS companyName,
                       toFloat(m.similarityScore) AS similarity,
                       m.MatchType AS matchType
                ORDER BY similarity DESC
                """,
                {"email": email},
            ).data()
            return rows
    finally:
        driver.close()

def start_matching() -> None:
    graph = load_base_ontology()
    graph, missing_skills = import_all_jobs_to_ontology(graph)
    graph = import_all_users_to_ontology(graph)
    match_results = calculate_all_user_job_similarities(graph)
    graph = add_user_job_matches_to_ontology(graph, match_results)
    reasoned_graph = apply_reasoning(graph)
    import_graph_to_neo4j_with_n10s(reasoned_graph, missing_skills)