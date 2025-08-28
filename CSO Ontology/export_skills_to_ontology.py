import os
import sys
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, List

from neo4j import GraphDatabase


def env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def log(msg: str):
    print(f"[export] {msg}")


def slugify_local(name: str) -> str:
    # Convert to a safe Turtle prefixed name (no spaces/punct, no leading digit)
    local = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    if not local:
        local = "Skill"
    if re.match(r"^[0-9]", local):
        local = f"S_{local}"
    return local


def fetch_skills_and_rels(session) -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    # elementId -> name
    skills: Dict[str, str] = {}
    # parent -> child by element ids
    rels: List[Tuple[str, str]] = []

    # Fetch skills
    for rec in session.run("MATCH (s:Skill) WHERE s.name IS NOT NULL RETURN elementId(s) AS id, s.name AS name"):
        if rec["name"] and isinstance(rec["name"], str):
            skills[rec["id"]] = rec["name"].strip()

    # Fetch relationships SUPER_TOPIC_OF (parent -> child)
    for rec in session.run(
        "MATCH (p:Skill)-[:SUPER_TOPIC_OF]->(c:Skill) RETURN elementId(p) AS pid, elementId(c) AS cid"
    ):
        rels.append((rec["pid"], rec["cid"]))

    return skills, rels


def build_ttl_fragment(skills: Dict[str, str], rels: List[Tuple[str, str]]) -> str:
    # Map names to unique locals
    name_to_local: Dict[str, str] = {}
    used: set[str] = set()

    def unique_local(base: str) -> str:
        if base not in used:
            used.add(base)
            return base
        i = 2
        while f"{base}_{i}" in used:
            i += 1
        alias = f"{base}_{i}"
        used.add(alias)
        return alias

    id_to_local: Dict[str, str] = {}
    for sid, name in skills.items():
        base = slugify_local(name)
        if name not in name_to_local:
            name_to_local[name] = unique_local(base)
        id_to_local[sid] = name_to_local[name]

    lines: List[str] = []
    ts = datetime.now().isoformat(timespec="seconds")
    lines.append("")
    lines.append("#################################################################")
    lines.append("#    Individuals (Skills exported from Neo4j)")
    lines.append("#################################################################")
    lines.append(f"# Generated {ts}")
    lines.append("")

    # Individuals
    for sid, name in sorted(skills.items(), key=lambda kv: kv[1].lower()):
        local = id_to_local[sid]
        # Write as :Local a :Skills ; rdfs:label "Name" .
        # Use triple quotes if needed; here standard string is fine with escaping
        escaped = name.replace('"', '\\"')
        lines.append(f":{local} rdf:type :Skills ;")
        lines.append(f"        rdfs:label \"{escaped}\" .")
        lines.append("")

    # Relationships parentOf (map from SUPER_TOPIC_OF)
    for pid, cid in rels:
        if pid in id_to_local and cid in id_to_local:
            p_local = id_to_local[pid]
            c_local = id_to_local[cid]
            lines.append(f":{p_local} :parentOf :{c_local} .")

    lines.append("")
    return "\n".join(lines)


def append_to_ontology(ontology_path: Path, fragment: str) -> None:
    # Backup then append
    bak = ontology_path.with_suffix(ontology_path.suffix + ".bak")
    if not bak.exists():
        bak.write_text(ontology_path.read_text(encoding="utf-8"), encoding="utf-8")
        log(f"Backup created: {bak}")
    with ontology_path.open("a", encoding="utf-8") as f:
        f.write("\n")
        f.write(fragment)
    log(f"Appended {len(fragment.splitlines())} lines to {ontology_path}")


def write_separate_ttl(out_path: Path, fragment: str, base_prefix_iri: str) -> None:
    header = (
        f"@prefix : <{base_prefix_iri}> .\n"
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
        "@prefix xml: <http://www.w3.org/XML/1998/namespace> .\n"
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
        f"@base <{base_prefix_iri}> .\n\n"
    )
    out_path.write_text(header + fragment, encoding="utf-8")
    log(f"Wrote individuals to {out_path}")


def main():
    workspace = Path(__file__).resolve().parent
    ontology_path = Path(env("ONTOLOGY_PATH", str(workspace / "ontology.ttl")))
    base_prefix_iri = env(
        "BASE_PREFIX_IRI",
        "http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/",
    )
    append_mode = env("APPEND", "1") == "1"
    out_path = Path(env("OUT_TTL", str(workspace / "ontology.skills.ttl")))

    if not ontology_path.exists() and append_mode:
        log(f"ontology.ttl not found at {ontology_path}. Switch to standalone output or set ONTOLOGY_PATH.")
        sys.exit(2)

    uri = env("NEO4J_URI", "neo4j://localhost:7687")
    user = env("NEO4J_USER", "neo4j")
    password = env("NEO4J_PASSWORD") or "12345678"
    database = env("NEO4J_DATABASE")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=database) as session:
            # Optional: verify n10s exists but not required for reading
            skills, rels = fetch_skills_and_rels(session)
            if not skills:
                log("No Skill nodes with name found. Nothing to export.")
                return
            fragment = build_ttl_fragment(skills, rels)
    finally:
        driver.close()

    if append_mode:
        append_to_ontology(ontology_path, fragment)
    else:
        write_separate_ttl(out_path, fragment, base_prefix_iri)


if __name__ == "__main__":
    main()
