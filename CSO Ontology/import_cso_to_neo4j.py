import os
import sys
import json
import threading
from pathlib import Path
from contextlib import contextmanager
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer

from neo4j import GraphDatabase


def env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    return val if val is not None and val != "" else default


def log(msg: str):
    print(f"[import] {msg}")


def get_config_map() -> dict:
    return {
        # Core config per request
        "handleVocabUris": "MAP",
        "handleMultival": "OVERWRITE",
        "handleRDFTypes": "LABELS",
        "keepLangTag": False,
        "multivalPropList": None,
        "keepCustomDataTypes": False,
        "customDataTypePropList": None,
        "applyNeo4jNaming": True,
        # Label/rel naming preferences
        "classLabel": "Class",
        "subClassOfRel": "SCO",
        "dataTypePropertyLabel": "Property",
        "objectPropertyLabel": "Relationship",
        "subPropertyOfRel": "SPO",
        "domainRel": "DOMAIN",
        "rangeRel": "RANGE",
    }


def configure_n10s(session):
    cfg = get_config_map()

    # Initialize graph config (robust across versions/signatures)
    log("Ensuring n10s graph configuration…")
    init_done = False
    try:
        session.run("CALL n10s.graphconfig.init($cfg)", cfg=cfg).consume()
        init_done = True
    except Exception:
        # Try no-arg init (older signature) or already-initialized case
        try:
            session.run("CALL n10s.graphconfig.init()").consume()
            init_done = True
        except Exception:
            # Probably already initialized; continue to set()
            pass

    if init_done:
        log("n10s graph configuration initialized.")
    else:
        log("n10s graph configuration init skipped (likely already initialized).")

    # Apply settings
    try:
        session.run("CALL n10s.graphconfig.set($cfg)", cfg=cfg).consume()
        log("n10s graph configuration updated.")
    except Exception:
        log("n10s graph configuration set skipped (procedure not available in this version).")

    log("Registering namespace prefixes…")
    # rdfs
    session.run(
        "CALL n10s.nsprefixes.add($prefix, $ns)",
        prefix="rdfs",
        ns="http://www.w3.org/2000/01/rdf-schema#",
    ).consume()
    # CSO schema
    session.run(
        "CALL n10s.nsprefixes.add($prefix, $ns)",
        prefix="ns0",
        ns="http://cso.kmi.open.ac.uk/schema/cso#",
    ).consume()

    log("Adding element mappings…")

    def try_mapping(kind: str, from_uri: str, to_name: str):
        # Try shape 1: keyed by kind
        try:
            key = {"property": "property", "class": "class", "rel": "rel"}[kind]
            session.run("CALL n10s.mapping.add($m)", m={key: from_uri, "name": to_name}).consume()
            return
        except Exception:
            pass
        # Try shape 2: explicit from/to/type
        try:
            type_map = {"property": "PROPERTY", "class": "NODE", "rel": "RELATIONSHIP"}
            session.run(
                "CALL n10s.mapping.add($m)",
                m={"from": from_uri, "to": to_name, "type": type_map[kind]},
            ).consume()
            return
        except Exception:
            pass
        # Try shape 3: two-arg form
        session.run(
            "CALL n10s.mapping.add($from, $to)",
            parameters={"from": from_uri, "to": to_name},
        ).consume()

    # Map rdfs:label -> name (property)
    try_mapping("property", "http://www.w3.org/2000/01/rdf-schema#label", "name")
    # Map cso:Topic -> :Skill (label)
    try_mapping("class", "http://cso.kmi.open.ac.uk/schema/cso#Topic", "Skill")
    # Map cso:superTopicOf -> :SUPER_TOPIC_OF (relationship)
    try_mapping("rel", "http://cso.kmi.open.ac.uk/schema/cso#superTopicOf", "SUPER_TOPIC_OF")

    # Show final config (optional)
    # Try to display config with different getters depending on version
    shown = False
    for stmt in (
        "CALL n10s.graphconfig.get() YIELD param, value RETURN collect({param:param, value:value}) AS cfg",
        "CALL n10s.graphconfig.show() YIELD param, value RETURN collect({param:param, value:value}) AS cfg",
        "CALL n10s.graphconfig() YIELD param, value RETURN collect({param:param, value:value}) AS cfg",
    ):
        try:
            rec = session.run(stmt).single()
            if rec and rec[0] is not None:
                log(f"n10s config: {json.dumps(rec[0], ensure_ascii=False)}")
                shown = True
                break
        except Exception:
            continue
    if not shown:
        log("n10s config readout unavailable on this version; proceeding.")


@contextmanager
def local_http_server(serve_dir: Path):
    class QuietHandler(SimpleHTTPRequestHandler):
        # Reduce noise
        def log_message(self, format, *args):  # noqa: N802 (match parent)
            pass

    # Bind to random port
    with TCPServer(("127.0.0.1", 0), QuietHandler) as httpd:
        port = httpd.server_address[1]
        # Change the directory served by the handler
        def chdir_then_serve():
            os.chdir(serve_dir)
            httpd.serve_forever()

        thread = threading.Thread(target=chdir_then_serve, daemon=True)
        thread.start()
        try:
            yield port
        finally:
            httpd.shutdown()
            thread.join(timeout=2)


def import_ttl(session, ttl_path: Path, prefer_inline: bool | None = None):
    size = ttl_path.stat().st_size
    # Heuristic: inline for small files (< 8 MB) unless FORCE_INLINE=1
    if prefer_inline is None:
        prefer_inline = env("FORCE_INLINE", "0") == "1" or size < 8 * 1024 * 1024

    if prefer_inline:
        log(f"Importing inline (size={size/1024:.1f} KiB)…")
        data = ttl_path.read_text(encoding="utf-8")
        res = session.run(
            "CALL n10s.rdf.import.inline($rdf, 'Turtle') YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces",
            rdf=data,
        ).single()
        log(f"Inline import status: {res['terminationStatus']}, loaded={res['triplesLoaded']}/{res['triplesParsed']}")
        return

    # Fallback to fetch via a local HTTP server
    log("Importing via temporary local HTTP server…")
    with local_http_server(ttl_path.parent) as port:
        url = f"http://127.0.0.1:{port}/{ttl_path.name}"
        log(f"Fetching from {url}")
        res = session.run(
            "CALL n10s.rdf.import.fetch($url, 'Turtle') YIELD terminationStatus, triplesLoaded, triplesParsed, namespaces RETURN terminationStatus, triplesLoaded, triplesParsed, namespaces",
            url=url,
        ).single()
        log(f"Fetch import status: {res['terminationStatus']}, loaded={res['triplesLoaded']}/{res['triplesParsed']}")


def main():
    # Inputs
    workspace = Path(__file__).resolve().parent
    ttl_path = Path(env("TTL_PATH", str(workspace / "CSO.3.5.ttl")))
    if not ttl_path.exists():
        log(f"TTL file not found: {ttl_path}")
        sys.exit(1)

    uri = env("NEO4J_URI", "neo4j://localhost:7687")
    user = env("NEO4J_USER", "neo4j")
    password = env("NEO4J_PASSWORD", "12345678")
    if not password:
        log("Missing NEO4J_PASSWORD (set env var)")
        sys.exit(2)

    log(f"Connecting to {uri} as {user}…")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session(database=env("NEO4J_DATABASE", None)) as session:
            # Preflight: ensure n10s plugin is installed and enabled on this DB
            names_rec = session.run(
                "SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'n10s.' RETURN collect(name) AS names"
            ).single()
            if not names_rec or not names_rec[0]:
                log(
                    "Neosemantics (n10s) procedures not found in this database.\n"
                    "Install/enable n10s 5.15 and restart Neo4j, then re-run.\n"
                    "Quick checklist:\n"
                    "1) Place the n10s-5.15.x.jar in the Neo4j plugins directory.\n"
                    "2) In neo4j.conf, set:\n"
                    "   dbms.security.procedures.unrestricted=n10s.*\n"
                    "   dbms.security.procedures.allowlist=n10s.*\n"
                    "3) Restart Neo4j and target the correct database (NEO4J_DATABASE)."
                )
                sys.exit(3)
            configure_n10s(session)
            import_ttl(session, ttl_path)
        log("Done.")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
