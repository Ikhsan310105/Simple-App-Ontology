# CSO TTL import to Neo4j with n10s

This project provides a small Python script that configures neosemantics (n10s) and imports `CSO.3.5.ttl` into Neo4j 5.15 with the requested configuration and mappings.

## What it does
- Sets n10s graph config:
  - handleVocabUris=MAP, handleMultival=OVERWRITE, handleRDFTypes=LABELS
  - keepLangTag=false, keepCustomDataTypes=false, applyNeo4jNaming=true
  - Labels and relationships: Class, Property, Relationship, SCO, SPO, DOMAIN, RANGE
- Adds namespace prefixes: `rdfs`, `ns0`
- Adds mappings:
  - `rdfs:label` -> property `name`
  - `cso:Topic` -> label `Skill`
  - `cso:superTopicOf` -> relationship `SUPER_TOPIC_OF`
- Imports `CSO.3.5.ttl` either inline (small files) or via a temporary local HTTP server (for larger files)

## Prereqs
- Neo4j 5.15 running and neosemantics 5.15 installed in the target database
- Python 3.9+ and a virtual environment activated

## Setup
```pwsh
# In the repo folder (ensure your venv is active)
pip install -r requirements.txt
```

## Run
Set your connection info and run the script:
```pwsh
$env:NEO4J_URI = "neo4j://localhost:7687"   # or bolt://localhost:7687
$env:NEO4J_USER = "neo4j"
$env:NEO4J_PASSWORD = "<your-password>"
# Optional: if not "neo4j"
# $env:NEO4J_DATABASE = "<database-name>"

# Optional: override TTL path if needed
# $env:TTL_PATH = "E:/Project/Penelitian Modul/CSO Ontology/CSO.3.5.ttl"

python .\import_cso_to_neo4j.py
```

If the TTL is large, the script will host a temporary local HTTP server and use `n10s.rdf.import.fetch`.

## Notes
- This script is idempotent for the config and mappings; re-running will keep settings.
- Ensure your database has the n10s procedures available: try `CALL n10s.graphconfig.get()` in Neo4j Browser.
