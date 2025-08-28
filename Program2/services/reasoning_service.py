import tempfile
import os
from owlready2 import sync_reasoner_pellet, get_ontology, OwlReadyOntologyParsingError
from rdflib import Graph

def apply_reasoning(graph):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ttl', delete=False, encoding='utf-8') as temp_ttl:
        graph.serialize(temp_ttl.name, format='turtle')
        temp_ttl_path = temp_ttl.name

    temp_owl_path = temp_ttl_path.replace('.ttl', '.owl')
    temp_owl_output = temp_ttl_path.replace('.ttl', '_output.owl')
    temp_ttl_output = temp_ttl_path.replace('.ttl', '_output.ttl')
    
    try:
        temp_graph = Graph()
        temp_graph.parse(temp_ttl_path, format='turtle')
        temp_graph.serialize(temp_owl_path, format='xml')
        
        onto = get_ontology(f"file://{temp_owl_path}").load()
        
        with onto:
            sync_reasoner_pellet()
        
        onto.save(temp_owl_output, format="rdfxml")
        
        reasoned_graph = Graph()
        reasoned_graph.parse(temp_owl_output, format='xml')
        
        return reasoned_graph
        
    except OwlReadyOntologyParsingError as e:
        print(f"Ontology parsing error: {e}")
        print("Returning original graph without reasoning...")
        return graph
        
    except Exception as e:
        print(f"Reasoning error: {e}")
        print("Returning original graph without reasoning...")
        return graph
        
    finally:
        for temp_file in [temp_ttl_path, temp_owl_path, temp_owl_output, temp_ttl_output]:
            if os.path.exists(temp_file):
                os.remove(temp_file)