import math

def sanchez_similarity(set_a, set_b):
    if set_a == set_b:
        return 1.0

    a_minus_b = len(set_a - set_b)
    b_minus_a = len(set_b - set_a)
    intersection = len(set_a.intersection(set_b))

    if intersection == 0:
        return 0.0

    numerator = a_minus_b + b_minus_a
    denominator = a_minus_b + b_minus_a + intersection

    similarity = 1 - math.log2(1 + numerator / denominator)
    return similarity


def get_limited_ancestors(graph, skill_uri, max_levels=4):
    ancestors = set()
    ancestors.add(str(skill_uri))

    SKILLS_CLASS = "http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/Skills"
    ancestors.add(SKILLS_CLASS)

    current_level = [skill_uri]
    level = 0

    while current_level and level < max_levels:
        next_level = []
        for node in current_level:
            query = f"""
            SELECT ?parent
            WHERE {{
                ?parent <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/parentOf> <{node}> .
            }}
            """

            for row in graph.query(query):
                parent = row[0]
                parent_str = str(parent)

                if parent_str not in ancestors:
                    ancestors.add(parent_str)
                    next_level.append(parent)

        current_level = next_level
        level += 1
    return ancestors


def calculate_all_user_job_similarities(graph):
    users_query = """
    SELECT ?user ?email
    WHERE {
        ?user rdf:type <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/User> .
        ?user <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/user_email> ?email .
    }
    """

    jobs_query = """
    SELECT ?job
    WHERE {
        ?job rdf:type <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/Job> .
    }
    """

    users = [(row[0], str(row[1])) for row in graph.query(users_query)]
    jobs = [row[0] for row in graph.query(jobs_query)]

    match_results = []
    processed_combinations = 0

    for user_uri, email in users:
        user_skills_query = f"""
        SELECT ?skill
        WHERE {{
            <{user_uri}> <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/HAS_SKILL> ?skill .
        }}
        """

        user_skills = [row[0] for row in graph.query(user_skills_query)]

        if not user_skills:
            continue

        user_matches = 0

        for job_uri in jobs:
            job_skills_query = f"""
            SELECT ?skill
            WHERE {{
                <{job_uri}> <http://www.semanticweb.org/kota203/ontologies/2025/3/talent-matching-ontology/REQUIRED_SKILL> ?skill .
            }}
            """

            job_skills = [row[0] for row in graph.query(job_skills_query)]

            if not job_skills:
                continue

            skill_similarities = []

            for user_skill in user_skills:
                user_skill_features = get_limited_ancestors(graph, user_skill)
                max_skill_similarity = 0

                for job_skill in job_skills:
                    job_skill_features = get_limited_ancestors(graph, job_skill)
                    skill_similarity = sanchez_similarity(
                        user_skill_features, job_skill_features
                    )

                    if skill_similarity > max_skill_similarity:
                        max_skill_similarity = skill_similarity

                skill_similarities.append(max_skill_similarity)

            skill_similarities = sorted(skill_similarities, reverse=True)
            if skill_similarities:
                overall_similarity = sum(skill_similarities[0 : len(job_skills)]) / len(
                    job_skills
                )

                if overall_similarity > 0:
                    match_results.append(
                        {
                            "user": user_uri,
                            "job": job_uri,
                            "similarity": overall_similarity,
                        }
                    )
                    user_matches += 1

            processed_combinations += 1

    return match_results