#!/usr/bin/env python3
"""
This script contains specific functions to interpret a specific set of
spreadsheets

Authors:
    - Jon Clucas, 2017–2018 (jon.clucas@childmind.org)
    - Anirudh Krishnakumar, 2017–2018
    - Arno Klein, 2019 (arno@childmind.org)  http://binarybottle.com

Copyright 2019, Child Mind Institute (http://childmind.org), Apache v2.0 License

"""
try:
    from mhdb.spreadsheet_io import download_google_sheet, return_string
    from mhdb.write_ttl import check_iri, language_string
except:
    from mhdb.mhdb.spreadsheet_io import download_google_sheet, return_string
    from mhdb.mhdb.write_ttl import check_iri, language_string
import numpy as np
import pandas as pd


def add_if(subject, predicate, object, statements={}):
    """
    Function to add an object and predicate to a dictionary, checking for that
    predicate first.

    Parameters
    ----------
    subject: string
        Turtle-formatted IRI

    predicate: string
        Turtle-formatted IRI

    object: string
        Turtle-formatted IRI

    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Return
    ------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    >>> print(add_if(":goose", ":chases", ":it"))
    {':goose': {':chases': {':it'}}}
    """
    if subject not in statements:
        statements[subject] = {}
    if predicate not in statements[subject]:
        statements[subject][predicate] = {
            object
        }
    else:
        statements[subject][predicate].add(
            object
        )
    return(statements)


def audience_statements(statements={}):
    """
    Function to generate PeopleAudience subClasses.

    Parameter
    ---------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    >>> print(
    ...     audience_statements()["mhdb:MaleAudience"]["rdfs:subClassOf"]
    ... )
    {'schema:PeopleAudience'}
    """
    for gendered_audience in {
        "Male",
        "Female"
    }:
        gendered_iri = check_iri(
            "".join([
                    gendered_audience,
                    "Audience"
            ])
        )
        schema_gender = "schema:{0}".format(
            gendered_audience
        )
        g_statements = {
            "rdfs:subClassOf": {
                "schema:PeopleAudience"
            },
            "rdfs:label": {
                 language_string(
                    " ".join([
                            gendered_audience,
                            "Audience"
                    ])
                )
            },
            "schema:requiredGender": {
                schema_gender
            }
        }
        if gendered_iri not in statements:
            statements[gendered_iri] = g_statements
        else:
            statements[gendered_iri] = {
                **statements[gendered_iri],
                **g_statements
            }
    return(statements)


def ingest_dsm5(dsm5_xls, behaviors_xls, references_xls, statements={}):
    """
    Function to ingest dsm5 spreadsheet

    Parameters
    ----------
    dsm5_xls: pandas ExcelFile

    behaviors_xls: pandas ExcelFile

    references_xls: pandas ExcelFile

    statements:  dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    >>> try:
    ...     from mhdb.spreadsheet_io import download_google_sheet
    ...     from mhdb.write_ttl import turtle_from_dict
    ... except:
    ...     from mhdb.mhdb.spreadsheet_io import download_google_sheet
    ...     from mhdb.mhdb.write_ttl import turtle_from_dict
    >>> import os
    >>> import pandas as pd
    >>> try:
    ...     dsm5FILE = download_google_sheet(
    ...         'data/dsm5.xlsx',
    ...         "13a0w3ouXq5sFCa0fBsg9xhWx67RGJJJqLjD_Oy1c3b0"
    ...     )
    ...     behaviorsFILE = download_google_sheet(
    ...         'data/behaviors.xlsx',
    ...         "1OHtVRqRXvCUuhyavcLSBU9YkiEJfThFKrXHmcg4627M"
    ...     )
    ...     referencesFILE = download_google_sheet(
    ...         'data/references.xlsx',
    ...         "1KDZhoz9CgHBVclhoOKBgDegUA9Vczui5wj61sXMgh34"
    ...     )
    ... except:
    ...     dsm5FILE = 'data/dsm5.xlsx'
    ...     behaviorsFILE = 'data/behaviors.xlsx'
    ...     referencesFILE = 'data/references.xlsx'
    >>> dsm5_xls = pd.ExcelFile(dsm5FILE)
    >>> behaviors_xls = pd.ExcelFile(behaviorsFILE)
    >>> references_xls = pd.ExcelFile(referencesFILE)
    >>> statements = ingest_dsm5(dsm5_xls, behaviors_xls, references_xls,
    ...     statements={})
    >>> print(turtle_from_dict({
    ...     statement: statements[
    ...         statement
    ...     ] for statement in statements if statement == "mhdb:despair"
    ... }).split("\\n\\t")[0])
    #mhdb:despair rdfs:label "despair"@en ;
    """

    sign_or_symptoms = dsm5_xls.parse("sign_or_symptoms")
    # behaviors = behaviors_xls.parse("behaviors")
    # genders = behaviors_xls.parse("genders")
    references = references_xls.parse("references")

    statements = audience_statements(statements)

    for row in sign_or_symptoms.iterrows():
        sign_or_symptom = "health-lifesci:MedicalSign" \
            if (row[1]["index_sign_or_symptom_index"]) == 1 \
            else "health-lifesci:MedicalSymptom" \
            if (row[1]["index_sign_or_symptom_index"] == 2) \
            else "health-lifesci:MedicalSignOrSymptom"

        source = references[
            references["index"] == row[1]["index_reference"]
        ]["link"].values[0]
        source = None if isinstance(
            source,
            float
        ) else check_iri(source)

        symptom_label = language_string(row[1]["sign_or_symptom"])
        symptom_iri = check_iri(row[1]["sign_or_symptom"])

        for predicates in [
            ("rdfs:label", symptom_label),
            ("rdfs:subClassOf", sign_or_symptom),
            ("dcterms:source", source)
        ]:
            statements = add_if(
                symptom_iri,
                predicates[0],
                predicates[1],
                statements
            )

        # audience_gender = None
        # for x in behaviors.indices_sign_or_symptom:
        #     if type(x) != 'NoneType' and \
        #             row[1]["index"] == x or \
        #             (np.size(x) > 1 and row[1]["index"] in x):
        #         audience_gender = genders.gender[
        #             behaviors.index_gender[row[1]["index"]]
        #         ]
        #         break
        # if audience_gender:
        #     for prop in [
        #         "schema:audience",
        #         "schema:epidemiology"
        #     ]:
        #         statements = add_if(
        #             symptom_iri,
        #             prop,
        #             audience_gender,
        #             statements
        #         )

    return(statements)


def ingest_assessments(assessments_xls, dsm5_xls, behaviors_xls,
                       technologies_xls, references_xls, statements={}):
    """
    Function to ingest assessments spreadsheet

    Parameters
    ----------
    assessments_xls: pandas ExcelFile

    dsm5_xls: pandas ExcelFile

    behaviors_xls: pandas ExcelFile

    technologies_xls: pandas ExcelFile

    references_xls: pandas ExcelFile

    statements:  dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    """

    questions = assessments_xls.parse("questions")
    response_types = assessments_xls.parse("response_types")
    tasks = assessments_xls.parse("tasks")
    presentations = assessments_xls.parse("presentations")
    domains = behaviors_xls.parse("domains")
    domain_categories = behaviors_xls.parse("domain_categories")
    technologies = technologies_xls.parse("technologies")
    software = technologies_xls.parse("software")
    digital_platforms = technologies_xls.parse("digital_platforms")
    disorders = dsm5_xls.parse("disorders")
    references = references_xls.parse("references")

    #statements = audience_statements(statements)

    # questions worksheet
    for row in questions.iterrows():

        source = references[
            references["index"] == row[1]["index_reference"]
        ]["link"].values[0]
        if isinstance(source, float):
            source = check_iri(references[
                references["index"] == row[1]["index_reference"]
                ]["reference"].values[0])
        else:
            source = check_iri(source)

        question_label = language_string(row[1]["question"])
        question_iri = check_iri(row[1]["question"])

        for predicates in [
            ("rdfs:label", question_label),
            ("dcterms:source", source)
        ]:
            statements = add_if(
                question_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        instructions = row[1]["instructions"]
        group_instructions = row[1]["question_group_instructions"]
        response_options = row[1]["response_options"]

        if isinstance(instructions, str):
            predicates_list.append(("rdfs:instructionsBLOOP",
                                    language_string(instructions)))
        if isinstance(group_instructions, str):
            predicates_list.append(("rdfs:grpinstructionsBLOOP",
                                    language_string(group_instructions)))
        if isinstance(response_options, str):
            predicates_list.append(("rdfs:responseoptionsBLOOP",
                                    language_string(response_options)))

        response_type = response_types.response_type[
            response_types["index"] == row[1]["index_response_type"]
        ]
        if isinstance(response_type, str):
            predicates_list.append(("rdfs:responsetypeBLOOP",
                                    language_string(response_type)))

        for predicates in predicates_list:
            statements = add_if(
                question_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # tasks worksheet
    for row in tasks.iterrows():

        task_label = language_string(row[1]["task"])
        task_iri = check_iri(row[1]["task"])

        for predicates in [
            ("rdfs:label", task_label)
        ]:
            statements = add_if(
                task_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        instructions = row[1]["instructions"]
        respond_to_advance = row[1]["respond_to_advance"]
        response_affects_presentation = row[1]["response_affects_presentation"]
        response_based_feedback = row[1]["response_based_feedback"]
        animation = row[1]["animation"]

        if isinstance(instructions, str):
            predicates_list.append(("rdfs:instructionsBLOOP",
                                    language_string(instructions)))
        if respond_to_advance == "1":
            predicates_list.append(("rdfs:respond_to_advanceBLOOP",
                                    language_string(respond_to_advance)))
        if response_affects_presentation == "1":
            predicates_list.append(("rdfs:response_affects_presentationBLOOP",
                                    language_string(response_affects_presentation)))
        if response_based_feedback == "1":
            predicates_list.append(("rdfs:response_based_feedbackBLOOP",
                                    language_string(response_based_feedback)))
        if animation == "1":
            predicates_list.append(("rdfs:animationBLOOP",
                                    language_string(animation)))

        indices_domain = row[1]["indices_domain"]
        indices_domain_category = row[1]["indices_domain_category"]
        indices_software = row[1]["indices_software"]
        indices_task_groups = row[1]["indices_task_groups"]
        indices_presentations = row[1]["indices_presentations"]
        indices_digital_platform = row[1]["indices_digital_platform"]
        indices_claimed_disorder = row[1]["indices_claimed_disorder"]

        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains.domain[domains["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domainBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_domain_category, str):
            indices = [np.int(x) for x in indices_domain_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domain_categories.domain_category[
                    domain_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domain_categoryBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_software, str):
            indices = [np.int(x) for x in indices_software.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = software.software[technologies["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:softwareBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_task_groups, str):
            indices = [np.int(x) for x in indices_task_groups.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = task_groups.task_group[task_groups["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:task_groupBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_presentations, str):
            indices = [np.int(x) for x in indices_presentations.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = presentations.presentation[
                    presentations["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:presentationBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_digital_platform, str):
            indices = [np.int(x) for x in indices_digital_platform.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = digital_platforms.digital_platform[technologies["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:digital_platformBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_claimed_disorder, str):
            indices = [np.int(x) for x in indices_claimed_disorder.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = disorders.disorder[disorders["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:claimed_disorderBLOOP",
                                            language_string(objectRDF)))



        for predicates in predicates_list:
            statements = add_if(
                task_iri,
                predicates[0],
                predicates[1],
                statements
            )

    return(statements)

def ingest_technologies(technologies_xls, dsm5_xls, behaviors_xls, statements={}):
    """
    Function to ingest technologies spreadsheet

    Parameters
    ----------
    technologies_xls: pandas ExcelFile

    dsm5_xls: pandas ExcelFile

    behaviors_xls: pandas ExcelFile

    statements:  dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    """

    technologies = technologies_xls.parse("technologies")
    links = technologies_xls.parse("links")
    technology_types = technologies_xls.parse("technology_types")
    software = technologies_xls.parse("software")
    people = technologies_xls.parse("people")
    sensors = behaviors_xls.parse("sensors")
    measures = behaviors_xls.parse("measures")
    measure_categories = behaviors_xls.parse("measure_categories")
    keywords = behaviors_xls.parse("keywords")
    keywords_categories = behaviors_xls.parse("keywords_categories")
    domains = behaviors_xls.parse("domains")
    domain_categories = behaviors_xls.parse("domain_categories")
    disorders = dsm5_xls.parse("disorders")

    #statements = audience_statements(statements)

    # technologies worksheet
    for row in technologies.iterrows():

        technology_label = row[1]["technology"]

        # follow index to link in separate worksheet
        technology_iri = links[links["index"] == row[1]["index_link"]]
        if isinstance(technology_iri, float):
            technology_iri = check_iri(row[1]["technology"])
        else:
            technology_iri = check_iri(technology_iri)

        for predicates in [
            ("rdfs:label", technology_label)
        ]:
            statements = add_if(
                technology_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        description = row[1]["description"]

        if isinstance(description, str):
            predicates_list.append(("rdfs:descriptionBLOOP",
                                    language_string(description)))

        indices_domain = row[1]["indices_domain"]
        indices_domain_category = row[1]["indices_domain_category"]
        indices_disorder = row[1]["indices_disorder"]
        indices_technology_type = row[1]["indices_technology_type"]
        indices_people = row[1]["indices_people"]
        indices_sensor = row[1]["indices_sensor"]
        indices_measure = row[1]["indices_measure"]
        indices_measure_category = row[1]["indices_measure_category"]
        indices_keywords = row[1]["indices_keywords"]
        indices_keywords_category = row[1]["indices_keywords_category"]

        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in
                       indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains.domain[domains["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domainBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_domain_category, str):
            indices = [np.int(x) for x in
                       indices_domain_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domain_categories.domain_category[
                    domain_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domain_categoryBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_disorder, str):
            indices = [np.int(x) for x in
                       indices_disorder.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = disorders.disorder[disorders["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:claimed_disorderBLOOP",
                                            language_string(objectRDF)))
        # IRI
        if isinstance(indices_technology_type, str):
            indices = [np.int(x) for x in
                       indices_technology_type.strip().split(',') if len(x)>0]
            for index in indices:
                technology_type_label = technology_types.technology_type[technology_types["index"] == index]
                technology_type_iri = technology_types.IRI[technology_types["index"] == index]
                if isinstance(technology_type_iri, float):
                    technology_type_label = technology_types.technology_type[
                        technology_types["index"] == index]
                    technology_type_iri = check_iri(technology_type_label)
                else:
                    technology_type_iri = check_iri(technology_type_iri)
                if isinstance(technology_type_iri, str):
                    predicates_list.append(("rdfs:technology_type_iriBLOOP",
                                            technology_type_iri))
        if isinstance(indices_people, str):
            indices = [np.int(x) for x in
                       indices_people.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = people.people[people["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:peopleBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_sensor, str):
            indices = [np.int(x) for x in
                       indices_sensor.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sensors.sensor[sensors["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:sensorBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_measure, str):
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures.measure[measures["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:measureBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_measure_category, str):
            indices = [np.int(x) for x in
                       indices_measure_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measure_categories.measure_category[measure_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:measure_categoryBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_keywords, str):
            indices = [np.int(x) for x in
                       indices_keywords.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = keywords.keywords[keywords["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:keywordsBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_keywords_category, str):
            indices = [np.int(x) for x in
                       indices_keywords_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = keywords_categories.keywords_category[keywords_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:keywords_categoryBLOOP",
                                            language_string(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                technology_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # software worksheet
    for row in software.iterrows():

        software_label = language_string(row[1]["software"])

        software_iri = row[1]["link"]
        if isinstance(software_iri, float):
            software_iri = check_iri(software_label)
        else:
            software_iri = check_iri(software_iri)

        for predicates in [
            ("rdfs:label", software_label)
        ]:
            statements = add_if(
                software_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        abbreviation = row[1]["abbreviation"]

        if isinstance(abbreviation, str):
            predicates_list.append(("rdfs:abbreviationBLOOP",
                                    language_string(abbreviation)))

        for predicates in predicates_list:
            statements = add_if(
                technology_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # people worksheet
    for pred in [
        ("rdfs:label", language_string("site")),
        ("rdfs:comment", language_string(
            "Site, place or location of anything."
        )),
        ("rdfs:range", "schema:Place"),
        ("rdfs:range", "dcterms:Location"),
        ("rdf:type", "rdf:Property")
    ]:
        statements = add_if(
            "mhdb:site",
            pred[0],
            pred[1],
            statements
        )

    for row in people.iterrows():
        predicates = set()
        person_iri = check_iri(row[1]["link"])
        person_label = language_string(
            row[1]["people"]
        ) if (
            (
                len(str(row[1]["people"]))
            ) and not (
                isinstance(
                    row[1]["people"],
                    float
                )
            ) and not (
                str(row[1]["people"]).startswith("Also")
            )
        ) else None
        person_place = check_iri(row[1]["location"]) if (
            len(
                str(row[1]["location"]).strip()
            ) and not (
                isinstance(
                    row[1]["location"],
                    float
                )
            )
        ) else None

        if person_label:
            predicates.add(
                ("rdfs:label", person_label)
            )

        if person_place:
            predicates.add(
                ("mhdb:site", person_place)
            )
            statements = add_if(
                person_place,
                "rdfs:label",
                language_string(row[1]["location"]),
                statements
            )

        if "<" in person_iri:
            predicates.add(
                ("schema:WebPage", person_iri)
            )

        if len(predicates):
            for prop in predicates:
                statements = add_if(
                    person_iri,
                    prop[0],
                    prop[1],
                    statements
                )

        for affiliate_i in range(1, 10):
            affiliate = "{0}{1}".format(
                "affiliate",
                str(affiliate_i)
            )
            if row[1][affiliate] and len(
                str(row[1][affiliate])
            ) and not isinstance(
                row[1][affiliate],
                float
            ):
                affiliate_iri = check_iri(
                    row[1][affiliate].split("(")[1].rstrip(")")
                ) if (
                    (
                        "@" in row[1][affiliate]
                    ) or (
                        "://" in row[1][affiliate]
                    )
                ) else check_iri(", ".join([
                    " ".join(list(
                        row[1][affiliate].strip().split(
                            "("
                        )[0].split(" ")[1:])).strip(),
                    row[1][affiliate].strip().split(
                        "("
                    )[0].split(" ")[0].strip()
                ])) if "(" in row[1][affiliate] else check_iri(", ".join([
                    " ".join(list(
                        row[1][affiliate].strip().split(" ")[1:])).strip(),
                    row[1][affiliate].strip().split(" ")[0].strip()
                ]))
                affiliate_preds = {
                    (
                        property,
                        language_string(
                            row[1][affiliate].strip().split(
                                "("
                            )[0].strip() if "(" in row[1][
                                affiliate
                            ] else row[1][affiliate]
                        )
                    ) for property in ["rdfs:label", "foaf:name"]
                }
                if "(" in row[1][affiliate]:
                    if "@" in row[1][affiliate]:
                        affiliate_preds.add(
                            (
                                "schema:email",
                                check_iri(row[1][affiliate].split(
                                    "("
                                )[1].rstrip(")").strip())
                            )
                        )
                    elif "://" in row[1][affiliate]:
                        affiliate_webpage = row[1][affiliate].split(
                            "("
                        )[1].rstrip(")").strip()
                        affiliate_preds.add(
                            (
                                "schema:WebPage",
                                check_iri(row[1][affiliate].split(
                                    "("
                                )[1].rstrip(")").strip())
                            )
                        )
                    elif "lab pup" in row[1][affiliate]:
                        affiliate_preds.add(
                            (
                                "rdfs:comment",
                                language_string("lab pup")
                            )
                        )
                    else:
                        affiliate_preds.add(
                            (
                                "mhdb:site",
                                check_iri(
                                    row[1][affiliate].split(
                                        "("
                                    )[1].rstrip(")").strip()
                                )
                            )
                        )

                for pred in affiliate_preds:
                    statements = add_if(
                        affiliate_iri,
                        pred[0],
                        pred[1],
                        statements
                    )

                statements = add_if(
                    person_iri,
                    "dcterms:contributor",
                    affiliate_iri,
                    statements
                )

    return(statements)


def ingest_behaviors(behaviors_xls, technologies_xls, dsm5_xls, statements={}):
    """
    Function to ingest behaviors spreadsheet

    Parameters
    ----------
    behaviors_xls: pandas ExcelFile

    assessments_xls: pandas ExcelFile

    dsm5_xls: pandas ExcelFile

    technologies_xls: pandas ExcelFile

    references_xls: pandas ExcelFile

    statements:  dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    """

    behaviors = behaviors_xls.parse("behaviors")
    behavior_question_prepends = behaviors_xls.parse("behavior_question_prepends")
    genders = behaviors_xls.parse("genders")
    sensors = behaviors_xls.parse("sensors")
    measures = behaviors_xls.parse("measures")
    measure_categories = behaviors_xls.parse("measure_categories")
    locations = behaviors_xls.parse("locations")
    domains = behaviors_xls.parse("domains")
    domain_categories = behaviors_xls.parse("domain_categories")
    domain_types = behaviors_xls.parse("domain_types")
    keywords = behaviors_xls.parse("keywords")
    keywords_categories = behaviors_xls.parse("keywords_categories")
    claims = behaviors_xls.parse("claims")
    technologies = technologies_xls.parse("technologies")
    sign_or_symptoms = dsm5_xls.parse("sign_or_symptoms")

    #statements = audience_statements(statements)

    # behaviors worksheet
    for row in behaviors.iterrows():

        behavior_label = row[1]["behavior"]
        behavior_iri = check_iri(row[1]["behavior"])

        for predicates in [
            ("rdfs:label", behavior_label)
        ]:
            statements = add_if(
                behavior_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        #index_adverb_suffix = row[1]["index_adverb_suffix"]

        adverb_prefix = behavior_question_prepends.question_prepend[
            behavior_question_prepends["index"] == row[1]["index_abverb_prefix"]
        ]
        if isinstance(adverb_prefix, str):
            predicates_list.append(("rdfs:adverb_prefixBLOOP",
                                    language_string(adverb_prefix)))

        gender = genders.gender[
            genders["index"] == row[1]["index_gender"]
        ]
        if isinstance(adverb_prefix, str):
            predicates_list.append(("rdfs:genderBLOOP",
                                    language_string(gender)))

        indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]

        if isinstance(indices_sign_or_symptom, str):
            indices = [np.int(x) for x in
                       indices_sign_or_symptom.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sign_or_symptoms.sign_or_symptom[sign_or_symptoms["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:sign_or_symptomBLOOP",
                                            language_string(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                behavior_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # sensors worksheet
    for row in sensors.iterrows():

        sensor_label = language_string(row[1]["sensor"])
        sensor_iri = check_iri(row[1]["sensor"])

        for predicates in [
            ("rdfs:label", sensor_label)
        ]:
            statements = add_if(
                sensor_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        abbreviation = row[1]["abbreviation"]

        if isinstance(abbreviation, str):
            predicates_list.append(("rdfs:abbreviationBLOOP",
                                    language_string(abbreviation)))

        indices_measure = row[1]["indices_measure"]

        if isinstance(indices_measure, str):
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures.measure[measures["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:measureBLOOP",
                                            language_string(objectRDF)))
        for predicates in predicates_list:
            statements = add_if(
                sensor_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # measures worksheet
    for row in measures.iterrows():

        measure_label = language_string(row[1]["measure"])
        measure_iri = check_iri(row[1]["measure"])

        for predicates in [
            ("rdfs:label", measure_label)
        ]:
            statements = add_if(
                measure_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        indices_measure_category = row[1]["indices_measure_category"]

        if isinstance(indices_measure_category, str):
            indices = [np.int(x) for x in
                       indices_measure_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measure_categories.measure_category[measure_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:measure_categoryBLOOP",
                                            language_string(objectRDF)))

        indices_location = row[1]["indices_location"]

        if isinstance(indices_location, str):
            indices = [np.int(x) for x in
                       indices_location.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measure_categories.location[measure_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:locationBLOOP",
                                            language_string(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                measure_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # locations worksheet
    for row in locations.iterrows():

        location_label = language_string(row[1]["location"])

        location_iri = row[1]["IRI"]
        if isinstance(location_iri, float):
            location_iri = check_iri(location_label)
        else:
            location_iri = check_iri(location_iri)

        for predicates in [
            ("rdfs:label", location_label)
        ]:
            statements = add_if(
                location_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # domains worksheet
    for row in domains.iterrows():

        domain_label = language_string(row[1]["domain"])
        domain_iri = check_iri(row[1]["domain"])

        for predicates in [
            ("rdfs:label", domain_label)
        ]:
            statements = add_if(
                domain_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        index_domain_type = row[1]["index_domain_type"]

        if isinstance(index_domain_type, str):
            domain_type = domain_types.domain_type[domain_types["index"] == index]
            predicates_list.append(("rdfs:domain_typeBLOOP",
                                    language_string(domain_type)))

        indices_domain_category = row[1]["indices_domain_category"]

        if isinstance(indices_domain_category, str):
            indices = [np.int(x) for x in
                       indices_domain_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domain_categories.domain_category[domain_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domain_categoryBLOOP",
                                            language_string(objectRDF)))
        for predicates in predicates_list:
            statements = add_if(
                domain_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # domain_categories worksheet
    for row in domain_categories.iterrows():

        domain_category_label = language_string(row[1]["domain_category"])
        domain_category_iri = check_iri(row[1]["domain_category"])

        for predicates in [
            ("rdfs:label", domain_category_label)
        ]:
            statements = add_if(
                domain_category_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        index_domain_type = row[1]["index_domain_type"]

        if isinstance(index_domain_type, str):
            domain_type = domain_types.domain_type[domain_types["index"] == index]
            predicates_list.append(("rdfs:domain_typeBLOOP",
                                    language_string(domain_type)))

        for predicates in predicates_list:
            statements = add_if(
                domain_category_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # keywords worksheet
    for row in keywords.iterrows():

        keyword_label = language_string(row[1]["keywords"])
        keyword_iri = check_iri(row[1]["keywords"])

        for predicates in [
            ("rdfs:label", keyword_label)
        ]:
            statements = add_if(
                keyword_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        index_domain_type = row[1]["index_domain_type"]

        if isinstance(index_domain_type, str):
            domain_type = domain_types.domain_type[domain_types["index"] == index]
            predicates_list.append(("rdfs:domain_typeBLOOP",
                                    language_string(domain_type)))

        indices_domain_category = row[1]["indices_keywords_category"]

        if isinstance(indices_domain_category, str):
            indices = [np.int(x) for x in
                       indices_domain_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domain_categories.domain_category[domain_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domain_categoryBLOOP",
                                            language_string(objectRDF)))

        indices_keyword_category = row[1]["indices_keywords_category"]

        if isinstance(indices_keyword_category, str):
            indices = [np.int(x) for x in
                       indices_keyword_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = keywords_categories.keywords_category[keywords_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:keyword_categoryBLOOP",
                                            language_string(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                keyword_iri,
                predicates[0],
                predicates[1],
                statements
            )

    # claims worksheet
    for row in claims.iterrows():

        claim_label = language_string(row[1]["claim"])
        claim_iri = check_iri(row[1]["claim"])

        for predicates in [
            ("rdfs:label", claim_label)
        ]:
            statements = add_if(
                claim_iri,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        indices_domain = row[1]["indices_domain"]

        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in
                       indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains.domain[domains["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domainBLOOP",
                                            language_string(objectRDF)))

        indices_domain_category = row[1]["indices_domain_category"]

        if isinstance(indices_domain_category, str):
            indices = [np.int(x) for x in
                       indices_domain_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domain_categories.domain_category[domain_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domain_categoryBLOOP",
                                            language_string(objectRDF)))

        indices_measure = row[1]["indices_measure"]

        if isinstance(indices_measure, str):
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures.measure[measures["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:measureBLOOP",
                                            language_string(objectRDF)))

        indices_measure_category = row[1]["indices_measure_category"]

        if isinstance(indices_measure_category, str):
            indices = [np.int(x) for x in
                       indices_measure_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measure_categories.measure_category[measure_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:measure_categoryBLOOP",
                                            language_string(objectRDF)))

        indices_sensor = row[1]["indices_sensor"]

        if isinstance(indices_sensor, str):
            indices = [np.int(x) for x in
                       indices_sensor.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sensors.sensor[sensors["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:sensorBLOOP",
                                            language_string(objectRDF)))

        indices_location = row[1]["indices_location"]

        if isinstance(indices_location, str):
            indices = [np.int(x) for x in
                       indices_location.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = locations.location[locations["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:locationBLOOP",
                                            language_string(objectRDF)))

        indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]

        if isinstance(indices_sign_or_symptom, str):
            indices = [np.int(x) for x in
                       indices_sign_or_symptom.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = signs_or_symptoms.sign_or_symptom[signs_or_symptoms["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:sign_or_symptomBLOOP",
                                            language_string(objectRDF)))

        indices_technology = row[1]["indices_technology"]

        if isinstance(indices_technology, str):
            indices = [np.int(x) for x in
                       indices_technology.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = technologies.technology[technologies["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:technologyBLOOP",
                                            language_string(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                claim_iri,
                predicates[0],
                predicates[1],
                statements
            )

    return(statements)


def ingest_references(references_xls, behaviors_xls, statements={}):
    """
    Function to ingest references spreadsheet

    Parameters
    ----------
    assessments_xls: pandas ExcelFile

    dsm5_xls: pandas ExcelFile

    behaviors_xls: pandas ExcelFile

    technologies_xls: pandas ExcelFile

    references_xls: pandas ExcelFile

    statements:  dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    """

    references = references_xls.parse("references")
    reference_types = references_xls.parse("reference_types")
    domains = behaviors_xls.parse("domains")
    domain_categories = behaviors_xls.parse("domain_categories")
    age_groups = behaviors_xls.parse("age_groups")
    keywords = behaviors_xls.parse("keywords")
    informants = behaviors_xls.parse("informants")

    #statements = audience_statements(statements)

    # references worksheet
    for row in references.iterrows():

        reference_label = language_string(row[1]["reference"])

        source = row[1]["link"]
        if isinstance(source, float):
            source = check_iri(row[1]["reference"])
        else:
            source = check_iri(source)

        for predicates in [
            ("rdfs:label", reference_label)
        ]:
            statements = add_if(
                source,
                predicates[0],
                predicates[1],
                statements
            )

        predicates_list = []

        abbreviation = row[1]["abbreviation"]
        description = row[1]["description"]
        cited_reference = row[1]["cited_reference"]
        number_of_questions = row[1]["number_of_questions"]
        minutes_to_complete = row[1]["minutes_to_complete"]

        if isinstance(abbreviation, str):
            predicates_list.append(("rdfs:abbreviationBLOOP",
                                    language_string(abbreviation)))
        if isinstance(description, str):
            predicates_list.append(("rdfs:descriptionBLOOP",
                                    language_string(description)))
        if isinstance(cited_reference, str):
            predicates_list.append(("rdfs:cited_referenceBLOOP",
                                    language_string(cited_reference)))
        if isinstance(number_of_questions, str):
            predicates_list.append(("rdfs:number_of_questionsBLOOP",
                                    language_string(number_of_questions)))
        if isinstance(minutes_to_complete, str):
            predicates_list.append(("rdfs:minutes_to_completeBLOOP",
                                    language_string(minutes_to_complete)))

        index_reference_type = row[1]["index_reference_type"]
        age_group_index = row[1]["age_group_index"]

        if isinstance(index_reference_type, str):
            objectRDF = reference_types.reference_type[
                reference_types["index"] == index_reference_type]
            if isinstance(objectRDF, str):
                predicates_list.append(("rdfs:reference_typeBLOOP",
                                        language_string(objectRDF)))
        if isinstance(age_group_index, str):
            objectRDF = age_groups.age_group[
                age_group_index["index"] == age_group_index]
            if isinstance(objectRDF, str):
                predicates_list.append(("rdfs:age_group_indexBLOOP",
                                        language_string(objectRDF)))

        indices_study_or_clinic = row[1]["indices_study_or_clinic"]
        indices_domain = row[1]["indices_domain"]
        indices_domain_category = row[1]["indices_domain_category"]
        indices_keywords = row[1]["indices_keywords"]
        indices_age_group = row[1]["indices_age_group"]
        indices_informants = row[1]["indices_informants"]

        if isinstance(indices_study_or_clinic, str):
            indices = [np.int(x) for x in
                       indices_study_or_clinic.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = studies_or_clinics.study_or_clinic[
                    studies_or_clinics["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:study_or_clinicBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in
                       indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains.domain[domains["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domainBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_domain_category, str):
            indices = [np.int(x) for x in
                       indices_domain_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domain_categories.domain_category[
                    domain_categories["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domain_categoryBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_keywords, str):
            indices = [np.int(x) for x in
                       indices_keywords.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = keywords.keywords[keywords["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:keywordsBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_age_group, str):
            indices = [np.int(x) for x in
                       indices_age_group.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = age_groups.age_group[age_groups["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:age_groupBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_informants, str):
            indices = [np.int(x) for x in
                       indices_informants.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = informants.informant[informants["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:informantsBLOOP",
                                            language_string(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                source,
                predicates[0],
                predicates[1],
                statements
            )

    return(statements)


def TODO_disorder_iri(
        index,
        dsm5_xls=None,
        pre_specifiers_indices=[
            6,
            7,
            24,
            25,
            26
        ],
        post_specifiers_indices=[
            27,
            28,
            56,
            78
        ]
    ):
    """
    Function to figure out IRIs for disorders based on
    mentalhealth.xls::Disorder

    Parameters
    ----------
    index: int
        key to lookup in Disorder table

    dsm5_xls: spreadsheet workbook, optional
        1MfW9yDw7e8MLlWWSBBXQAC2Q4SDiFiMMb7mRtr7y97Q

    pre_specifiers_indices: [int], optional
        list of indices of diagnostic specifiers to precede disorder names

    post_specifiers_indices: [int], optional
        list of indices of diagnostic specifiers to be preceded by disorder
        names

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects
    """
    disorder = dsm5_xls.parse("Disorder")
    severity = dsm5_xls.parse("DisorderSeverity")
    specifier = dsm5_xls.parse("DiagnosticSpecifier")
    criterion = dsm5_xls.parse("DiagnosticCriterion")
    disorderSeries = disorder[disorder["index"]==index]
    disorder_name = disorderSeries["DisorderName"].values[0]
    if (
        not isinstance(
            disorderSeries["DiagnosticSpecifier_index"].values[0],
            float
        )
    ) or (
        not np.isnan(
            disorderSeries["DiagnosticSpecifier_index"].values[0]
        )
    ):
        disorder_name = " ".join([
            specifier[
                specifier[
                    "index"
                ]==disorderSeries[
                    "DiagnosticSpecifier_index"
                ].values[0]
            ]["DiagnosticSpecifierName"].values[0],
            disorder_name
        ]) if disorderSeries[
            "DiagnosticSpecifier_index"
        ].values[0] in pre_specifiers_indices else " ".join([
            disorder_name,
            specifier[
                specifier[
                    "index"
                ]==disorderSeries[
                    "DiagnosticSpecifier_index"
                ].values[0]
            ]["DiagnosticSpecifierName"].values[0]
        ]) if disorderSeries[
            "DiagnosticSpecifier_index"
        ].values[0] in post_specifiers_indices else ", ".join([
            disorder_name,
            specifier[
                specifier[
                    "index"
                ]==disorderSeries[
                    "DiagnosticSpecifier_index"
                ].values[0]
            ]["DiagnosticSpecifierName"].values[0]
        ])
    disorder_name = " with ".join([
        disorder_name,
        criterion[
            criterion["index"]==disorderSeries[
                "DiagnosticInclusionCriterion_index"
            ]
        ]["DiagnosticCriterionName"].values[0]
    ]) if (
        not isinstance(
            disorderSeries["DiagnosticInclusionCriterion_index"].values[0],
            float
        )
    ) or (
        not np.isnan(
            disorderSeries["DiagnosticInclusionCriterion_index"].values[0]
        )
    ) else disorder_name
    disorder_name = " and ".join([
        disorder_name,
        criterion[
            criterion["index"]==disorderSeries[
                "DiagnosticInclusionCriterion2_index"
            ]
        ]["DiagnosticCriterionName"].values[0]
    ]) if (
        not isinstance(
            disorderSeries["DiagnosticInclusionCriterion2_index"].values[0],
            float
        )
    ) or (
        not np.isnan(
            disorderSeries["DiagnosticInclusionCriterion2_index"].values[0]
        )
    ) else disorder_name
    disorder_name = " without ".join([
        disorder_name,
        criterion[
            criterion["index"]==disorderSeries[
                "DiagnosticExclusionCriterion_index"
            ]
        ]["DiagnosticCriterionName"].values[0]
    ]) if (
        not isinstance(
            disorderSeries["DiagnosticExclusionCriterion_index"].values[0],
            float
        )
    ) or (
        not np.isnan(
            disorderSeries["DiagnosticExclusionCriterion_index"].values[0]
        )
    ) else disorder_name
    disorder_name = " and ".join([
        disorder_name,
        criterion[
            criterion["index"]==disorderSeries[
                "DiagnosticExclusionCriterion2_index"
            ]
        ]["DiagnosticCriterionName"].values[0]
    ]) if (
        not isinstance(
            disorderSeries["DiagnosticExclusionCriterion2_index"].values[0],
            float
        )
    ) or (
        not np.isnan(
            disorderSeries["DiagnosticExclusionCriterion2_index"].values[0]
        )
    ) else disorder_name
    disorder_name = " ".join([
        severity[
            severity[
                "index"
            ]==int(disorderSeries[
                "DisorderSeverity_index"
            ])
        ]["DisorderSeverityName"].values[0],
        disorder_name
    ]) if (
        not isinstance(
            disorderSeries[
                "DisorderSeverity_index"
            ].values[0],
            float
        )
    ) or (
        not np.isnan(
            disorderSeries[
                "DisorderSeverity_index"
            ].values[0]
        )
    ) else disorder_name
    iri = check_iri(disorder_name)
    label = language_string(disorder_name)
    statements = {iri: {"rdfs:label": [label]}}
    return(statements)


def TODO_Project(domains_xls, dsm5_xls=None, statements={}
):
    '''
    Function to ingest 1cuJXT1Un7HPLYcDyHAXprH-wGS1azuUNmVQnb3dV1cY Project

    Parameters
    ----------
    sheet: spreadsheet workbook
        1cuJXT1Un7HPLYcDyHAXprH-wGS1azuUNmVQnb3dV1cY

    dsm5_xls: spreadsheet workbook, optional
        1MfW9yDw7e8MLlWWSBBXQAC2Q4SDiFiMMb7mRtr7y97Q

    statements:  dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Returns
    -------
    statements: dictionary
        key: string
            RDF subject
        value: dictionary
            key: string
                RDF predicate
            value: {string}
                set of RDF objects

    Example
    -------
    # TODO
    '''

    def doi_iri(doi, title=None, statements={}
                ):
        """
        Function to create relevant statements about a DOI.

        Parameters
        ----------
        doi: string
            Digital Object Identifier

        title: string, optional
            title of digital object

        Returns
        -------
        statements: dictionary
            key: string
                RDF subject
            value: dictionary
                key: string
                    RDF predicate
                value: {string}
                    set of RDF objects

        Example
        -------
        >>> print([k for k in doi_iri(
        ...     "10.1109/IEEESTD.2015.7084073",
        ...     "1872-2015 - IEEE Standard Ontologies for Robotics and Automation"
        ... )][0])
        <https://dx.doi.org/10.1109/IEEESTD.2015.7084073>
        """
        local_iri = check_iri(
            'https://dx.doi.org/{0}'.format(
                doi
            )
        )
        doi = '"""{0}"""^^rdfs:Literal'.format(doi)
        for pred in [
            ("datacite:usesIdentifierScheme", "datacite:doi"),
            ("datacite:hasIdentifier", doi)
        ]:
            statements = add_if(
                local_iri,
                pred[0],
                pred[1],
                statements
            )
        return (
            add_if(
                local_iri,
                "rdfs:label",
                language_string(
                    title
                ),
                statements
            ) if title else statements
        )

    def object_split_lookup(object_indices, lookup_sheet, lookup_key_column,
                            lookup_value_column, separator=","
                            ):
        """
        Function to lookup values from comma-separated key columns.

        Parameters
        ----------
        object_indices: string
            maybe-separated string of foreign keys

        lookup_sheet: DataFrame
            foreign table

        lookup_key_column: string
            foreign table key column header

        lookup_value_column: string
            foreign table value column header

        separator: string
            default=","

        Returns
        -------
        object_iris: list of strings
            list of Turtle-formatted IRIs or empty list if none

        Example
        -------
        >>> import pandas as pd
        >>> sheet = pd.DataFrame({
        ...     "index": list(range(3)),
        ...     "bird": [":duck", ":goose", ":swan"]
        ... })
        >>> print(object_split_lookup(
        ...     object_indices="0/2",
        ...     lookup_sheet=sheet,
        ...     lookup_key_column="index",
        ...     lookup_value_column="bird",
        ...     separator="/"
        ... ))
        [':duck', ':swan']
        """
        try:
            if not isinstance(
                    object_indices,
                    float
            ) and len(str(object_indices).strip()):
                object_indices = str(object_indices)
                if separator not in object_indices:
                    object_iris = [check_iri(
                        lookup_sheet[
                            lookup_sheet[
                                lookup_key_column
                            ] == int(
                                object_indices
                            )
                            ][lookup_value_column].values[0]
                    )] if lookup_sheet[
                        lookup_sheet[
                            lookup_key_column
                        ] == int(
                            object_indices
                        )
                        ][lookup_value_column].values.size else None
                else:
                    object_iris = [
                        int(
                            s.strip()
                        ) for s in object_indices.split(
                            separator
                        )
                    ]
                    object_iris = [check_iri(
                        lookup_sheet[
                            lookup_sheet[lookup_key_column] == object_i
                            ][lookup_value_column].values[0]
                    ) for object_i in object_iris]
                return (object_iris)
            else:
                return ([])
        except:
            print(str(lookup_value_column))
            print(str(object_indices))
            return ([])

    for subject in [
        "schema:Book",
        "schema:Article"
    ]:
        statements = add_if(
            subject,
            "rdfs:subClassOf",
            "mhdb:BookOrArticle",
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "schema:CreativeWork"),
        ("rdfs:subClassOf", "dcterms:BibliographicResource"),
        ("rdfs:label", language_string("Book / Article"))
    ]:
        statements = add_if(
            "mhdb:BookOrArticle",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "schema:CreativeWork"),
        ("rdfs:subClassOf", "schema:MedicalTest"),
        ("rdfs:label", language_string("Assessment"))
    ]:
        statements = add_if(
            "mhdb:Assessment",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "schema:CreativeWork"),
        ("rdfs:subClassOf", "dcterms:InteractiveResource"),
        ("rdfs:label", language_string("Virtual Reality"))
    ]:
        statements = add_if(
            "mhdb:VirtualReality",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "schema:CreativeWork"),
        ("rdfs:subClassOf", "dcterms:InteractiveResource"),
        ("rdfs:label", language_string("Augmented Reality"))
    ]:
        statements = add_if(
            "mhdb:AugmentedReality",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "schema:Book"),
        ("rdfs:label", language_string("Resource Guide"))
    ]:
        statements = add_if(
            "mhdb:ResourceGuide",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "schema:Service"),
        ("rdfs:subClassOf", "schema:OrganizeAction"),
        ("rdfs:label", language_string("Community Initiative"))
    ]:
        statements = add_if(
            "mhdb:CommunityInitiative",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "ssn:Device"),
        ("rdfs:comment", language_string(
            "A smart electronic device (electronic device with "
            "micro-controller(s)) that can be worn on the body as implants or "
            "accessories."
        )),
        ("rdfs:isDefinedBy", check_iri(
            "https://en.wikipedia.org/wiki/Wearable_domains"
        )),
        ("rdfs:label", language_string("Wearable"))
    ]:
        statements = add_if(
            "mhdb:Wearable",
            pred[0],
            pred[1],
            statements
        )

        for pred in [
            ("rdfs:subClassOf", "ssn:Device"),
            ("rdfs:label", language_string("Tablet"))
        ]:
            statements = add_if(
                "mhdb:Tablet",
                pred[0],
                pred[1],
                statements
            )

    for pred in [
        ("rdfs:subClassOf", "schema:Game"),
        ("owl:disjointWith", "schema:VideoGame"),
        ("rdfs:label", language_string("Non-Digital Game"))
    ]:
        statements = add_if(
            "mhdb:NonDigitalGame",
            pred[0],
            pred[1],
            statements
        )

    statements = {
        **doi_iri(
            "10.1109/IEEESTD.2015.7084073",
            "1872-2015 - IEEE Standard Ontologies for Robotics and Automation"
        ),
        **statements
    }

    for pred in [
        ("rdfs:subClassOf", "dcterms:Agent"),
        ("rdfs:subClassOf", "ssn:Device"),
        (
            "dcterms:source",
            check_iri(
                'https://dx.doi.org/10.1109/IEEESTD.2015.7084073'
            )
        ),
        ("rdfs:label", language_string("Robot")),
        (
            "rdfs:comment",
            language_string(
                "An agentive device (Agent and Device in SUMO) in a broad "
                "sense, purposed to act in the physical world in order to "
                "accomplish one or more tasks. In some cases, the actions of a "
                "robot might be subordinated to actions of other agents (Agent "
                "in SUMO), such as software agents (bots) or humans. A robot "
                "is composed of suitable mechanical and electronic parts. "
                "Robots might form social groups, where they interact to "
                "achieve a common goal. A robot (or a group of robots) can "
                "form robotic systems together with special environments "
                "geared to facilitate their work."
            )
        )
    ]:
        statements = add_if(
            "mhdb:Robot",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "schema:CreativeWork"),
        (
            "dcterms:source",
            check_iri(
                "http://afirm.fpg.unc.edu/social-narratives"
            )
        ),
        (
            "rdfs:isDefinedBy",
            check_iri(
                "http://afirm.fpg.unc.edu/social-narratives"
            )
        ),
        (
            "rdfs:comment",
            language_string(
                "Social narratives (SN) describe social situations for "
                "learners by providing relevant cues, explanation of the "
                "feelings and thoughts of others, and descriptions of "
                "appropriate behavior expectations."
            )
        ),
        ("rdfs:label", language_string("Social Narrative"))
    ]:
        statements = add_if(
            "mhdb:SocialNarrative",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:label", language_string("Ann M. Sam")),
        ("foaf:name", language_string("Ann M. Sam")),
        ("foaf:familyName", language_string("Sam")),
        ("foaf:givenName", language_string("Ann")),
        ("rdfs:type", "foaf:Person"),
        ("rdfs:site", "mhdb:University_of_North_Carolina_at_Chapel_Hill")
    ]:
        statements = add_if(
            check_iri("http://fpg.unc.edu/profiles/ann-m-sam"),
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:label", language_string("AFIRM Team")),
        ("foaf:name", language_string("AFIRM Team")),
        ("rdfs:type", "foaf:Organization"),
        ("rdfs:site", "mhdb:University_of_North_Carolina_at_Chapel_Hill")
    ]:
        statements = add_if(
            check_iri("AFIRM Team"),
            pred[0],
            pred[1],
            statements
        )

    for contributor in [
        check_iri("http://fpg.unc.edu/profiles/ann-m-sam"),
        check_iri("AFIRM Team")
    ]:
        statements = add_if(
            check_iri(
                "http://afirm.fpg.unc.edu/social-narratives"
            ),
            "dcterms:contributor",
            contributor,
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "mhdb:SocialNarrative"),
        ("rdfs:subClassOf", "schema:Game"),
        (
            "rdfs:label",
            language_string(
                "Combination of a Social Narrative and Gaming System"
            )
        )
    ]:
        statements = add_if(
            "mhdb:SocialNarrativeGamingSystem",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:subClassOf", "sio:SIO_001066"),
        ("schema:participant", "schema:ParentAudience")
    ]:
        statements = add_if(
            "mhdb:StudyWithParents",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:label", language_string("Competition")),
        ("rdfs:subClassOf", "schema:Event")
    ]:
        statements = add_if(
            "mhdb:Competition",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:label", language_string("Science Contest")),
        ("rdfs:subClassOf", "mhdb:Competition")
    ]:
        statements = add_if(
            "mhdb:ScienceContest",
            pred[0],
            pred[1],
            statements
        )

    for pred in [
        ("rdfs:label", language_string("Massive Open Online Course")),
        ("rdfs:subClassOf", "schema:Course")
    ]:
        statements = add_if(
            "mhdb:MOOC",
            pred[0],
            pred[1],
            statements
        )

    #TODO: define Toy, StudentProject, Hackathon, OutreachProgram, SupportGroup

    project = domains_xls.parse("Project", convert_float=False)
    homepage = domains_xls.parse("HomePageLink")
    type_of_project = domains_xls.parse("TypeOfProject")
    mhealthpeople = domains_xls.parse("MHealthPeople")
    research_study = domains_xls.parse("ResearchStudyOnProject")

    for row in project.iterrows():
        if isinstance(
            row[1]["project"],
            float
        ) and np.isnan(row[1]["project"]):
            continue
        project_iri = check_iri(row[1]["project"])
        project_label = language_string(row[1]["project"])

        disorder_iris = [
            int(
                disorder_index.strip()
            ) for disorder_index in row[1][
                "Disorder_index"
            ].split(",")
        ] if (
            (
                isinstance(
                    row[1][
                        "Disorder_index"
                    ],
                    str
                )
            ) and (
                "," in row[1][
                    "Disorder_index"
                ]
            )
        ) else [
            int(row[1][
                "Disorder_index"
            ])
        ] if (
            not isinstance(
                row[1]["Disorder_index"],
                float
            ) or (
                not np.isnan(
                    row[1][
                        "Disorder_index"
                    ]
                )
            )
        ) else None

        homepage_iris = object_split_lookup(
            row[1]["HomePageLink_index"],
            homepage,
            "index",
            "HomePageLink",
            ","
        )

        type_of_project_iris = object_split_lookup(
            row[1]["TypeOfProject_index"],
            type_of_project,
            "index",
            "IRI",
            ","
        )

        mhealthpeople_iris = object_split_lookup(
            row[1]["MHealthPeople_index"],
            mhealthpeople,
            "index",
            "URL",
            ","
        )

        study_iris = object_split_lookup(
            row[1]["ResearchStudyOnProjectLink_index"],
            research_study,
            "index",
            "ResearchStudyOnProjectLink",
            ","
        )
        disorder_statements = {}
        if disorder_iris and len(disorder_iris):
            for disorder in disorder_iris:
                disorder_statements = disorder_iri(
                    disorder,
                    dsm5_xls=dsm5_xls,
                    pre_specifiers_indices=[
                        6,
                        7,
                        24,
                        25,
                        26
                    ],
                    post_specifiers_indices=[
                        27,
                        28,
                        56,
                        78
                    ]
                )
                statements = add_if(
                    project_iri,
                    "dcterms:subject",
                    [
                        k for k in disorder_statements
                    ][0],
                    {
                        **statements,
                        **disorder_statements
                    }
                )

        if homepage_iris and len(homepage_iris):
            for homepage_iri in homepage_iris:
                for prop in [
                    ("schema:about", project_iri),
                    ("rdf:type", "schema:WebPage")
                ]:
                    statements = add_if(
                        homepage_iri,
                        prop[0],
                        prop[1],
                        statements
                    )

        if type_of_project_iris and len(type_of_project_iris):
            for type_of_project_iri in type_of_project_iris:
                statements = add_if(
                    project_iri,
                    "rdf:type",
                    type_of_project_iri,
                    statements
                )

        if mhealthpeople_iris and len(mhealthpeople_iris):
            for mhealthpeople_iri in mhealthpeople_iris:
                for prop in [
                    ("dcterms:contributor", mhealthpeople_iri)
                ]:
                    statements = add_if(
                        project_iri,
                        prop[0],
                        prop[1],
                        statements
                    )

        if study_iris and len(study_iris):
            for study_iri in study_iris:
                for prop in [
                    ("schema:about", project_iri),
                    ("rdf:type", "schema:ScholarlyArticle")
                ]:
                    statements = add_if(
                        study_iri,
                        prop[0],
                        prop[1],
                        statements
                    )

        for prop in [
            ("rdfs:label", project_label),
            ("rdfs:subClassOf", "schema:Product")
        ]:
            statements = add_if(
                project_iri,
                prop[0],
                prop[1],
                statements
            )

    return(statements)
