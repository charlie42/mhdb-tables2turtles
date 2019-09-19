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
    from mhdb.spreadsheet_io import download_google_sheet
    from mhdb.write_ttl import check_iri, mhdb_iri_simple, language_string
except:
    from mhdb.mhdb.spreadsheet_io import download_google_sheet
    from mhdb.mhdb.write_ttl import check_iri, mhdb_iri_simple, language_string
import numpy as np
import pandas as pd
import re

emptyValue = 'EmptyValue'
exclude_list = [emptyValue, '', [], 'NaN', 'NAN', 'nan', np.nan, None]


def add_to_statements(subject, predicate, object, statements={},
                      exclude_list=exclude_list):
    """
    Function to add predicate and object to a dictionary, after checking predicate.

    Parameters
    ----------
    subject: string
    predicate: string
    object: string
    statements: dictionary
    exclude_list: list
        do not add statement if it contains any of these

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
    >>> print(add_to_statements(":goose", ":chases", ":it"))
    {':goose': {':chases': {':it'}}}
    """
    if subject not in exclude_list and \
        predicate not in exclude_list and \
        object not in exclude_list:
        subject.strip()
        predicate.strip()
        object.strip()

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

    return statements


def ingest_states(states_xls, references_xls, statements={}):
    """
    Function to ingest states spreadsheet

    Parameters
    ----------
    states_xls: pandas ExcelFile

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

    # load worksheets as pandas dataframes
    state_classes = states_xls.parse("Classes")
    state_properties = states_xls.parse("Properties")
    states = states_xls.parse("states")
    state_types = states_xls.parse("state_types")
    references = references_xls.parse("references")

    # fill NANs with emptyValue
    state_classes = state_classes.fillna(emptyValue)
    state_properties = state_properties.fillna(emptyValue)
    states = states.fillna(emptyValue)
    state_types = state_types.fillna(emptyValue)
    references = references.fillna(emptyValue)

    statements = audience_statements(statements)

    # Classes worksheet
    for row in state_classes.iterrows():

        state_class_label = language_string(row[1]["ClassName"])
        state_class_iri = mhdb_iri_simple(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", state_class_label))
        predicates_list.append(("a", "rdf:Class"))

        if row[1]["DefinitionReference_index"] not in exclude_list:
            source = references[references["index"] ==
                np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
            if source not in exclude_list:
                source = check_iri(source)
                #predicates_list.append(("dcterms:isReferencedBy", source))
                predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    mhdb_iri_simple(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                state_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in state_properties.iterrows():

        state_property_label = language_string(row[1]["property"])
        state_property_iri = mhdb_iri_simple(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", state_property_label))
        predicates_list.append(("a", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:state",
                                    mhdb_iri_simple(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    mhdb_iri_simple(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list:
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    mhdb_iri_simple(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    mhdb_iri_simple(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                state_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # states worksheet
    for row in states.iterrows():

        state_label = language_string(row[1]["state"])
        state_iri = check_iri(row[1]["state"])

        predicates_list = []
        predicates_list.append(("a", "m3-lite:DomainOfInterest"))
        predicates_list.append(("rdfs:label", state_label))

        indices_state_type = row[1]["indices_state_type"]
        if indices_state_type not in exclude_list:
            indices = [np.int(x) for x in
                       indices_state_type.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = state_types[state_types["index"] ==
                                         index]["state_type"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasDomainType",
                                            check_iri(objectRDF)))
        indices_state_category = row[1]["indices_state_category"]
        if indices_state_category not in exclude_list:
            indices = [np.int(x) for x in
                       indices_state_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = states[states["index"] ==
                                         index]["state"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF)))

        for predicates in predicates_list:
            statements = add_to_statements(
                state_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # state_types worksheet
    for row in state_types.iterrows():

        state_type_label = language_string(row[1]["state_type"])
        state_type_iri = check_iri(row[1]["state_type"])

        predicates_list = []
        predicates_list.append(("a", "mhdb:DomainType"))
        predicates_list.append(("rdfs:label", state_type_label))

        for predicates in predicates_list:
            statements = add_to_statements(
                state_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_measures(measures_xls, references_xls, statements={}):
    """
    Function to ingest measures spreadsheet

    Parameters
    ----------
    measures_xls: pandas ExcelFile

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

    # load worksheets as pandas dataframes
    measure_classes = measures_xls.parse("Classes")
    measure_properties = measures_xls.parse("Properties")
    sensors = measures_xls.parse("sensors")
    measures = measures_xls.parse("measures")
    references = references_xls.parse("references")

    # fill NANs with emptyValue
    measure_classes = measure_classes.fillna(emptyValue)
    measure_properties = measure_properties.fillna(emptyValue)
    sensors = sensors.fillna(emptyValue)
    measures = measures.fillna(emptyValue)
    references = references.fillna(emptyValue)

    # Classes worksheet
    for row in measure_classes.iterrows():

        measure_class_label = language_string(row[1]["ClassName"])
        measure_class_iri = mhdb_iri_simple(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", measure_class_label))
        predicates_list.append(("a", "rdf:Class"))

        if row[1]["DefinitionReference_index"] not in exclude_list:
            source = references[references["index"] ==
                np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
            if source not in exclude_list:
                source = check_iri(source)
                #predicates_list.append(("dcterms:isReferencedBy", source))
                predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    mhdb_iri_simple(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                measure_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in measure_properties.iterrows():

        measure_property_label = language_string(row[1]["property"])
        measure_property_iri = mhdb_iri_simple(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", measure_property_label))
        predicates_list.append(("a", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:state",
                                    mhdb_iri_simple(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    mhdb_iri_simple(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list:
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    mhdb_iri_simple(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    mhdb_iri_simple(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                measure_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # sensors worksheet
    for row in sensors.iterrows():

        sensor_label = language_string(row[1]["sensor"])
        sensor_iri = check_iri(row[1]["sensor"])

        predicates_list = []
        predicates_list.append(("a", "ssn:SensingDevice"))
        predicates_list.append(("rdfs:label", sensor_label))

        if row[1]["abbreviation"] not in exclude_list:
            predicates_list.append(("dbpedia-owl:abbreviation",
                                    check_iri(row[1]["abbreviation"])))

        indices_measure = row[1]["indices_measure"]
        if indices_measure not in exclude_list:
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures.measure[measures["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("ssn:observes",
                                            check_iri(objectRDF)))
        for predicates in predicates_list:
            statements = add_to_statements(
                sensor_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # measures worksheet
    for row in measures.iterrows():

        measure_label = language_string(row[1]["measure"])
        measure_iri = check_iri(row[1]["measure"])

        predicates_list = []
        predicates_list.append(("a", "m3-lite:MeasurementType"))
        predicates_list.append(("rdfs:label", measure_label))

        indices_measure_category = row[1]["indices_measure_category"]
        if indices_measure_category not in exclude_list:
            indices = [np.int(x) for x in
                       indices_measure_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures[measures["index"] ==
                                     index]["measure"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF)))

        for predicates in predicates_list:
            statements = add_to_statements(
                measure_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_tasks(tasks_xls, states_xls, projects_xls, references_xls,
                 statements={}):
    """
    Function to ingest tasks spreadsheet

    Parameters
    ----------
    tasks_xls: pandas ExcelFile

    states_xls: pandas ExcelFile

    projects_xls: pandas ExcelFile

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

    # load worksheets as pandas dataframes
    task_classes = tasks_xls.parse("Classes")
    task_properties = tasks_xls.parse("Properties")
    tasks = tasks_xls.parse("tasks")
    implementations = tasks_xls.parse("implementations")
    indicators = tasks_xls.parse("indicators")
    contrasts = tasks_xls.parse("contrasts")
    conditions = tasks_xls.parse("conditions")
    assertions = tasks_xls.parse("assertions")
    relationships = tasks_xls.parse("relationships")
    states = states_xls.parse("states")
    projects = projects_xls.parse("projects")
    references = references_xls.parse("references")

    # fill NANs with emptyValue
    task_classes = task_classes.fillna(emptyValue)
    task_properties = task_properties.fillna(emptyValue)
    tasks = tasks.fillna(emptyValue)
    implementations = implementations.fillna(emptyValue)
    indicators = indicators.fillna(emptyValue)
    contrasts = contrasts.fillna(emptyValue)
    conditions = conditions.fillna(emptyValue)
    assertions = assertions.fillna(emptyValue)
    relationships = relationships.fillna(emptyValue)
    states = states.fillna(emptyValue)
    projects = projects.fillna(emptyValue)
    references = references.fillna(emptyValue)

    # Classes worksheet
    for row in task_classes.iterrows():

        task_class_label = language_string(row[1]["ClassName"])
        task_class_iri = mhdb_iri_simple(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", task_class_label))
        predicates_list.append(("a", "rdf:Class"))

        if row[1]["DefinitionReference_index"] not in exclude_list:
            source = references[references["index"] ==
                np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
            if source not in exclude_list:
                source = check_iri(source)
                #predicates_list.append(("dcterms:isReferencedBy", source))
                predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    mhdb_iri_simple(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                task_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in task_properties.iterrows():

        task_property_label = language_string(row[1]["property"])
        task_property_iri = mhdb_iri_simple(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", task_property_label))
        predicates_list.append(("a", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:state",
                                    mhdb_iri_simple(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    mhdb_iri_simple(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list:
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    mhdb_iri_simple(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    mhdb_iri_simple(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                task_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # tasks worksheet
    for row in tasks.iterrows():

        task_label = language_string(row[1]["task"])
        task_iri = check_iri(row[1]["task"])

        predicates_list = []
        predicates_list.append(("rdfs:label", task_label))
        predicates_list.append(("a", "demcare:Task"))

        if row[1]["description"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["description"])))
        if row[1]["aliases"] not in exclude_list:
            aliases = row[1]["aliases"].split(',')
            for alias in aliases:
                predicates_list.append(("rdfs:label", language_string(alias)))

        # indices_state = row[1]["indices_state"]
        # if indices_state not in exclude_list:
        #     indices = [np.int(x) for x in indices_state.strip().split(',') if len(x)>0]
        #     for index in indices:
        #         objectRDF = states[states["index"] ==
        #                             index]["state"].values[0]
        #         if isinstance(objectRDF, str):
        #             predicates_list.append(("mhdb:assessesDomain",
        #                                     check_iri(objectRDF)))

        # # Cognitive Atlas-specific columns
        # cogatlas_node_id = check_iri(row[1]["cogatlas_node_id"])
        # if cogatlas_node_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasNodeID",
        #                             cogatlas_node_id))
        # cogatlas_prop_id = check_iri(row[1]["cogatlas_prop_id"])
        # if cogatlas_prop_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasPropID",
        #                             cogatlas_prop_id))

        for predicates in predicates_list:
            statements = add_to_statements(
                task_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # implementations worksheet
    for row in implementations.iterrows():

        implementation_label = language_string(row[1]["implementation"])
        implementation_iri = check_iri(row[1]["implementation"])

        predicates_list = []
        predicates_list.append(("rdfs:label", implementation_label))
        predicates_list.append(("a", "demcare:Task"))
        if row[1]["description"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["description"])))
        if row[1]["link"] not in exclude_list:
            predicates_list.append(("doap:homepage", check_iri(row[1]["link"])))

        # indices to other worksheets
        indices_task = row[1]["indices_task"]
        indices_project = row[1]["indices_project"]
        #indices_state = row[1]["indices_state"]
        if indices_task not in exclude_list:
            indices = [np.int(x) for x in indices_task.strip().split(',')
                       if len(x)>0]
            for index in indices:
                objectRDF = tasks[tasks["index"] == index]["task"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF)))
        if indices_project not in exclude_list:
            indices = [np.int(x) for x in indices_project.strip().split(',')
                       if len(x)>0]
            for index in indices:
                objectRDF = projects[projects["index"] ==
                                     index]["project"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasProject",
                                            check_iri(objectRDF)))
        # if indices_state not in exclude_list:
        #     indices = [np.int(x) for x in indices_state.strip().split(',')
        #                if len(x)>0]
        #     for index in indices:
        #         objectRDF = states[states["index"] ==
        #                            index]["state"].values[0]
        #         if isinstance(objectRDF, str):
        #             predicates_list.append(("mhdb:???",
        #                                     check_iri(objectRDF)))

        # # Cognitive Atlas-specific columns
        # cogatlas_node_id = row[1]["cogatlas_node_id"]
        # if cogatlas_node_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasNodeID",
        #                             check_iri(cogatlas_node_id)))
        # cogatlas_prop_id = row[1]["cogatlas_prop_id"]
        # if cogatlas_prop_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasPropID",
        #                             check_iri(cogatlas_prop_id)))
        # cogatlas_task_id = row[1]["cogatlas_task_id"]
        # if cogatlas_task_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasTaskID",
        #                             check_iri(cogatlas_task_id)))

        for predicates in predicates_list:
            statements = add_to_statements(
                implementation_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # indicators worksheet
    for row in indicators.iterrows():

        indicator_label = language_string(row[1]["indicator"])
        indicator_iri = check_iri(row[1]["indicator"])

        predicates_list = []
        predicates_list.append(("rdfs:label", indicator_label))
        predicates_list.append(("a", "mhdb:Indicator"))

        # # Cognitive Atlas-specific columns
        # cogatlas_node_id = row[1]["cogatlas_node_id"]
        # if cogatlas_node_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasNodeID",
        #                             check_iri(cogatlas_node_id)))

        for predicates in predicates_list:
            statements = add_to_statements(
                indicator_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # contrasts worksheet
    for row in contrasts.iterrows():

        contrast_label = language_string(row[1]["contrast"])
        contrast_iri = check_iri(row[1]["contrast"])

        predicates_list = []
        predicates_list.append(("rdfs:label", contrast_label))
        predicates_list.append(("a", "mhdb:Contrast"))

        # # Cognitive Atlas-specific columns
        # cogatlas_node_id = row[1]["cogatlas_node_id"]
        # if cogatlas_node_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasNodeID",
        #                             check_iri(cogatlas_node_id)))
        # cogatlas_prop_id = row[1]["cogatlas_prop_id"]
        # if cogatlas_prop_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasPropID",
        #                             check_iri(cogatlas_prop_id)))

        for predicates in predicates_list:
            statements = add_to_statements(
                contrast_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # conditions worksheet
    for row in conditions.iterrows():

        condition_label = language_string(row[1]["condition"])
        condition_iri = check_iri(row[1]["condition"])

        predicates_list = []
        predicates_list.append(("rdfs:label", condition_label))
        predicates_list.append(("a", "mhdb:Condition"))
        if row[1]["description"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["description"])))

        # # Cognitive Atlas-specific columns
        # cogatlas_node_id = row[1]["cogatlas_node_id"]
        # if cogatlas_node_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasNodeID",
        #                             check_iri(cogatlas_node_id)))
        # cogatlas_prop_id = row[1]["cogatlas_prop_id"]
        # if cogatlas_prop_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasPropID",
        #                             check_iri(cogatlas_prop_id)))

        for predicates in predicates_list:
            statements = add_to_statements(
                condition_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # assertions worksheet
    for row in assertions.iterrows():

        assertion_label = language_string(row[1]["assertion"])
        assertion_iri = check_iri(row[1]["assertion"])

        predicates_list = []
        predicates_list.append(("rdfs:label", assertion_label))
        predicates_list.append(("a", "mhdb:Assertion"))

        # # Cognitive Atlas-specific columns
        # cogatlas_node_id = row[1]["cogatlas_node_id"]
        # if cogatlas_node_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasNodeID",
        #                             check_iri(cogatlas_node_id)))
        # cogatlas_prop_id = row[1]["cogatlas_prop_id"]
        # if cogatlas_prop_id not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasPropID",
        #                             check_iri(cogatlas_prop_id)))
        # confidence_level = row[1]["confidence_level"]
        # if confidence_level not in exclude_list:
        #     predicates_list.append(("mhdb:hasCognitiveAtlasConfidenceLevel",
        #                             check_iri(confidence_level)))
        subject_def = row[1]["subject_def"]
        if subject_def not in exclude_list:
            predicates_list.append(("mhdb:hasCognitiveAtlasSubjectDef",
                                    language_string(subject_def)))

        for predicates in predicates_list:
            statements = add_to_statements(
                assertion_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # relationships worksheet
    for row in relationships.iterrows():

        cogatlas_reln_type = str(row[1]["cogatlas_reln_type"])
        cogatlas_startNode = int(row[1]["cogatlas_startNode"])
        cogatlas_endNode = int(row[1]["cogatlas_endNode"])
        # cogatlas_reln_id = row[1]["cogatlas_reln_id"]
        # cogatlas_reln_prop_id = row[1]["cogatlas_reln_prop_id"]
        # cogatlas_reln_contrast_id = row[1]["cogatlas_reln_contrast_id"]
        # cogatlas_reln_task_id = row[1]["cogatlas_reln_task_id"]
        # cogatlas_reln_disorder = row[1]["cogatlas_reln_disorder"]

        subject = ""
        object = ""

        # tasks worksheet
        if subject in exclude_list:
            subject_task = tasks[tasks['cogatlas_node_id'] == cogatlas_startNode]["task"]
            if not subject_task.empty:
                subject = tasks[tasks['cogatlas_node_id'] == cogatlas_startNode]["task"].values[0]
        if object in exclude_list:
            object_task = tasks[tasks['cogatlas_node_id'] == cogatlas_endNode]["task"]
            if not object_task.empty:
                object = tasks[tasks['cogatlas_node_id'] == cogatlas_endNode]["task"].values[0]

        # implementations worksheet
        if subject in exclude_list:
            subject_implementation = implementations[implementations['cogatlas_node_id'] == cogatlas_startNode]["implementation"]
            if not subject_implementation.empty:
                subject = implementations[implementations['cogatlas_node_id'] == cogatlas_startNode]["implementation"].values[0]
        if object in exclude_list:
            object_implementation = implementations[implementations['cogatlas_node_id'] == cogatlas_endNode]["implementation"]
            if not object_implementation.empty:
                object = implementations[implementations['cogatlas_node_id'] == cogatlas_endNode]["implementation"].values[0]

        # indicators worksheet
        if subject in exclude_list:
            subject_indicator = indicators[indicators['cogatlas_node_id'] == cogatlas_startNode]["indicator"]
            if not subject_indicator.empty:
                subject = indicators[indicators['cogatlas_node_id'] == cogatlas_startNode]["indicator"].values[0]
        if object in exclude_list:
            object_indicator = indicators[indicators['cogatlas_node_id'] == cogatlas_endNode]["indicator"]
            if not object_indicator.empty:
                object = indicators[indicators['cogatlas_node_id'] == cogatlas_endNode]["indicator"].values[0]

        # contrasts worksheet
        if subject in exclude_list:
            subject_contrast = contrasts[contrasts['cogatlas_node_id'] == cogatlas_startNode]["contrast"]
            if not subject_contrast.empty:
                subject = contrasts[contrasts['cogatlas_node_id'] == cogatlas_startNode]["contrast"].values[0]
        if object in exclude_list:
            object_contrast = contrasts[contrasts['cogatlas_node_id'] == cogatlas_endNode]["contrast"]
            if not object_contrast.empty:
                object = contrasts[contrasts['cogatlas_node_id'] == cogatlas_endNode]["contrast"].values[0]

        # conditions worksheet
        if subject in exclude_list:
            subject_condition = conditions[conditions['cogatlas_node_id'] == cogatlas_startNode]["condition"]
            if not subject_condition.empty:
                subject = conditions[conditions['cogatlas_node_id'] == cogatlas_startNode]["condition"].values[0]
        if object in exclude_list:
            object_condition = conditions[conditions['cogatlas_node_id'] == cogatlas_endNode]["condition"]
            if not object_condition.empty:
                object = conditions[conditions['cogatlas_node_id'] == cogatlas_endNode]["condition"].values[0]

        # assertions worksheet
        if subject in exclude_list:
            subject_assertion = assertions[assertions['cogatlas_node_id'] == cogatlas_startNode]["assertion"]
            if not subject_assertion.empty:
                subject = assertions[assertions['cogatlas_node_id'] == cogatlas_startNode]["assertion"].values[0]
        if object in exclude_list:
            object_assertion = assertions[assertions['cogatlas_node_id'] == cogatlas_endNode]["assertion"]
            if not object_assertion.empty:
                object = assertions[assertions['cogatlas_node_id'] == cogatlas_endNode]["assertion"].values[0]

        # states worksheet
        if subject in exclude_list:
            subject_state = states[states['cogatlas_node_id'] == cogatlas_startNode]["state"]
            if not subject_state.empty:
                subject = states[states['cogatlas_node_id'] == cogatlas_startNode]["state"].values[0]
        if object in exclude_list:
            object_state = states[states['cogatlas_node_id'] == cogatlas_endNode]["state"]
            if not object_state.empty:
                object = states[states['cogatlas_node_id'] == cogatlas_endNode]["state"].values[0]

        # references spreadsheet
        if subject in exclude_list:
            subject_reference = references[references['cogatlas_node_id'] == cogatlas_startNode]["reference"]
            if not subject_reference.empty:
                subject = references[references['cogatlas_node_id'] == cogatlas_startNode]["reference"].values[0]
        if object in exclude_list:
            object_reference = references[references['cogatlas_node_id'] == cogatlas_endNode]["reference"]
            if not object_reference.empty:
                object = references[references['cogatlas_node_id'] == cogatlas_endNode]["reference"].values[0]

        if subject not in exclude_list and object not in exclude_list and not subject == object:

            if cogatlas_reln_type == "ASSERTS":
                cogatlas_reln_type = "mhdb:asserts"
            if cogatlas_reln_type == "HASCITATION":
                cogatlas_reln_type = "mhdb:hasCitation"
            if cogatlas_reln_type == "CLASSIFIEDUNDER":
                cogatlas_reln_type = "mhdb:classifiedUnder"
            if cogatlas_reln_type == "HASCONDITION":
                cogatlas_reln_type = "mhdb:hasPossibleCondition"
            if cogatlas_reln_type == "HASCONTRAST":
                cogatlas_reln_type = "mhdb:hasPossibleContrast"
            if cogatlas_reln_type == "HASDIFFERENCE":
                cogatlas_reln_type = "mhdb:hasDifference"
            if cogatlas_reln_type == "HASIMPLEMENTATION":
                cogatlas_reln_type = "mhdb:hasImplementation"
            if cogatlas_reln_type == "HASINDICATOR":
                cogatlas_reln_type = "mhdb:hasIndicator"
            if cogatlas_reln_type == "ISA":
                cogatlas_reln_type = "mhdb:isA"
            if cogatlas_reln_type == "KINDOF":
                cogatlas_reln_type = "mhdb:kindOf"
            if cogatlas_reln_type == "MEASUREDBY":
                cogatlas_reln_type = "mhdb:measuredBy"
            if cogatlas_reln_type == "PARTOF":
                cogatlas_reln_type = "mhdb:partOf"
            if cogatlas_reln_type == "PREDICATE":
                cogatlas_reln_type = "mhdb:hasPredicate"
            if cogatlas_reln_type == "PREDICATE_DEF":
                cogatlas_reln_type = "mhdb:hasPredicateDefinition"
            if cogatlas_reln_type == "SUBJECT":
                cogatlas_reln_type = "mhdb:hasSubject"

            statements = add_to_statements(check_iri(subject),
                                           cogatlas_reln_type,
                                           check_iri(object),
                                           statements, exclude_list)
            #print('"{0}", {1}, "{2}"'.format(subject, cogatlas_reln_type, object))

    return statements


def ingest_questions(questions_xls, references_xls, statements={}):
    """
    Function to ingest questions spreadsheet

    Parameters
    ----------
    questions_xls: pandas ExcelFile

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

    # load worksheets as pandas dataframes
    question_classes = questions_xls.parse("Classes")
    question_properties = questions_xls.parse("Properties")
    questions = questions_xls.parse("questions")
    response_types = questions_xls.parse("response_types")
    scale_types = questions_xls.parse("scale_types")
    value_types = questions_xls.parse("value_types")
    references = references_xls.parse("references")

    # fill NANs with emptyValue
    question_classes = question_classes.fillna(emptyValue)
    question_properties = question_properties.fillna(emptyValue)
    questions = questions.fillna(emptyValue)
    response_types = response_types.fillna(emptyValue)
    scale_types = scale_types.fillna(emptyValue)
    value_types = value_types.fillna(emptyValue)
    references = references.fillna(emptyValue)

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in question_classes.iterrows():

        question_class_label = language_string(row[1]["ClassName"])
        question_class_iri = mhdb_iri_simple(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", question_class_label))
        predicates_list.append(("a", "rdf:Class"))

        if row[1]["DefinitionReference_index"] not in exclude_list:
            source = references[references["index"] ==
                np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
            if source not in exclude_list:
                source = check_iri(source)
                #predicates_list.append(("dcterms:isReferencedBy", source))
                predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    mhdb_iri_simple(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                question_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in question_properties.iterrows():

        question_property_label = language_string(row[1]["property"])
        question_property_iri = mhdb_iri_simple(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", question_property_label))
        predicates_list.append(("a", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:state",
                                    mhdb_iri_simple(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    mhdb_iri_simple(row[1]["propertyRange"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    mhdb_iri_simple(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    mhdb_iri_simple(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                question_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # questions worksheet
    qnum = 1
    old_questionnaires = []
    for row in questions.iterrows():

        ref = references[references["index"] ==
                         row[1]["index_reference"]]["link"].values[0]
        if ref not in exclude_list:
            questionnaire = ref
        else:
            questionnaire = references[references["index"] ==
                            row[1]["index_reference"]]["reference"].values[0]
        if questionnaire not in old_questionnaires:
            qnum = 1
            old_questionnaires.append(questionnaire)
        else:
            qnum += 1

        question = row[1]["question"]
        question_label = language_string(question)
        question_iri = check_iri("{0}_Q{1}".format(questionnaire, qnum))

        predicates_list = []
        predicates_list.append(("rdfs:label", question_label))
        predicates_list.append(("a", "disco:Question"))
        predicates_list.append(("disco:questionText", question_label))
        predicates_list.append(("dcterms:isReferencedBy", check_iri(questionnaire)))

        instructions = row[1]["instructions"]
        group_instructions = row[1]["question_group_instructions"]
        digital_instructions = row[1]["digital_instructions"]
        digital_group_instructions = row[1]["digital_group_instructions"]
        response_options = row[1]["response_options"]

        if instructions not in exclude_list:
            predicates_list.append(("mhdb:hasPaperInstructions",
                                    language_string(instructions)))
            # statements = add_to_statements(
            #     check_iri(instructions),
            #     "a",
            #     "mhdb:PaperInstructions",
            #     statements,
            #     exclude_list
            # )
            # statements = add_to_statements(
            #     check_iri(instructions),
            #     "rdfs:label",
            #     language_string(instructions),
            #     statements,
            #     exclude_list
            # )
            if group_instructions.strip() not in exclude_list:
                predicates_list.append(("mhdb:hasGroupInstructions",
                                        language_string(group_instructions)))
                # statements = add_to_statements(
                #     check_iri(group_instructions),
                #     "a",
                #     "mhdb:PaperInstructions",
                #     statements,
                #     exclude_list
                # )
                # statements = add_to_statements(
                #     check_iri(instructions),
                #     "mhdb:hasPaperInstructions",
                #     check_iri(group_instructions),
                #     statements,
                #     exclude_list
                # )
                # statements = add_to_statements(
                #     check_iri(group_instructions),
                #     "rdfs:label",
                #     language_string(group_instructions),
                #     statements,
                #     exclude_list
                # )
        if digital_instructions not in exclude_list:
            predicates_list.append(("mhdb:hasInstructions",
                                    language_string(digital_instructions)))
            # statements = add_to_statements(
            #     check_iri(digital_instructions),
            #     "a",
            #     "mhdb:Instructions",
            #     statements,
            #     exclude_list
            # )
            # statements = add_to_statements(
            #     check_iri(digital_instructions),
            #     "rdfs:label",
            #     language_string(digital_instructions),
            #     statements,
            #     exclude_list
            # )
            if digital_group_instructions not in exclude_list:
                predicates_list.append(("mhdb:hasDigitalGroupInstructions",
                                        language_string(digital_group_instructions)))
                # statements = add_to_statements(
                #     check_iri(digital_group_instructions),
                #     "a",
                #     "mhdb:Instructions",
                #     statements,
                #     exclude_list
                # )
                # statements = add_to_statements(
                #     check_iri(digital_instructions),
                #     "mhdb:hasInstructions",
                #     check_iri(digital_group_instructions),
                #     statements,
                #     exclude_list
                # )
                # statements = add_to_statements(
                #     check_iri(digital_group_instructions),
                #     "rdfs:label",
                #     language_string(digital_group_instructions),
                #     statements,
                #     exclude_list
                # )

        if response_options not in exclude_list:
            response_options = response_options.strip('-')
            response_options = response_options.replace("\n", "")
            response_options_iri = check_iri(response_options)
            if '"' in response_options:
                response_options = re.findall('[-+]?[0-9]+=".*?"', response_options)
            else:
                response_options = response_options.split(",")
            #print(row[1]["index"], ' response options: ', response_options)

            statements = add_to_statements(
                question_iri,
                "mhdb:hasResponseOptions",
                response_options_iri,
                statements,
                exclude_list
            )
            statements = add_to_statements(response_options_iri,
                                           "a", "rdf:Seq",
                                           statements, exclude_list)
            for iresponse, response_option in enumerate(response_options):
                #response_index = response_option.split("=")[0].strip()
                response = response_option.split("=")[1].strip()
                #response = response.strip('"').strip()
                #response = response.strip("'").strip()
                if response in exclude_list:
                    response_iri = "mhdb:Empty"
                else:
                    response_iri = check_iri(response)
                    statements = add_to_statements(
                        response_iri,
                        "rdf:label",
                        language_string(response),
                        statements,
                        exclude_list
                    )
                    statements = add_to_statements(
                        response_options_iri,
                        "rdf:_{0}".format(iresponse + 1),
                        response_iri,
                        statements,
                        exclude_list
                    )

            # response_sequence = "\n    [ a rdf:Seq ; "
            # for iresponse, response in enumerate(response_options):
            #     response_index = response.split("=")[0].strip()
            #     response = response.split("=")[1].strip()
            #     response = response.strip('"').strip()
            #     response = response.strip("'").strip()
            #     if response in [""]:
            #         response_iri = "mhdb:Empty"
            #     else:
            #         #print(row[1]["index"], response)
            #         response_iri = check_iri(response)
            #         statements = add_to_statements(
            #             response_iri,
            #             "rdf:label",
            #             language_string(response),
            #             statements,
            #             exclude_list
            #         )
            #
            #     if iresponse == len(response_options) - 1:
            #         delim = ""  # ""."
            #     else:
            #         delim = ";"
            #     response_sequence += \
            #         '\n      rdf:_{0} {1} {2}'.format(response_index,
            #                                           response_iri,
            #                                           delim)
            # response_sequence += "\n    ]"
            #
            # statements = add_to_statements(
            #     response_options_iri,
            #     "rdfs:value",
            #     response_sequence,
            #     statements,
            #     exclude_list
            # )

        indices_response_type = row[1]["indices_response_type"]
        index_scale_type = row[1]["scale_type"]
        index_value_type = row[1]["value_type"]
        num_options = row[1]["num_options"]
        index_neutral = row[1]["index_neutral"]
        index_min = row[1]["index_min_extreme_oo_unclear_na_none"]
        index_max = row[1]["index_max_extreme_oo_unclear_na_none"]
        index_dontknow = row[1]["index_dontknow_na"]

        if indices_response_type not in exclude_list:
            indices = [np.int(x) for x in
                       indices_response_type.strip().split(',') if len(x) > 0]
            for index in indices:
                objectRDF = response_types[response_types["index"] ==
                                           index]["response_type"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasResponseType",
                                            check_iri(objectRDF)))
        if index_scale_type not in exclude_list:
            scale_type_iri = scale_types[scale_types["index"] ==
                                         index_scale_type]["IRI"].values[0]
            if scale_type_iri in exclude_list:
                scale_type_iri = check_iri(scale_types[scale_types["index"] ==
                                          index_scale_type]["scale_type"].values[0])
            if scale_type_iri not in exclude_list:
                predicates_list.append(("mhdb:hasScaleType", check_iri(scale_type_iri)))

        if index_value_type not in exclude_list:
            value_type_iri = value_types[value_types["index"] ==
                                         index_value_type]["IRI"].values[0]
            if value_type_iri in exclude_list:
                value_type_iri = check_iri(value_types[value_types["index"] ==
                                          index_value_type]["value_type"].values[0])
            if value_type_iri not in exclude_list:
                predicates_list.append(("mhdb:hasValueType", check_iri(value_type_iri)))

        if num_options not in exclude_list:
            predicates_list.append(("mhdb:hasNumberOfOptions",
                                    '"{0}"^^xsd:nonNegativeInteger'.format(
                                        num_options)))
        if index_neutral not in exclude_list:
            if index_neutral not in ['oo', 'n/a']:
                predicates_list.append(("mhdb:hasNeutralValueForResponseIndex",
                                        '"{0}"^^xsd:integer'.format(
                                            index_neutral)))
        if index_min not in exclude_list:
            if index_min not in ['oo', 'n/a']:
                predicates_list.append(("mhdb:hasExtremeValueForResponseIndex",
                                        '"{0}"^^xsd:integer'.format(
                                            index_min)))
        if index_max not in exclude_list:
            if index_max not in ['oo', 'n/a']:
                predicates_list.append(("mhdb:hasExtremeValueForResponseIndex",
                                        '"{0}"^^xsd:integer'.format(
                                            index_max)))
        if index_dontknow not in exclude_list:
            predicates_list.append(("mhdb:hasDontKnowOrNanForResponseIndex",
                                    '"{0}"^^xsd:integer'.format(
                                        index_dontknow)))

        for predicates in predicates_list:
            statements = add_to_statements(
                question_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # response_types worksheet
    for row in response_types.iterrows():
        response_type_iri = check_iri(row[1]["IRI"])
        if response_type_iri in exclude_list:
            response_type_iri = check_iri(row[1]["response_type"])
            response_type_label = language_string(row[1]["response_type"])
            statements = add_to_statements(
                response_type_iri, "rdfs:label", response_type_label,
                statements, exclude_list)
            statements = add_to_statements(
                response_type_iri, "a", "mhdb:ResponseType",
                statements, exclude_list)

    # scale_types worksheet
    for row in scale_types.iterrows():
        scale_type_iri = check_iri(row[1]["IRI"])
        if scale_type_iri in exclude_list:
            scale_type_iri = check_iri(row[1]["scale_type"])
            scale_type_label = language_string(row[1]["scale_type"])
            statements = add_to_statements(
                scale_type_iri, "rdfs:label", check_iri(scale_type_label),
                statements, exclude_list)
            statements = add_to_statements(
                scale_type_iri, "a", "mhdb:ScaleType",
                statements, exclude_list)

    # value_types worksheet
    for row in value_types.iterrows():
        value_type_iri = check_iri(row[1]["IRI"])
        if value_type_iri in exclude_list:
            value_type_iri = check_iri(row[1]["value_type"])
            value_type_label = language_string(row[1]["value_type"])
            statements = add_to_statements(
                value_type_iri, "rdfs:label", value_type_label,
                statements, exclude_list)
            statements = add_to_statements(
                value_type_iri, "a", "mhdb:ValueType",
                statements, exclude_list)

    return statements


def ingest_dsm5(dsm5_xls, references_xls, statements={}):
    """
    Function to ingest dsm5 spreadsheet

    Parameters
    ----------
    dsm5_xls: pandas ExcelFile

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
    ...     referencesFILE = download_google_sheet(
    ...         'data/references.xlsx',
    ...         "1KDZhoz9CgHBVclhoOKBgDegUA9Vczui5wj61sXMgh34"
    ...     )
    ... except:
    ...     dsm5FILE = 'data/dsm5.xlsx'
    ...     referencesFILE = 'data/references.xlsx'
    >>> dsm5_xls = pd.ExcelFile(dsm5FILE)
    >>> behaviors_xls = pd.ExcelFile(behaviorsFILE)
    >>> references_xls = pd.ExcelFile(referencesFILE)
    >>> statements = ingest_dsm5(dsm5_xls, references_xls, statements={})
    >>> print(turtle_from_dict({
    ...     statement: statements[
    ...         statement
    ...     ] for statement in statements if statement == "mhdb:despair"
    ... }).split("\\n\\t")[0])
    #mhdb:despair rdfs:label "despair"@en ;
    """
    import math

    # load worksheets as pandas dataframes
    dsm_classes = dsm5_xls.parse("Classes")
    dsm_properties = dsm5_xls.parse("Properties")
    disorders = dsm5_xls.parse("disorders")
    sign_or_symptoms = dsm5_xls.parse("sign_or_symptoms")
    examples_sign_or_symptoms = dsm5_xls.parse("examples_sign_or_symptoms")
    severities = dsm5_xls.parse("severities")
    disorder_categories = dsm5_xls.parse("disorder_categories")
    disorder_subcategories = dsm5_xls.parse("disorder_subcategories")
    disorder_subsubcategories = dsm5_xls.parse("disorder_subsubcategories")
    disorder_subsubsubcategories = dsm5_xls.parse("disorder_subsubsubcategories")
    diagnostic_specifiers = dsm5_xls.parse("diagnostic_specifiers")
    diagnostic_criteria = dsm5_xls.parse("diagnostic_criteria")
    references = references_xls.parse("references")

    # fill NANs with emptyValue
    dsm_classes = dsm_classes.fillna(emptyValue)
    dsm_properties = dsm_properties.fillna(emptyValue)
    disorders = disorders.fillna(emptyValue)
    sign_or_symptoms = sign_or_symptoms.fillna(emptyValue)
    examples_sign_or_symptoms = examples_sign_or_symptoms.fillna(emptyValue)
    severities = severities.fillna(emptyValue)
    disorder_categories = disorder_categories.fillna(emptyValue)
    disorder_subcategories = disorder_subcategories.fillna(emptyValue)
    disorder_subsubcategories = disorder_subsubcategories.fillna(emptyValue)
    disorder_subsubsubcategories = disorder_subsubsubcategories.fillna(emptyValue)
    diagnostic_specifiers = diagnostic_specifiers.fillna(emptyValue)
    diagnostic_criteria = diagnostic_criteria.fillna(emptyValue)
    references = references.fillna(emptyValue)

    # Classes worksheet
    for row in dsm_classes.iterrows():

        dsm_class_label = language_string(row[1]["ClassName"])
        dsm_class_iri = mhdb_iri_simple(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", dsm_class_label))
        predicates_list.append(("a", "rdf:Class"))

        if row[1]["DefinitionReference_index"] not in exclude_list:
            source = references[references["index"] ==
                np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
            if source not in exclude_list:
                source = check_iri(source)
                #predicates_list.append(("dcterms:isReferencedBy", source))
                predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    mhdb_iri_simple(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                dsm_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in dsm_properties.iterrows():

        dsm_property_label = language_string(row[1]["property"])
        dsm_property_iri = mhdb_iri_simple(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", dsm_property_label))
        predicates_list.append(("a", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:state",
                                    mhdb_iri_simple(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    mhdb_iri_simple(row[1]["propertyRange"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    mhdb_iri_simple(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    mhdb_iri_simple(row[1]["subPropertyOf"])))
        if row[1]["health-lifesci_codingSystem"] not in exclude_list:
            predicates_list.append(("health-lifesci:codingSystem",
                                    check_iri(row[1]["health-lifesci_codingSystem"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                dsm_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # sign_or_symptoms worksheet
    for row in sign_or_symptoms.iterrows():

        # sign or symptom?
        sign_or_symptom_number = np.int(row[1]["sign_or_symptom_number"])
        if sign_or_symptom_number == 1:
            sign_or_symptom = "health-lifesci:MedicalSign"
        elif sign_or_symptom_number == 2:
            sign_or_symptom = "health-lifesci:MedicalSymptom"
        else:
            sign_or_symptom = "health-lifesci:MedicalSignOrSymptom"

        # reference
        source = None
        if row[1]["index_reference"] not in exclude_list:
            source = references[
                    references["index"] == row[1]["index_reference"]
                ]["link"].values[0]
            if source not in exclude_list:
                source_iri = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source_iri = check_iri(source)

        symptom_label = language_string(row[1]["sign_or_symptom"])
        symptom_iri = check_iri(row[1]["sign_or_symptom"])

        predicates_list = []
        predicates_list.append(("rdfs:label", symptom_label))
        predicates_list.append(("a", sign_or_symptom))
        predicates_list.append(("dcterms:isReferencedBy", source_iri))

        # specific to females/males?
        if row[1]["index_gender"] not in exclude_list:
            if np.int(row[1]["index_gender"]) == 1:  # female
                predicates_list.append(
                    ("schema:audienceType", "schema:Female"))
                predicates_list.append(
                    ("schema:epidemiology", "schema:Female"))
            elif np.int(row[1]["index_gender"]) == 2:  # male
                predicates_list.append(
                    ("schema:audienceType", "schema:Male"))
                predicates_list.append(
                    ("schema:epidemiology", "schema:Male"))

        # indices for disorders
        indices_disorder = row[1]["indices_disorder"]
        if indices_disorder not in exclude_list:
            if isinstance(indices_disorder, str):
                indices_disorder = [np.int(x) for x in
                           indices_disorder.strip().split(',') if len(x) > 0]
            elif isinstance(indices_disorder, int):
                indices_disorder = [indices_disorder]
            for index in indices_disorder:
                disorder = disorders[disorders["index"] == index
                                    ]["disorder"].values[0]
                if isinstance(disorder, str):
                    if sign_or_symptom_number == 1:
                        predicates_list.append(("mhdb:isMedicalSignOf",
                                                check_iri(disorder)))
                    elif sign_or_symptom_number == 2:
                        predicates_list.append(("mhdb:isMedicalSymptomOf",
                                                check_iri(disorder)))
                    else:
                        predicates_list.append(("mhdb:isMedicalSignOrSymptomOf",
                                                check_iri(disorder)))

        # Is the sign/symptom a subclass of other another sign/symptom?
        indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
        if indices_sign_or_symptom not in exclude_list:
            if isinstance(indices_sign_or_symptom, str):
                indices_sign_or_symptom = [np.int(x) for x in
                    indices_sign_or_symptom.strip().split(',') if len(x) > 0]
            elif isinstance(indices_sign_or_symptom, int):
                indices_sign_or_symptom = [indices_sign_or_symptom]
            for index in indices_sign_or_symptom:
                super_sign = sign_or_symptoms[sign_or_symptoms["index"] ==
                                              index]["sign_or_symptom"].values[0]
                if isinstance(super_sign, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(super_sign)))
        for predicates in predicates_list:
            statements = add_to_statements(
                symptom_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # examples_sign_or_symptoms worksheet
    for row in examples_sign_or_symptoms.iterrows():

        example_symptom_label = language_string(row[1]["examples_sign_or_symptoms"])
        example_symptom_iri = check_iri(row[1]["examples_sign_or_symptoms"])

        predicates_list = []
        predicates_list.append(("rdfs:label", example_symptom_label))
        predicates_list.append(("a", "mhdb:ExampleSignOrSymptom"))

        indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
        if indices_sign_or_symptom not in exclude_list:
            if isinstance(indices_sign_or_symptom, str):
                indices_sign_or_symptom = [np.int(x) for x in
                           indices_sign_or_symptom.strip().split(',') if len(x) > 0]
            elif isinstance(indices_sign_or_symptom, int):
                indices_sign_or_symptom = [indices_sign_or_symptom]
            for index in indices_sign_or_symptom:
                objectRDF = sign_or_symptoms[sign_or_symptoms["index"] ==
                                             index]["sign_or_symptom"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:isExampleOf",
                                            check_iri(objectRDF)))

        for predicates in predicates_list:
            statements = add_to_statements(
                example_symptom_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # severities worksheet
    for row in severities.iterrows():

        severity_label = language_string(row[1]["severity"])
        severity_iri = check_iri(row[1]["severity"])

        predicates_list = []
        predicates_list.append(("rdfs:label", severity_label))
        predicates_list.append(("a", "mhdb:DisorderSeverity"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                severity_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # diagnostic_specifiers worksheet
    for row in diagnostic_specifiers.iterrows():

        diagnostic_specifier_label = language_string(row[1]["diagnostic_specifier"])
        diagnostic_specifier_iri = check_iri(row[1]["diagnostic_specifier"])

        predicates_list = []
        predicates_list.append(("rdfs:label", diagnostic_specifier_label))
        predicates_list.append(("a", "mhdb:DiagnosticSpecifier"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                diagnostic_specifier_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # diagnostic_criteria worksheet
    for row in diagnostic_criteria.iterrows():

        diagnostic_criterion_label = language_string(row[1]["diagnostic_criterion"])
        diagnostic_criterion_iri = check_iri(row[1]["diagnostic_criterion"])

        predicates_list = []
        predicates_list.append(("rdfs:label", diagnostic_criterion_label))
        predicates_list.append(("a", "mhdb:DiagnosticCriterion"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                diagnostic_criterion_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # disorders worksheet
    exclude_categories = []
    for row in disorders.iterrows():

        disorder_label = row[1]["disorder"]
        disorder_iri_label = disorder_label

        predicates_list = []
        predicates_list.append(("a", "mhdb:Disorder"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        if row[1]["subClassOf_2"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf_2"])))
        if row[1]["note"] not in exclude_list:
            predicates_list.append(("mhdb:hasNote",
                                    language_string(row[1]["note"])))

        if row[1]["ICD9_code"] not in exclude_list:
            ICD9_code = str(row[1]["ICD9_code"])
            predicates_list.append(("mhdb:hasICD9Code",
                                    check_iri('ICD9_' + ICD9_code)))
            disorder_label += "; ICD9:{0}".format(ICD9_code)
            disorder_iri_label += " ICD9 {0}".format(ICD9_code)
        if row[1]["ICD10_code"] not in exclude_list:
            ICD10_code = row[1]["ICD10_code"]
            predicates_list.append(("mhdb:hasICD10Code",
                                    check_iri('ICD10_' + ICD10_code)))
            disorder_label += "; ICD10:{0}".format(ICD10_code)
            disorder_iri_label += " ICD10 {0}".format(ICD10_code)
        if row[1]["index_diagnostic_specifier"] not in exclude_list:
            diagnostic_specifier = diagnostic_specifiers[
            diagnostic_specifiers["index"] == int(row[1]["index_diagnostic_specifier"])
            ]["diagnostic_specifier"].values[0]
            if isinstance(diagnostic_specifier, str):
                predicates_list.append(("mhdb:hasDiagnosticSpecifier",
                                        check_iri(diagnostic_specifier)))
                disorder_label += "; specifier: {0}".format(diagnostic_specifier)
                disorder_iri_label += " specifier {0}".format(diagnostic_specifier)

        if row[1]["index_diagnostic_inclusion_criterion"] not in exclude_list:
            diagnostic_inclusion_criterion = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_inclusion_criterion"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_inclusion_criterion, str):
                predicates_list.append(("mhdb:hasInclusionCriterion",
                                        check_iri(diagnostic_inclusion_criterion)))
                disorder_label += \
                    "; inclusion: {0}".format(diagnostic_inclusion_criterion)
                disorder_iri_label += \
                    " inclusion {0}".format(diagnostic_inclusion_criterion)

        if row[1]["index_diagnostic_inclusion_criterion2"] not in exclude_list:
            diagnostic_inclusion_criterion2 = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_inclusion_criterion2"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_inclusion_criterion2, str):
                predicates_list.append(("mhdb:hasInclusionCriterion",
                                        check_iri(diagnostic_inclusion_criterion2)))
                disorder_label += \
                    ", {0}".format(diagnostic_inclusion_criterion2)
                disorder_iri_label += \
                    " {0}".format(diagnostic_inclusion_criterion2)

        if row[1]["index_diagnostic_exclusion_criterion"] not in exclude_list:
            diagnostic_exclusion_criterion = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_exclusion_criterion"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_exclusion_criterion, str):
                predicates_list.append(("mhdb:hasExclusionCriterion",
                                        check_iri(diagnostic_exclusion_criterion)))
                disorder_label += \
                    "; exclusion: {0}".format(diagnostic_exclusion_criterion)
                disorder_iri_label += \
                    " exclusion {0}".format(diagnostic_exclusion_criterion)

        if row[1]["index_diagnostic_exclusion_criterion2"] not in exclude_list:
            diagnostic_exclusion_criterion2 = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_exclusion_criterion2"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_exclusion_criterion2, str):
                predicates_list.append(("mhdb:hasExclusionCriterion",
                                        check_iri(diagnostic_exclusion_criterion2)))
                disorder_label += \
                    ", {0}".format(diagnostic_exclusion_criterion2)
                disorder_iri_label += \
                    " {0}".format(diagnostic_exclusion_criterion2)

        if row[1]["index_severity"] not in exclude_list:
            severity = severities[
            severities["index"] == int(row[1]["index_severity"])
            ]["severity"].values[0]
            if isinstance(severity, str) and severity not in exclude_list:
                predicates_list.append(("mhdb:hasSeverity",
                                        check_iri(severity)))
                disorder_label += \
                    "; severity: {0}".format(severity)
                disorder_iri_label += \
                    " severity {0}".format(severity)

        if row[1]["index_disorder_subsubsubcategory"] not in exclude_list:
            disorder_subsubsubcategory = disorder_subsubsubcategories[
                disorder_subsubsubcategories["index"] ==
                int(row[1]["index_disorder_subsubsubcategory"])
            ]["disorder_subsubsubcategory"].values[0]
            disorder_subsubcategory = disorder_subsubcategories[
                disorder_subsubcategories["index"] ==
                int(row[1]["index_disorder_subsubcategory"])
            ]["disorder_subsubcategory"].values[0]
            disorder_subcategory = disorder_subcategories[
                disorder_subcategories["index"] ==
                int(row[1]["index_disorder_subcategory"])
            ]["disorder_subcategory"].values[0]
            disorder_category = disorder_categories[
                disorder_categories["index"] ==
                int(row[1]["index_disorder_category"])
            ]["disorder_category"].values[0]
            predicates_list.append(("mhdb:hasDisorderCategory",
                                    check_iri(disorder_subsubsubcategory)))
            statements = add_to_statements(
                check_iri(disorder_subsubsubcategory),
                "rdfs:subClassOf",
                check_iri(disorder_subsubcategory),
                statements,
                exclude_list
            )
            if disorder_subsubcategory not in exclude_categories:
                statements = add_to_statements(
                    check_iri(disorder_subsubcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_subcategory),
                    statements,
                    exclude_list
                )
                statements = add_to_statements(
                    check_iri(disorder_subcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_category),
                    statements,
                    exclude_list
                )
                exclude_categories.append(disorder_subsubcategory)
        elif row[1]["index_disorder_subsubcategory"] not in exclude_list:
            disorder_subsubcategory = disorder_subsubcategories[
                disorder_subsubcategories["index"] ==
                int(row[1]["index_disorder_subsubcategory"])
            ]["disorder_subsubcategory"].values[0]
            disorder_subcategory = disorder_subcategories[
                disorder_subcategories["index"] ==
                int(row[1]["index_disorder_subcategory"])
            ]["disorder_subcategory"].values[0]
            disorder_category = disorder_categories[
                disorder_categories["index"] ==
                int(row[1]["index_disorder_category"])
            ]["disorder_category"].values[0]
            predicates_list.append(("mhdb:hasDisorderCategory",
                                    check_iri(disorder_subsubcategory)))
            statements = add_to_statements(
                check_iri(disorder_subsubcategory),
                "rdfs:subClassOf",
                check_iri(disorder_subcategory),
                statements,
                exclude_list
            )
            if disorder_subcategory not in exclude_categories:
                statements = add_to_statements(
                    check_iri(disorder_subcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_category),
                    statements,
                    exclude_list
                )
                exclude_categories.append(disorder_subcategory)
        elif row[1]["index_disorder_subcategory"] not in exclude_list:
            disorder_subcategory = disorder_subcategories[
                disorder_subcategories["index"] == int(row[1]["index_disorder_subcategory"])
            ]["disorder_subcategory"].values[0]
            disorder_category = disorder_categories[
                disorder_categories["index"] == int(row[1]["index_disorder_category"])
            ]["disorder_category"].values[0]
            predicates_list.append(("mhdb:hasDisorderCategory",
                                    check_iri(disorder_subcategory)))
            if disorder_category not in exclude_categories:
                statements = add_to_statements(
                    check_iri(disorder_subcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_category),
                    statements,
                    exclude_list
                )
                exclude_categories.append(disorder_category)
        elif row[1]["index_disorder_category"] not in exclude_list:
            disorder_category = disorder_categories[
                disorder_categories["index"] == int(row[1]["index_disorder_category"])
            ]["disorder_category"].values[0]
            predicates_list.append(("mhdb:hasDisorderCategory",
                                    check_iri(disorder_category)))

        disorder_label = language_string(disorder_label)
        disorder_iri = check_iri(disorder_iri_label)
        predicates_list.append(("rdfs:label", disorder_label))
        for predicates in predicates_list:
            statements = add_to_statements(
                disorder_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # disorder_categories worksheet
    for row in disorder_categories.iterrows():

        disorder_category_label = language_string(row[1]["disorder_category"])
        disorder_category_iri = check_iri(row[1]["disorder_category"])

        predicates_list = []
        predicates_list.append(("rdfs:label", disorder_category_label))
        predicates_list.append(("a", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_to_statements(
                disorder_category_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # disorder_subcategories worksheet
    for row in disorder_subcategories.iterrows():

        disorder_subcategory_label = language_string(row[1]["disorder_subcategory"])
        disorder_subcategory_iri = check_iri(row[1]["disorder_subcategory"])

        predicates_list = []
        predicates_list.append(("rdfs:label", disorder_subcategory_label))
        predicates_list.append(("a", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_to_statements(
                disorder_subcategory_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # disorder_subsubcategories worksheet
    for row in disorder_subsubcategories.iterrows():

        disorder_subsubcategory_label = language_string(row[1]["disorder_subsubcategory"])
        disorder_subsubcategory_iri = check_iri(row[1]["disorder_subsubcategory"])

        predicates_list = []
        predicates_list.append(("rdfs:label", disorder_subsubcategory_label))
        predicates_list.append(("a", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_to_statements(
                disorder_subsubcategory_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # disorder_subsubsubcategories worksheet
    for row in disorder_subsubsubcategories.iterrows():

        disorder_subsubsubcategory_label = language_string(row[1]["disorder_subsubsubcategory"])
        disorder_subsubsubcategory_iri = check_iri(row[1]["disorder_subsubsubcategory"])

        predicates_list = []
        predicates_list.append(("rdfs:label", disorder_subsubsubcategory_label))
        predicates_list.append(("a", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_to_statements(
                disorder_subsubsubcategory_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_projects(projects_xls, states_xls, measures_xls,
                    references_xls, statements={}):
    """
    Function to ingest projects spreadsheet

    Parameters
    ----------
    projects_xls: pandas ExcelFile

    states_xls: pandas ExcelFile

    measures_xls: pandas ExcelFile

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

    # load worksheets as pandas dataframes
    project_classes = projects_xls.parse("Classes")
    project_properties = projects_xls.parse("Properties")
    projects = projects_xls.parse("projects")
    project_types = projects_xls.parse("project_types")
    groups = projects_xls.parse("groups")
    sensors = measures_xls.parse("sensors")
    measures = measures_xls.parse("measures")
    #states = states_xls.parse("states")
    references = references_xls.parse("references")

    # fill NANs with emptyValue
    project_classes = project_classes.fillna(emptyValue)
    project_properties = project_properties.fillna(emptyValue)
    projects = projects.fillna(emptyValue)
    project_types = project_types.fillna(emptyValue)
    groups = groups.fillna(emptyValue)
    sensors = sensors.fillna(emptyValue)
    measures = measures.fillna(emptyValue)
    #states = states.fillna(emptyValue)
    references = references.fillna(emptyValue)

    # Classes worksheet
    for row in project_classes.iterrows():

        project_class_label = language_string(row[1]["ClassName"])
        project_class_iri = mhdb_iri_simple(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", project_class_label))
        predicates_list.append(("a", "rdf:Class"))

        if row[1]["DefinitionReference_index"] not in exclude_list:
            source = references[references["index"] ==
                np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
            if source not in exclude_list:
                source = check_iri(source)
                #predicates_list.append(("dcterms:isReferencedBy", source))
                predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    mhdb_iri_simple(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                project_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in project_properties.iterrows():

        project_property_label = language_string(row[1]["property"])
        project_property_iri = mhdb_iri_simple(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", project_property_label))
        predicates_list.append(("a", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:state",
                                    mhdb_iri_simple(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    mhdb_iri_simple(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list:
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    mhdb_iri_simple(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    mhdb_iri_simple(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                project_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # projects worksheet
    for row in projects.iterrows():

        project_iri = check_iri(row[1]["project"])
        project_label = language_string(row[1]["project"])

        predicates_list = []
        predicates_list.append(("a", "doap:Project"))
        predicates_list.append(("rdfs:label", project_label))
        if row[1]["description"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["description"])))
        if row[1]["link"] not in exclude_list:
            predicates_list.append(("doap:homepage", check_iri(row[1]["link"])))

        #indices_state = row[1]["indices_state"]
        indices_project_type = row[1]["indices_project_type"]
        indices_group = row[1]["indices_group"]
        indices_sensor = row[1]["indices_sensor"]
        indices_measure = row[1]["indices_measure"]

        # doap:license

        # reference
        if row[1]["indices_reference"] not in exclude_list:
            indices = [np.int(x) for x in
                       row[1]["indices_reference"].strip().split(',') if len(x)>0]
            for index in indices:
                source = references[references["index"] == index]["link"].values[0]
                if source not in exclude_list:
                    source_iri = check_iri(source)
                else:
                    source = references[references["index"] ==
                                        index]["reference"].values[0]
                    source_iri = check_iri(source)
            predicates_list.append(("dcterms:isReferencedBy", source_iri))

        # if indices_state not in exclude_list:
        #     indices = [np.int(x) for x in
        #                indices_state.strip().split(',') if len(x)>0]
        #     for index in indices:
        #         print(index)
        #         objectRDF = states[states["index"] == index]["state"].values[0]
        #         if isinstance(objectRDF, str):
        #             predicates_list.append(("mhdb:???",
        #                                     check_iri(objectRDF)))
        if indices_project_type not in exclude_list:
            indices = [np.int(x) for x in
                       indices_project_type.strip().split(',') if len(x)>0]
            for index in indices:
                project_type_label = project_types[project_types["index"] ==
                                        index]["project_type"].values[0]
                project_type_iri = project_types[project_types["index"] ==
                                        index]["IRI"].values[0]
                if project_type_iri in exclude_list:
                    project_type_iri = project_type_label
                if project_type_iri not in exclude_list:
                    predicates_list.append(("doap:category",
                                            check_iri(project_type_iri)))
        if indices_group not in exclude_list:
            indices = [np.int(x) for x in
                       indices_group.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = groups[groups["index"] == index]["group"].values[0]
                if objectRDF not in exclude_list:
                    predicates_list.append(("doap:maintainer",
                                            check_iri(objectRDF)))
        if indices_sensor not in exclude_list:
            indices = [np.int(x) for x in
                       indices_sensor.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sensors[sensors["index"] == index]["sensor"].values[0]
                if objectRDF not in exclude_list:
                    predicates_list.append(("ssn:hasSubSystem",
                                            check_iri(objectRDF)))
        if indices_measure not in exclude_list:
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures[measures["index"] ==
                                     index]["measure"].values[0]
                if objectRDF not in exclude_list:
                    predicates_list.append(("ssn:observes",
                                            check_iri(objectRDF)))
        for predicates in predicates_list:
            statements = add_to_statements(
                project_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # project_types worksheet
    for row in project_types.iterrows():

        predicates_list = []
        predicates_list.append(("a", "doap:Project"))
        predicates_list.append(("rdfs:label",
                                language_string(row[1]["project_type"])))
        if row[1]["IRI"] not in exclude_list:
            project_type_iri = check_iri(row[1]["IRI"])
        else:
            project_type_iri = check_iri(row[1]["project_type"])

        if row[1]["indices_project_type"] not in exclude_list:
            indices = [np.int(x) for x in
                       row[1]["indices_project_type"].strip().split(',')
                       if len(x)>0]
            for index in indices:
                objectRDF = project_types[project_types["index"]  ==
                                          index]["project_type"].values[0]
                if objectRDF not in exclude_list:
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF)))
        for predicates in predicates_list:
            statements = add_to_statements(
                project_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # groups worksheet
    for row in groups.iterrows():

        group_iri = check_iri(row[1]["group"])
        group_label = language_string(row[1]["group"])

        predicates_list = []
        predicates_list.append(("a", "foaf:Group"))
        predicates_list.append(("rdfs:label", group_label))

        if row[1]["link"] not in exclude_list:
            predicates_list.append(("foaf:homepage", check_iri(row[1]["link"])))
        if row[1]["abbreviation"] not in exclude_list:
            predicates_list.append(("dbpedia-owl:abbreviation",
                                    check_iri(row[1]["abbreviation"])))
        if row[1]["organization"] not in exclude_list:
            organization_iri = check_iri(row[1]["organization"])
            statements = add_to_statements(organization_iri, "a",
                                "org:organization", statements, exclude_list)
            statements = add_to_statements(organization_iri, "rdfs:label",
                                check_iri(row[1]["organization"], statements, exclude_list))
            predicates_list.append(("dcterms:isPartOf", organization_iri))
        if row[1]["member"] not in exclude_list:
            member_iri = check_iri(row[1]["member"])
            statements = add_to_statements(member_iri, "a", "foaf:Person",
                                           statements, exclude_list)
            predicates_list.append(("org:hasMember", member_iri))

        for predicates in predicates_list:
            statements = add_to_statements(
                group_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_references(references_xls, states_xls, projects_xls, dsm5_xls,
                      statements={}):
    """
    Function to ingest references spreadsheet

    Parameters
    ----------
    states_xls: pandas ExcelFile

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

    # load worksheets as pandas dataframes
    reference_classes = references_xls.parse("Classes")
    reference_properties = references_xls.parse("Properties")
    references = references_xls.parse("references")
    reference_types = references_xls.parse("reference_types")
    medications = references_xls.parse("medications")
    treatments = references_xls.parse("treatments")
    licenses = references_xls.parse("licenses")
    audiences = references_xls.parse("audiences")
    ages = references_xls.parse("ages")
    #genders = references_xls.parse("genders")
    #states = states_xls.parse("states")
    disorders = dsm5_xls.parse("disorders")
    disorder_categories = dsm5_xls.parse("disorder_categories")
    groups = projects_xls.parse("groups")

    # fill NANs with emptyValue
    reference_classes = reference_classes.fillna(emptyValue)
    reference_properties = reference_properties.fillna(emptyValue)
    references = references.fillna(emptyValue)
    reference_types = reference_types.fillna(emptyValue)
    medications = medications.fillna(emptyValue)
    treatments = treatments.fillna(emptyValue)
    licenses = licenses.fillna(emptyValue)
    audiences = audiences.fillna(emptyValue)
    ages = ages.fillna(emptyValue)
    #genders = genders.fillna(emptyValue)
    #states = states.fillna(emptyValue)
    disorders = disorders.fillna(emptyValue)
    disorder_categories = disorder_categories.fillna(emptyValue)
    groups = groups.fillna(emptyValue)

    # Classes worksheet
    for row in reference_classes.iterrows():

        reference_class_label = language_string(row[1]["ClassName"])
        reference_class_iri = mhdb_iri_simple(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", reference_class_label))
        predicates_list.append(("a", "rdf:Class"))

        if row[1]["DefinitionReference_index"] not in exclude_list:
            source = references[references["index"] ==
                np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
            if source not in exclude_list:
                source = check_iri(source)
                #predicates_list.append(("dcterms:isReferencedBy", source))
                predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    mhdb_iri_simple(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    mhdb_iri_simple(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                reference_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in reference_properties.iterrows():

        reference_property_label = language_string(row[1]["property"])
        reference_property_iri = mhdb_iri_simple(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", reference_property_label))
        predicates_list.append(("a", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:state",
                                    mhdb_iri_simple(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    mhdb_iri_simple(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list:
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    mhdb_iri_simple(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    mhdb_iri_simple(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    mhdb_iri_simple(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                reference_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # references worksheet
    for row in references.iterrows():

        predicates_list = []

        # reference IRI
        title = row[1]["reference"]
        if title not in exclude_list:
            reference_iri = check_iri(title)
            predicates_list.append(("rdfs:label", language_string(title)))
            predicates_list.append(("dcterms:title", language_string(title)))
            predicates_list.append(("a", "dcterms:BibliographicResource"))

            # general columns
            link = row[1]["link"]
            description = row[1]["description"]
            abbreviation = row[1]["abbreviation"]
            if link not in exclude_list:
                predicates_list.append(("foaf:homepage", check_iri(link)))
            if description not in exclude_list:
                predicates_list.append(("rdfs:comment", language_string(description)))
            if abbreviation not in exclude_list:
                predicates_list.append(("dbpedia-owl:abbreviation", check_iri(abbreviation)))

            # specific to females/males?
            index_gender = row[1]["index_gender"]
            if index_gender not in exclude_list:
                if np.int(index_gender) == 1:  # female
                    predicates_list.append(
                        ("schema:audienceType", "schema:Female"))
                    predicates_list.append(
                        ("schema:epidemiology", "schema:Female"))
                elif np.int(index_gender) == 2:  # male
                    predicates_list.append(
                        ("schema:audienceType", "schema:Male"))
                    predicates_list.append(
                        ("schema:epidemiology", "schema:Male"))

            # research article-specific columns
            authors = row[1]["authors"]
            pubdate = row[1]["pubdate"]
            PubMedID = row[1]["PubMedID"]
            if authors not in exclude_list:
                predicates_list.append(("bibo:authorList", check_iri(authors)))
            if pubdate not in exclude_list:
                predicates_list.append(("npg:publicationDate", check_iri(pubdate)))
                # npg:publicationYear
            if PubMedID not in exclude_list:
                predicates_list.append(("fabio:hasPubMedId", check_iri(PubMedID)))

            # questionnaire-specific columns
            number_of_questions = row[1]["number_of_questions"]
            minutes_to_complete = row[1]["minutes_to_complete"]
            age_min = row[1]["age_min"]
            age_max = row[1]["age_max"]
            if number_of_questions not in exclude_list and \
                    isinstance(number_of_questions, str):
                if "-" in number_of_questions:
                    predicates_list.append(("mhdb:hasNumberOfQuestions",
                                            '"{0}"^^xsd:string'.format(
                                                number_of_questions)))
                else:
                    predicates_list.append(("mhdb:hasNumberOfQuestions",
                                            '"{0}"^^xsd:nonNegativeInteger'.format(
                                                number_of_questions)))
            if minutes_to_complete not in exclude_list and \
                    isinstance(minutes_to_complete, str):
                if "-" in minutes_to_complete:
                    predicates_list.append(("mhdb:takesMinutesToComplete",
                        '"{0}"^^xsd:string'.format(minutes_to_complete)))
                else:
                    predicates_list.append(("mhdb:takesMinutesToComplete",
                        '"{0}"^^xsd:nonNegativeInteger'.format(minutes_to_complete)))
            if age_min not in exclude_list and isinstance(age_min, str):
                if "-" in age_min:
                    predicates_list.append(("schema:requiredMinAge",
                        '"{0}"^^xsd:string'.format(age_min)))
                else:
                    predicates_list.append(("schema:requiredMinAge",
                        '"{0}"^^xsd:nonNegativeInteger'.format(age_min)))
            if age_max not in exclude_list and isinstance(age_max, str):
                if "-" in age_max:
                    predicates_list.append(("schema:requiredMaxAge",
                        '"{0}"^^xsd:string'.format(age_max)))
                else:
                    predicates_list.append(("schema:requiredMaxAge",
                        '"{0}"^^xsd:nonNegativeInteger'.format(age_max)))

            # indices to other worksheets about who uses the references
            indices_reference_type = row[1]["indices_reference_type"]
            indices_group = row[1]["indices_group_users"]
            indices_audience = row[1]["indices_audience"]
            indices_age = row[1]["indices_age"]
            indices_cited_references = row[1]["indices_cited_references"]
            index_license = row[1]["index_license"]
            if indices_reference_type not in exclude_list:
                if isinstance(indices_reference_type, str):
                    indices = [np.int(x) for x in
                               indices_reference_type.strip().split(',') if len(x)>0]
                elif isinstance(indices_reference_type, float):
                    indices = [np.int(indices_reference_type)]
                for index in indices:
                    objectRDF = reference_types[
                        reference_types["index"] == index]["reference_type"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("mhdb:hasReferenceType",
                                                check_iri(objectRDF)))
            if indices_group not in exclude_list:
                if isinstance(indices_group, str):
                    indices = [np.int(x) for x in
                               indices_group.strip().split(',') if len(x) > 0]
                elif isinstance(indices_group, float):
                    indices = [np.int(indices_group)]
                for index in indices:
                    objectRDF = groups[
                        groups["index"] == index]["group"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("mhdb:usedByGroup",
                                                check_iri(objectRDF)))
            if indices_audience not in exclude_list:
                indices = [np.int(x) for x in
                           indices_audience.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = audiences[audiences["index"] == index]["audience"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("schema:audienceType",
                                                check_iri(objectRDF)))
            if indices_age not in exclude_list:
                indices = [np.int(x) for x in
                           indices_age.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = ages[
                        ages["index"] == index]["age"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("schema:audienceType",
                                                check_iri(objectRDF)))
            if indices_cited_references not in exclude_list:
                indices = [np.int(x) for x in
                           indices_cited_references.strip().split(',') if len(x)>0]
                for index in indices:
                    # cited reference IRI
                    title_cited = references[
                        references["index"] == index]["reference"].values
                    if title_cited not in exclude_list:
                        title_cited = references[
                            references["index"] == index]["reference"].values[0]
                        title_cited = check_iri(title_cited)
                    else:
                        break
                    predicates_list.append(("dcterms:isReferencedBy", title_cited))
            if index_license not in exclude_list:
                objectRDF = references[licenses["index"] ==
                                       index_license]["license"].values[0]
                if objectRDF not in exclude_list:
                    predicates_list.append(("dcterms:license", check_iri(objectRDF)))

            # indices to other worksheets about content of the references
            #indices_state = row[1]["indices_state"]
            comorbidity_indices_disorder = row[1]["comorbidity_indices_disorder"]
            medication_indices = row[1]["medication_indices"]
            treatment_indices = row[1]["treatment_indices"]
            indices_disorder = row[1]["indices_disorder"]
            indices_disorder_category = row[1]["indices_disorder_category"]
            # if indices_state not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_state.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = states[states["index"] == index]["state"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append(("mhdb:isAboutDomain",
            #                                     check_iri(objectRDF)))
            if comorbidity_indices_disorder not in exclude_list:
                indices = [np.int(x) for x in
                           comorbidity_indices_disorder.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = disorders[
                        disorders["index"] == index]["disorder"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("schema:about", check_iri(objectRDF)))
            if medication_indices not in exclude_list:
                indices = [np.int(x) for x in
                           medication_indices.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = medications[
                        medications["index"] == index]["medication"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("schema:about", check_iri(objectRDF)))
            if treatment_indices not in exclude_list:
                indices = [np.int(x) for x in
                           treatment_indices.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = treatments[treatments["index"] ==
                                           index]["treatment"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("schema:about", check_iri(objectRDF)))
            if indices_disorder not in exclude_list:
                indices = [np.int(x) for x in
                           indices_disorder.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = disorders[disorders["index"] ==
                                          index]["disorder"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("schema:about", check_iri(objectRDF)))
            if indices_disorder_category not in exclude_list:
                indices = [np.int(x) for x in
                           indices_disorder_category.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = disorder_categories[disorder_categories["index"] ==
                                     index]["disorder_category"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("schema:about", check_iri(objectRDF)))

            # # Cognitive Atlas-specific columns
            # cogatlas_node_id = row[1]["cogatlas_node_id"]
            # cogatlas_prop_id = row[1]["cogatlas_prop_id"]
            # if cogatlas_node_id not in exclude_list:
            #     predicates_list.append(("mhdb:hasCognitiveAtlasNodeID",
            #                             "cognitiveatlas_node_id_" + check_iri(cogatlas_node_id)))
            # if cogatlas_prop_id not in exclude_list:
            #     predicates_list.append(("mhdb:hasCognitiveAtlasPropID",
            #                             "cognitiveatlas_prop_id_" + check_iri(cogatlas_prop_id)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    reference_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # reference_types worksheet
    for row in reference_types.iterrows():

        reference_type_label = language_string(row[1]["reference_type"])

        if row[1]["IRI"] not in exclude_list:
            reference_type_iri = check_iri(row[1]["IRI"])
        else:
            reference_type_iri = check_iri(row[1]["reference_type"])

        predicates_list = []
        predicates_list.append(("rdfs:label",
                                language_string(reference_type_label)))
        predicates_list.append(("a", "mhdb:ReferenceType"))

        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_to_statements(
                reference_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # licenses worksheet
    for row in licenses.iterrows():

        license_label = language_string(row[1]["license"])
        license_link = check_iri(row[1]["link"])
        if row[1]["IRI"] not in exclude_list:
            license_iri = check_iri(row[1]["IRI"])
        else:
            license_iri = license_link

        predicates_list = []
        predicates_list.append(("a", "cc:License", exclude_list))
        predicates_list.append(("rdfs:label", license_label))

        if row[1]["abbreviation"] not in exclude_list:
            predicates_list.append(("dbpedia-owl:abbreviation",
                                    check_iri(row[1]["abbreviation"])))
        if row[1]["description"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["description"])))
        if row[1]["link"] not in exclude_list:
            predicates_list.append(("foaf:homepage", check_iri(row[1]["link"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                license_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # audiences worksheet
    for row in audiences.iterrows():
        if row[1]["IRI"] not in exclude_list:
            audience_iri = check_iri(row[1]["IRI"])
        else:
            audience_iri = check_iri(row[1]["audience"])
        statements = add_to_statements(audience_iri, "a",
                                       "schema:Audience",
                                       statements, exclude_list)
        statements = add_to_statements(audience_iri, "rdfs:label",
                            language_string(row[1]["audience"]),
                                       statements, exclude_list)

    # ages worksheet
    for row in ages.iterrows():
        if row[1]["IRI"] not in exclude_list:
            age_iri = check_iri(row[1]["IRI"])
        else:
            age_iri = check_iri(row[1]["age"])
        statements = add_to_statements(age_iri, "a",
                            "mhdb:AgeGroup", statements, exclude_list)
        statements = add_to_statements(age_iri, "rdfs:label",
                            language_string(row[1]["age"]),
                                       statements, exclude_list)

    # genders worksheet
    # for row in genders.iterrows():
    #     if row[1]["IRI"] not in exclude_list:
    #         gender_iri = check_iri(row[1]["IRI"])
    #     else:
    #         gender_iri = check_iri(row[1]["gender"])
    #     statements = add_to_statements(gender_iri, "rdfs:label",
    #                         language_string(row[1]["gender"]),
    #                         statements, exclude_list)

    # medications worksheet
    for row in medications.iterrows():
        if row[1]["IRI"] not in exclude_list:
            medication_iri = check_iri(row[1]["IRI"])
        else:
            medication_iri = check_iri(row[1]["medication"])
        statements = add_to_statements(medication_iri, "a",
                            "mhdb:Medication", statements, exclude_list)
        statements = add_to_statements(medication_iri, "rdfs:label",
                            language_string(row[1]["medication"]),
                                       statements, exclude_list)

    # treatments worksheet
    for row in treatments.iterrows():
        if row[1]["IRI"] not in exclude_list:
            treatment_iri = check_iri(row[1]["IRI"])
        else:
            treatment_iri = check_iri(row[1]["treatment"])
        statements = add_to_statements(treatment_iri, "a",
                            "mhdb:Treatment", statements, exclude_list)
        statements = add_to_statements(treatment_iri, "rdfs:label",
                            language_string(row[1]["treatment"]),
                                       statements, exclude_list)

    return statements


# def ingest_behaviors(behaviors_xls, references_xls, dsm5_xls, statements={}):
#     """
#     Function to ingest behaviors spreadsheet
#
#     Parameters
#     ----------
#     behaviors_xls: pandas ExcelFile
#
#     references_xls: pandas ExcelFile
#
#     dsm5_xls: pandas ExcelFile
#
#     statements:  dictionary
#         key: string
#             RDF subject
#         value: dictionary
#             key: string
#                 RDF predicate
#             value: {string}
#                 set of RDF objects
#
#     Returns
#     -------
#     statements: dictionary
#         key: string
#             RDF subject
#         value: dictionary
#             key: string
#                 RDF predicate
#             value: {string}
#                 set of RDF objects
#
#     Example
#     -------
#     """
#
#     # load worksheets as pandas dataframes
#     behavior_classes = behaviors_xls.parse("Classes")
#     behavior_properties = behaviors_xls.parse("Properties")
#     behaviors = behaviors_xls.parse("behaviors")
#     question_preposts = behaviors_xls.parse("question_preposts")
#     sign_or_symptoms = dsm5_xls.parse("sign_or_symptoms")
#     references = references_xls.parse("references")
#
#     # fill NANs with emptyValue
#     behavior_classes = behavior_classes.fillna(emptyValue)
#     behavior_properties = behavior_properties.fillna(emptyValue)
#     behaviors = behaviors.fillna(emptyValue)
#     question_preposts = question_preposts.fillna(emptyValue)
#     sign_or_symptoms = sign_or_symptoms.fillna(emptyValue)
#     references = references.fillna(emptyValue)
#
#     statements = audience_statements(statements)
#
#     # Classes worksheet
#     for row in behavior_classes.iterrows():
#
#         behavior_class_label = language_string(row[1]["ClassName"])
#         behavior_class_iri = mhdb_iri_simple(row[1]["ClassName"])
#
#         predicates_list = []
#         predicates_list.append(("rdfs:label", behavior_class_label))
#         predicates_list.append(("a", "rdf:Class"))
#
#         if row[1]["DefinitionReference_index"] not in exclude_list:
#             source = references[references["index"] ==
#                 np.int(row[1]["DefinitionReference_index"])]["link"].values[0]
#             if source not in exclude_list:
#                 source = check_iri(source)
#                 #predicates_list.append(("dcterms:isReferencedBy", source))
#                 predicates_list.append(("rdfs:isDefinedBy", source))
#
#         if row[1]["Definition"] not in exclude_list:
#             predicates_list.append(("rdfs:comment",
#                                     check_iri(row[1]["Definition"])))
#         if row[1]["sameAs"] not in exclude_list:
#             predicates_list.append(("owl:sameAs",
#                                     mhdb_iri_simple(row[1]["sameAs"])))
#         if row[1]["equivalentClass"] not in exclude_list:
#             predicates_list.append(("rdfs:equivalentClass",
#                                     mhdb_iri_simple(row[1]["equivalentClass"])))
#         if row[1]["equivalentClass_2"] not in exclude_list:
#             predicates_list.append(("rdfs:equivalentClass",
#                                     mhdb_iri_simple(row[1]["equivalentClass_2"])))
#         if row[1]["subClassOf"] not in exclude_list:
#             predicates_list.append(("rdfs:subClassOf",
#                                     mhdb_iri_simple(row[1]["subClassOf"])))
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 behavior_class_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     # Properties worksheet
#     for row in behavior_properties.iterrows():
#
#         behavior_property_label = language_string(row[1]["property"])
#         behavior_property_iri = mhdb_iri_simple(row[1]["property"])
#
#         predicates_list = []
#         predicates_list.append(("rdfs:label", behavior_property_label))
#         predicates_list.append(("a", "rdf:Property"))
#
#         if row[1]["propertyDomain"] not in exclude_list:
#             predicates_list.append(("rdfs:state",
#                                     mhdb_iri_simple(row[1]["propertyDomain"])))
#         if row[1]["propertyRange"] not in exclude_list:
#             predicates_list.append(("rdfs:range",
#                                     mhdb_iri_simple(row[1]["propertyRange"])))
#         # if row[1]["Definition"] not in exclude_list:
#         #     predicates_list.append(("rdfs:comment",
#         #                             check_iri(row[1]["Definition"])))
#         if row[1]["sameAs"] not in exclude_list:
#             predicates_list.append(("owl:sameAs",
#                                     mhdb_iri_simple(row[1]["sameAs"])))
#         if row[1]["equivalentProperty"] not in exclude_list:
#             predicates_list.append(("rdfs:equivalentProperty",
#                                     mhdb_iri_simple(row[1]["equivalentProperty"])))
#         if row[1]["subPropertyOf"] not in exclude_list:
#             predicates_list.append(("rdfs:subPropertyOf",
#                                     mhdb_iri_simple(row[1]["subPropertyOf"])))
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 behavior_property_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     # behaviors worksheet
#     for row in behaviors.iterrows():
#
#         behavior_label = language_string(row[1]["behavior"])
#         behavior_iri = check_iri(row[1]["behavior"])
#
#         predicates_list = []
#         predicates_list.append(("a", "mhdb:Behavior"))
#         predicates_list.append(("rdfs:label", behavior_label))
#
#         if row[1]["index_adverb_prefix"] not in exclude_list:
#             index_adverb_prefix = np.int(row[1]["index_adverb_prefix"])
#             adverb_prefix = question_preposts[question_preposts["index"] ==
#                 index_adverb_prefix]["question_prepost"].values[0]
#             if isinstance(adverb_prefix, str):
#                 predicates_list.append(("mhdb:hasQuestionPrefix",
#                                         check_iri(adverb_prefix)))
#
#         if row[1]["index_gender"] not in exclude_list:
#             index_gender = np.int(row[1]["index_gender"])
#             if index_gender == 2:  # male
#                 predicates_list.append(
#                     ("schema:audience", "MaleAudience"))
#             elif index_gender == 3: # female
#                 predicates_list.append(
#                     ("schema:audience", "FemaleAudience"))
#
#         indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
#         if indices_sign_or_symptom not in exclude_list:
#             indices = [np.int(x) for x in
#                        indices_sign_or_symptom.strip().split(',') if len(x)>0]
#             for index in indices:
#                 #print(row[1]["index"], index)
#                 objectRDF = sign_or_symptoms[sign_or_symptoms["index"] ==
#                                              index]["sign_or_symptom"].values[0]
#                 if isinstance(objectRDF, str):
#                     predicates_list.append(("mhdb:relatedToMedicalSignOrSymptom",
#                                             check_iri(objectRDF)))
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 behavior_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     # question_preposts worksheet
#     for row in question_preposts.iterrows():
#
#         question_prepost_label = language_string(row[1]["question_prepost"])
#         question_prepost_iri = check_iri(row[1]["question_prepost"])
#
#         predicates_list = []
#         predicates_list.append(("a", "mhdb:QuestionPrefix"))
#         predicates_list.append(("rdfs:label", question_prepost_label))
#
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 question_prepost_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     return statements
#

# def ingest_claims(claims_xls, states_xls, measures_xls, references_xls,
#                   statements={}):
#     """
#     Function to ingest claims spreadsheet
#
#     Parameters
#     ----------
#     claims_xls: pandas ExcelFile
#
#     references_xls: pandas ExcelFile
#
#     statements:  dictionary
#         key: string
#             RDF subject
#         value: dictionary
#             key: string
#                 RDF predicate
#             value: {string}
#                 set of RDF objects
#
#     Returns
#     -------
#     statements: dictionary
#         key: string
#             RDF subject
#         value: dictionary
#             key: string
#                 RDF predicate
#             value: {string}
#                 set of RDF objects
#
#     Example
#     -------
#     """
#
#     # load worksheets as pandas dataframes
#     claim_classes = claims_xls.parse("Classes")
#     claim_properties = claims_xls.parse("Properties")
#     sensors = measures_xls.parse("sensors")
#     measures = measures_xls.parse("measures")
#     states = states_xls.parse("states")
#     claims = claims_xls.parse("claims")
#     references = references_xls.parse("references")
#
#     # fill NANs with emptyValue
#     claim_classes = claim_classes.fillna(emptyValue)
#     claim_properties = claim_properties.fillna(emptyValue)
#     sensors = sensors.fillna(emptyValue)
#     measures = measures.fillna(emptyValue)
#     states = states.fillna(emptyValue)
#     claims = claims.fillna(emptyValue)
#     references = references.fillna(emptyValue)
#
#     statements = audience_statements(statements)
#
#     # Classes worksheet
#     for row in claim_classes.iterrows():
#
#         claim_class_label = language_string(row[1]["ClassName"])
#         claim_class_iri = check_iri(row[1]["ClassName"])
#
#         predicates_list = []
#         predicates_list.append(("rdfs:label", claim_class_label))
#         predicates_list.append(("a", "rdf:Class"))
#
#         if row[1]["DefinitionReference_index"] not in exclude_list:
#             source = references[references["index"] ==
#                             np.int(row[1]["DefinitionReference_index"])][
#                 "link"].values[0]
#             if source not in exclude_list:
#                 source = check_iri(source)
#                 # predicates_list.append(("dcterms:isReferencedBy", source))
#                 predicates_list.append(("rdfs:isDefinedBy", source))
#
#         if row[1]["Definition"] not in exclude_list and not \
#                     isinstance(row[1]["Definition"], float):
#             predicates_list.append(("rdfs:comment",
#                                     check_iri(row[1]["Definition"])))
#         if row[1]["sameAs"] not in exclude_list and not \
#                     isinstance(row[1]["sameAs"], float):
#             predicates_list.append(("owl:sameAs",
#                                     check_iri(row[1]["sameAs"])))
#         if row[1]["equivalentClass"] not in exclude_list and not \
#                     isinstance(row[1]["equivalentClass"], float):
#             predicates_list.append(("rdfs:equivalentClass",
#                                     check_iri(row[1]["equivalentClass"])))
#         if row[1]["equivalentClass_2"] not in exclude_list and not \
#                     isinstance(row[1]["equivalentClass_2"], float):
#             predicates_list.append(("rdfs:equivalentClass",
#                                     check_iri(row[1]["equivalentClass_2"])))
#         if row[1]["subClassOf"] not in exclude_list and not \
#                     isinstance(row[1]["subClassOf"], float):
#             predicates_list.append(("rdfs:subClassOf",
#                                     check_iri(row[1]["subClassOf"])))
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 claim_class_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     # Properties worksheet
#     for row in claim_properties.iterrows():
#
#         claim_property_label = language_string(row[1]["property"])
#         claim_property_iri = check_iri(row[1]["property"])
#
#         predicates_list = []
#         predicates_list.append(("rdfs:label", claim_property_label))
#         predicates_list.append(("a", "rdf:Property"))
#
#         if row[1]["propertyDomain"] not in exclude_list and not \
#                     isinstance(row[1]["propertyDomain"], float):
#             predicates_list.append(("rdfs:state",
#                                     check_iri(row[1]["propertyDomain"])))
#         if row[1]["propertyRange"] not in exclude_list and not \
#                     isinstance(row[1]["propertyRange"], float):
#             predicates_list.append(("rdfs:range",
#                                     check_iri(row[1]["propertyRange"])))
#         # if row[1]["Definition"] not in exclude_list and not \
#         #                     isinstance(index_source, float):
#         #     predicates_list.append(("rdfs:comment",
#         #                             check_iri(row[1]["Definition"])))
#         if row[1]["sameAs"] not in exclude_list and not \
#                     isinstance(row[1]["sameAs"], float):
#             predicates_list.append(("owl:sameAs",
#                                     check_iri(row[1]["sameAs"])))
#         if row[1]["equivalentProperty"] not in exclude_list and not \
#                     isinstance(row[1]["equivalentProperty"], float):
#             predicates_list.append(("rdfs:equivalentProperty",
#                                     check_iri(row[1]["equivalentProperty"])))
#         if row[1]["subPropertyOf"] not in exclude_list and not \
#                     isinstance(row[1]["subPropertyOf"], float):
#             predicates_list.append(("rdfs:subPropertyOf",
#                                     check_iri(row[1]["subPropertyOf"])))
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 claim_property_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     # claims worksheet
#     for row in claims.iterrows():
#
#         claim_label = language_string(row[1]["claim"])
#         claim_iri = check_iri(row[1]["claim"])
#
#         predicates_list = []
#         predicates_list.append(("a", "mhdb:Claim"))
#         predicates_list.append(("rdfs:label", claim_label))
#
#         indices_state = row[1]["indices_state"]
#         if isinstance(indices_state, str) and \
#                 indices_state not in exclude_list:
#             indices = [np.int(x) for x in
#                        indices_state.strip().split(',') if len(x)>0]
#             for index in indices:
#                 objectRDF = states[states["index"] ==
#                                     index]["state"].values[0]
#                 if isinstance(objectRDF, str):
#                     predicates_list.append(("mhdb:makesClaimAbout",
#                                             check_iri(objectRDF)))
#
#         indices_measure = row[1]["indices_measure"]
#         if isinstance(indices_measure, str) and \
#                 indices_measure not in exclude_list:
#             indices = [np.int(x) for x in
#                        indices_measure.strip().split(',') if len(x)>0]
#             for index in indices:
#                 objectRDF = measures[measures["index"] ==
#                                      index]["measure"].values[0]
#                 if isinstance(objectRDF, str):
#                     predicates_list.append(("mhdb:makesClaimAbout",
#                                             check_iri(objectRDF)))
#
#         indices_sensor = row[1]["indices_sensor"]
#         if isinstance(indices_sensor, str) and \
#                 indices_sensor not in exclude_list:
#             indices = [np.int(x) for x in
#                        indices_sensor.strip().split(',') if len(x)>0]
#             for index in indices:
#                 objectRDF = sensors[sensors["index"] ==
#                                      index]["sensor"].values[0]
#                 if isinstance(objectRDF, str):
#                     predicates_list.append(("mhdb:makesClaimAbout",
#                                             check_iri(objectRDF)))
#
#         indices_reference = row[1]["indices_references"]
#         if isinstance(indices_reference, str) and \
#                 indices_reference not in exclude_list:
#             indices = [np.int(x) for x in
#                        indices_reference.strip().split(',') if len(x)>0]
#             for index in indices:
#                 objectRDF = references[references["index"] ==
#                                        index]["reference"].values[0]
#                 if isinstance(objectRDF, str):
#                     predicates_list.append(("dcterms:isReferencedBy",
#                                             check_iri(objectRDF)))
#
#         if isinstance(row[1]["link"], str) and \
#                 row[1]["link"] not in exclude_list:
#             predicates_list.append(("dcterms:isReferencedBy",
#                                     check_iri(row[1]["link"])))
#
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 claim_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     return statements


