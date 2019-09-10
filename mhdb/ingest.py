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
    from mhdb.write_ttl import check_iri, mhdb_iri, language_string
except:
    from mhdb.mhdb.spreadsheet_io import download_google_sheet, return_string
    from mhdb.mhdb.write_ttl import check_iri, mhdb_iri, language_string
import numpy as np
import pandas as pd
import re
import math

exclude_list = ['', 'nan', np.nan, 'None', None, []]


def add_if(subject, predicate, object, statements={},
           exclude_list=exclude_list):
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

    exclude_list: list
        do not add statements if they contain any of these

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

    question_classes = questions_xls.parse("Classes")
    question_properties = questions_xls.parse("Properties")
    questions = questions_xls.parse("questions")
    response_types = questions_xls.parse("response_types")
    scale_types = questions_xls.parse("scale_types")
    value_types = questions_xls.parse("value_types")
    references = references_xls.parse("references")

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in question_classes.iterrows():

        question_class_label = language_string(row[1]["ClassName"])
        question_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", question_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                question_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in question_properties.iterrows():

        question_property_label = language_string(row[1]["property"])
        question_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", question_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
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
        if ref not in exclude_list and not isinstance(ref, float):
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
        question_label = language_string(str(question))
        question_iri = mhdb_iri("{0}_Q{1}".format(questionnaire, qnum))

        predicates_list = []
        predicates_list.append(("rdfs:label", question_label))
        predicates_list.append(("rdf:type", "disco:Question"))
        predicates_list.append(("disco:questionText", question_label))
        predicates_list.append(("dcterms:source", mhdb_iri(questionnaire)))

        instructions = row[1]["instructions"]
        group_instructions = row[1]["question_group_instructions"]
        digital_instructions = row[1]["digital_instructions"]
        digital_group_instructions = row[1]["digital_group_instructions"]
        response_options = row[1]["response_options"]

        if instructions not in exclude_list and \
                isinstance(instructions, str):
            predicates_list.append(("mhdb:hasPaperInstructions",
                                    mhdb_iri(instructions)))
            statements = add_if(
                mhdb_iri(instructions),
                "rdf:type",
                "mhdb:PaperInstructions",
                statements,
                exclude_list
            )
            statements = add_if(
                mhdb_iri(instructions),
                "rdfs:label",
                language_string(instructions),
                statements,
                exclude_list
            )
            if isinstance(group_instructions, str) and \
                    group_instructions.strip() not in exclude_list:
                statements = add_if(
                    mhdb_iri(group_instructions),
                    "rdf:type",
                    "mhdb:PaperInstructions",
                    statements,
                    exclude_list
                )
                statements = add_if(
                    mhdb_iri(instructions),
                    "mhdb:hasPaperInstructions",
                    mhdb_iri(group_instructions),
                    statements,
                    exclude_list
                )
                statements = add_if(
                    mhdb_iri(group_instructions),
                    "rdfs:label",
                    language_string(group_instructions),
                    statements,
                    exclude_list
                )
        if digital_instructions not in exclude_list and \
                isinstance(digital_instructions, str):
            predicates_list.append(("mhdb:hasInstructions",
                                    mhdb_iri(digital_instructions)))
            statements = add_if(
                mhdb_iri(digital_instructions),
                "rdf:type",
                "mhdb:Instructions",
                statements,
                exclude_list
            )
            statements = add_if(
                mhdb_iri(digital_instructions),
                "rdfs:label",
                language_string(digital_instructions),
                statements,
                exclude_list
            )
            if digital_group_instructions not in exclude_list and \
                    isinstance(digital_group_instructions, str):
                statements = add_if(
                    mhdb_iri(digital_group_instructions),
                    "rdf:type",
                    "mhdb:Instructions",
                    statements,
                    exclude_list
                )
                statements = add_if(
                    mhdb_iri(digital_instructions),
                    "mhdb:hasInstructions",
                    mhdb_iri(digital_group_instructions),
                    statements,
                    exclude_list
                )
                statements = add_if(
                    mhdb_iri(digital_group_instructions),
                    "rdfs:label",
                    language_string(digital_group_instructions),
                    statements,
                    exclude_list
                )

        if response_options not in exclude_list and \
                isinstance(response_options, str):
            response_options = response_options.strip('-')
            response_options = response_options.replace("\n", "")
            response_options_iri = mhdb_iri(response_options)
            if '"' in response_options:
                response_options = re.findall('[-+]?[0-9]+=".*?"', response_options)
            else:
                response_options = response_options.split(",")
            #print(row[1]["index"], ' response options: ', response_options)
            response_sequence = "\n    [ a rdf:Seq ; "
            for iresponse, response in enumerate(response_options):
                response_index = response.split("=")[0].strip()
                response = response.split("=")[1].strip()
                response = response.strip('"').strip()
                response = response.strip("'").strip()
                if response in [""]:
                    response_iri = "mhdb:Empty"
                else:
                    #print(row[1]["index"], response)
                    response_iri = mhdb_iri(response)
                    statements = add_if(
                        response_iri,
                        "rdf:label",
                        language_string(response),
                        statements,
                        exclude_list
                    )

                if iresponse == len(response_options) - 1:
                    delim = "."
                else:
                    delim = ";"
                response_sequence += \
                    '\n      rdf:_{0} {1} {2}'.format(response_index,
                                                      response_iri,
                                                      delim)
            response_sequence += "\n    ]"

            statements = add_if(
                response_options_iri,
                "rdfs:value",
                response_sequence,
                statements,
                exclude_list
            )
            statements = add_if(
                question_iri,
                "mhdb:hasResponseOptions",
                response_options_iri,
                statements,
                exclude_list
            )

        indices_response_type = row[1]["indices_response_type"]
        scale_type = row[1]["scale_type"]
        discrete_continuous = row[1]["discrete_continuous"]
        num_options = row[1]["num_options"]
        index_min = row[1]["index_min_extreme_oo_unclear_na_none"]
        index_max = row[1]["index_max_extreme_oo_unclear_na_none"]
        index_dontknow = row[1]["index_dontknow_na"]

        if indices_response_type not in exclude_list and \
                isinstance(indices_response_type, str):
            indices = [np.int(x) for x in
                       indices_response_type.strip().split(',') if len(x) > 0]
            for index in indices:
                objectRDF = response_types[response_types["index"] ==
                                           index]["response_type"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasResponseType",
                                            check_iri(objectRDF)))
        if scale_type not in exclude_list and \
                isinstance(scale_type, str):
            predicates_list.append(("mhdb:hasScaleType",
                                    mhdb_iri(scale_type)))
        if discrete_continuous not in exclude_list and \
                isinstance(discrete_continuous, str):
            predicates_list.append(("mhdb:hasValueType",
                                    mhdb_iri(discrete_continuous)))
        if num_options not in exclude_list and not isinstance(num_options, float):
            predicates_list.append(("mhdb:hasNumberOfOptions",
                                    '"{0}"^^xsd:nonNegativeInteger'.format(
                                        num_options)))
        if index_min not in exclude_list and not isinstance(index_min, float):
            if index_min not in ['oo', 'n/a']:
                predicates_list.append(("mhdb:hasExtremeValueForResponseIndex",
                                        '"{0}"^^xsd:integer'.format(
                                            index_min)))
        if index_max not in exclude_list and not isinstance(index_max, float):
            if index_max not in ['oo', 'n/a']:
                predicates_list.append(("mhdb:hasExtremeValueForResponseIndex",
                                        '"{0}"^^xsd:integer'.format(
                                            index_max)))
        if index_dontknow not in exclude_list and not isinstance(index_dontknow, float):
            predicates_list.append(("mhdb:hasDontKnowOrNanForResponseIndex",
                                    '"{0}"^^xsd:integer'.format(
                                        index_dontknow)))

        for predicates in predicates_list:
            statements = add_if(
                question_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # response_types worksheet
    for row in response_types.iterrows():

        response_type_label = language_string(row[1]["response_type"])
        response_type_iri = check_iri(row[1]["response_type"])

        predicates_list = []
        predicates_list.append(("rdfs:label", response_type_label))
        predicates_list.append(("rdf:type", "mhdb:ResponseType"))

        for predicates in predicates_list:
            statements = add_if(
                response_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # scale_types worksheet
    for row in scale_types.iterrows():

        scale_type_label = language_string(row[1]["scale_type"])
        scale_type_iri = check_iri(row[1]["scale_type"])

        predicates_list = []
        predicates_list.append(("rdfs:label", scale_type_label))
        predicates_list.append(("rdf:type", "mhdb:ScaleType"))

        for predicates in predicates_list:
            statements = add_if(
                scale_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # value_types worksheet
    for row in value_types.iterrows():

        value_type_label = language_string(row[1]["value_type"])
        value_type_iri = check_iri(row[1]["value_type"])

        predicates_list = []
        predicates_list.append(("rdfs:label", value_type_label))
        predicates_list.append(("rdf:type", "mhdb:ResponseType"))

        for predicates in predicates_list:
            statements = add_if(
                value_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_tasks(tasks_xls, domains_xls, projects_xls, references_xls,
                 statements={}):
    """
    Function to ingest tasks spreadsheet

    Parameters
    ----------
    tasks_xls: pandas ExcelFile

    domains_xls: pandas ExcelFile

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

    task_classes = tasks_xls.parse("Classes")
    task_properties = tasks_xls.parse("Properties")
    tasks = tasks_xls.parse("tasks")
    task_categories = tasks_xls.parse("task_categories")
    domains = domains_xls.parse("domains")
    projects = projects_xls.parse("projects")
    references = references_xls.parse("references")

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in task_classes.iterrows():

        task_class_label = language_string(row[1]["ClassName"])
        task_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", task_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                task_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in task_properties.iterrows():

        task_property_label = language_string(row[1]["property"])
        task_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", task_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "demcare:Task"))

        instructions = row[1]["instructions"]

        if instructions not in exclude_list and \
                isinstance(instructions, str):
            predicates_list.append(("mhdb:hasInstructions",
                                    mhdb_iri(instructions)))
            statements = add_if(
                mhdb_iri(instructions),
                "rdf:type",
                "mhdb:Instructions",
                statements,
                exclude_list
            )
            statements = add_if(
                mhdb_iri(instructions),
                "rdfs:label",
                language_string(instructions),
                statements,
                exclude_list
            )
        indices_task_categories = row[1]["indices_task_categories"]
        indices_domain = row[1]["indices_domain"]
        indices_project = row[1]["indices_project"]

        if indices_task_categories not in exclude_list and \
                not isinstance(indices_task_categories, float):
            if isinstance(indices_task_categories, int):
                indices = [indices_task_categories]
            else:
                indices = [np.int(x) for x in
                           indices_task_categories.strip().split(',')
                           if len(x)>0]
            for index in indices:
                objectRDF = task_categories[task_categories["index"] ==
                                            index]["task_category\n"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF.strip())))
        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains[domains["index"] ==
                                    index]["domain"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:assessesDomain",
                                            check_iri(objectRDF)))
        if isinstance(indices_project, str):
            indices = [np.int(x) for x in indices_project.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = projects[projects["index"] ==
                                     index]["project"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasProject",
                                            check_iri(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                task_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # task_categories worksheet
    for row in task_categories.iterrows():

        task_category_label = language_string(row[1]["task_category\n"])
        task_category_iri = check_iri(row[1]["task_category\n"])

        predicates_list = []
        predicates_list.append(("rdfs:label", task_category_label))
        predicates_list.append(("rdf:type", "demcare:Task"))

        indices_domain = row[1]["indices_domain"]
        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in indices_domain.strip().split(',')
                       if len(x)>0]
            for index in indices:
                objectRDF = domains[domains["index"] ==
                                    index]["domain"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:assessesDomain",
                                            check_iri(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                task_category_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_projects(projects_xls, domains_xls, measures_xls, statements={}):
    """
    Function to ingest projects spreadsheet

    Parameters
    ----------
    projects_xls: pandas ExcelFile

    domains_xls: pandas ExcelFile

    measures_xls: pandas ExcelFile

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

    project_classes = projects_xls.parse("Classes")
    project_properties = projects_xls.parse("Properties")
    projects = projects_xls.parse("projects")
    project_types = projects_xls.parse("project_types")
    people = projects_xls.parse("people")
    sensors = measures_xls.parse("sensors")
    measures = measures_xls.parse("measures")
    domains = domains_xls.parse("domains")

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in project_classes.iterrows():

        project_class_label = language_string(row[1]["ClassName"])
        project_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", project_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                project_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in project_properties.iterrows():

        project_property_label = language_string(row[1]["property"])
        project_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", project_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
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
        project_link = check_iri(row[1]["link"])

        predicates_list = []
        predicates_list.append(("rdf:type", "foaf:Project"))
        predicates_list.append(("rdfs:label", project_label))
        predicates_list.append(("schema:WebSite", project_link))
        if isinstance(row[1]["description"], str):
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["description"])))

        indices_domain = row[1]["indices_domain"]
        indices_project_type = row[1]["indices_project_type"]
        indices_people = row[1]["indices_people"]
        indices_sensor = row[1]["indices_sensor"]
        indices_measure = row[1]["indices_measure"]

        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in
                       indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains[domains["index"] ==
                                    index]["domain"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:isForDomain",
                                            check_iri(objectRDF)))
        if isinstance(indices_project_type, str):
            indices = [np.int(x) for x in
                       indices_project_type.strip().split(',') if len(x)>0]
            for index in indices:
                project_type_label = project_types[project_types["index"] ==
                                        index]["project_type"].values[0]
                project_type_iri = project_types[project_types["index"] ==
                                        index]["IRI"].values[0]
                if isinstance(project_type_iri, float):
                    project_type_iri = project_type_label
                if isinstance(project_type_iri, str):
                    predicates_list.append(("mhdb:hasProjectType",
                                            check_iri(project_type_iri)))
        if isinstance(indices_people, str):
            indices = [np.int(x) for x in
                       indices_people.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = people[people["index"] ==
                                        index]["person"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasOrganization",
                                            check_iri(objectRDF)))
        if isinstance(indices_sensor, str):
            indices = [np.int(x) for x in
                       indices_sensor.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sensors[sensors["index"]  ==
                                        index]["sensor"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasSensor",
                                            check_iri(objectRDF)))
        if isinstance(indices_measure, str):
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures[measures["index"]  ==
                                        index]["measure"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("ssn:observes",
                                            check_iri(objectRDF)))
        for predicates in predicates_list:
            statements = add_if(
                project_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # project_types worksheet
    for row in project_types.iterrows():

        predicates_list = []
        predicates_list.append(("rdf:type", "mhdb:ProjectType"))
        predicates_list.append(("rdfs:label",
                                language_string(row[1]["project_type"])))
        if isinstance(row[1]["IRI"], str) and \
                row[1]["IRI"] not in exclude_list:
            project_type_iri = check_iri(row[1]["IRI"])
        else:
            project_type_iri = check_iri(row[1]["project_type"])

        if isinstance(row[1]["indices_project_category"], str):
            indices = [np.int(x) for x in
                       row[1]["indices_project_category"].strip().split(',')
                       if len(x)>0]
            for index in indices:
                objectRDF = project_types[project_types["index"]  ==
                                        index]["project_type"].values[0]
                #print(objectRDF, type(objectRDF))
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF)))
        for predicates in predicates_list:
            statements = add_if(
                project_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # people worksheet
    for row in people.iterrows():

        person_iri = check_iri(row[1]["person"])
        person_label = language_string(row[1]["person"])
        predicates_list = []
        predicates_list.append(("rdf:type", "org:organization"))
        predicates_list.append(("rdfs:label", person_label))
        if isinstance(row[1]["link"], str) and \
                row[1]["link"] not in exclude_list:
            predicates_list.append(("schema:WebSite",
                                    check_iri(row[1]["link"])))
        if isinstance(row[1]["affiliate"], str) and \
                row[1]["affiliate"] not in exclude_list:
            predicates_list.append(("org:hasMember",
                                    check_iri(row[1]["affiliate"])))
        if isinstance(row[1]["location"], str) and \
                row[1]["location"] not in exclude_list:
            predicates_list.append(("frapo:hasLocation",
                                    mhdb_iri(row[1]["location"])))
        for predicates in predicates_list:
            statements = add_if(
                person_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_behaviors(behaviors_xls, references_xls, dsm5_xls, statements={}):
    """
    Function to ingest behaviors spreadsheet

    Parameters
    ----------
    behaviors_xls: pandas ExcelFile

    references_xls: pandas ExcelFile

    dsm5_xls: pandas ExcelFile

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

    behavior_classes = behaviors_xls.parse("Classes")
    behavior_properties = behaviors_xls.parse("Properties")
    behaviors = behaviors_xls.parse("behaviors")
    question_preposts = behaviors_xls.parse("question_preposts")
    sign_or_symptoms = dsm5_xls.parse("sign_or_symptoms")
    references = references_xls.parse("references")

    statements = audience_statements(statements)

    # Classes worksheet
    for row in behavior_classes.iterrows():

        behavior_class_label = language_string(row[1]["ClassName"])
        behavior_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", behavior_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                behavior_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in behavior_properties.iterrows():

        behavior_property_label = language_string(row[1]["property"])
        behavior_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", behavior_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
                behavior_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # behaviors worksheet
    for row in behaviors.iterrows():

        behavior_label = language_string(row[1]["behavior"])
        behavior_iri = check_iri(row[1]["behavior"])

        predicates_list = []
        predicates_list.append(("rdf:type", "mhdb:Behavior"))
        predicates_list.append(("rdfs:label", behavior_label))

        if row[1]["index_adverb_prefix"] not in exclude_list and \
                not np.isnan(row[1]["index_adverb_prefix"]):
            index_adverb_prefix = np.int(row[1]["index_adverb_prefix"])
            adverb_prefix = question_preposts[question_preposts["index"] ==
                index_adverb_prefix]["question_prepost"].values[0]
            if isinstance(adverb_prefix, str):
                predicates_list.append(("mhdb:hasQuestionPrefix",
                                        check_iri(adverb_prefix)))

        if row[1]["index_gender"] not in exclude_list and \
                not np.isnan(row[1]["index_gender"]):
            gender_index = np.int(row[1]["index_gender"])
            if gender_index == 2:  # male
                predicates_list.append(
                    ("schema:audience", "MaleAudience"))
            elif gender_index == 3: # female
                predicates_list.append(
                    ("schema:audience", "FemaleAudience"))

        indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
        if isinstance(indices_sign_or_symptom, str) and \
                indices_sign_or_symptom not in exclude_list:
            indices = [np.int(x) for x in
                       indices_sign_or_symptom.strip().split(',') if len(x)>0]
            for index in indices:
                #print(row[1]["index"], index)
                objectRDF = sign_or_symptoms[sign_or_symptoms["index"] ==
                                             index]["sign_or_symptom"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:relatedToMedicalSignOrSymptom",
                                            check_iri(objectRDF)))
        for predicates in predicates_list:
            statements = add_if(
                behavior_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # question_preposts worksheet
    for row in question_preposts.iterrows():

        question_prepost_label = language_string(row[1]["question_prepost"])
        question_prepost_iri = check_iri(row[1]["question_prepost"])

        predicates_list = []
        predicates_list.append(("rdf:type", "mhdb:QuestionPrefix"))
        predicates_list.append(("rdfs:label", question_prepost_label))

        for predicates in predicates_list:
            statements = add_if(
                question_prepost_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_domains(domains_xls, references_xls, statements={}):
    """
    Function to ingest domains spreadsheet

    Parameters
    ----------
    domains_xls: pandas ExcelFile

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

    domain_classes = domains_xls.parse("Classes")
    domain_properties = domains_xls.parse("Properties")
    domains = domains_xls.parse("domains")
    domain_types = domains_xls.parse("domain_types")
    references = references_xls.parse("references")

    statements = audience_statements(statements)

    # Classes worksheet
    for row in domain_classes.iterrows():

        domain_class_label = language_string(row[1]["ClassName"])
        domain_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", domain_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                domain_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in domain_properties.iterrows():

        domain_property_label = language_string(row[1]["property"])
        domain_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", domain_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
                domain_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # domains worksheet
    for row in domains.iterrows():

        domain_label = language_string(row[1]["domain"])
        domain_iri = check_iri(row[1]["domain"])

        predicates_list = []
        predicates_list.append(("rdf:type", "m3-lite:DomainOfInterest"))
        predicates_list.append(("rdfs:label", domain_label))

        indices_domain_type = row[1]["indices_domain_type"]
        if isinstance(indices_domain_type, str) and \
                indices_domain_type not in exclude_list:
            indices = [np.int(x) for x in
                       indices_domain_type.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domain_types[domain_types["index"] ==
                                         index]["domain_type"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasDomainType",
                                            check_iri(objectRDF)))
        indices_domain_category = row[1]["indices_domain_category"]
        if isinstance(indices_domain_category, str) and \
                indices_domain_category not in exclude_list:
            indices = [np.int(x) for x in
                       indices_domain_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains[domains["index"] ==
                                         index]["domain"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                domain_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # domain_types worksheet
    for row in domain_types.iterrows():

        domain_type_label = language_string(row[1]["domain_type"])
        domain_type_iri = check_iri(row[1]["domain_type"])

        predicates_list = []
        predicates_list.append(("rdf:type", "mhdb:DomainType"))
        predicates_list.append(("rdfs:label", domain_type_label))

        for predicates in predicates_list:
            statements = add_if(
                domain_type_iri,
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

    measure_classes = measures_xls.parse("Classes")
    measure_properties = measures_xls.parse("Properties")
    sensors = measures_xls.parse("sensors")
    measures = measures_xls.parse("measures")
    references = references_xls.parse("references")

    statements = audience_statements(statements)

    # Classes worksheet
    for row in measure_classes.iterrows():

        measure_class_label = language_string(row[1]["ClassName"])
        measure_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", measure_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                measure_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in measure_properties.iterrows():

        measure_property_label = language_string(row[1]["property"])
        measure_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", measure_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "ssn:SensingDevice"))
        predicates_list.append(("rdfs:label", sensor_label))

        abbreviation = row[1]["abbreviation"]
        if isinstance(abbreviation, str)and \
                abbreviation not in exclude_list:
            predicates_list.append(("mhdb:hasAbbreviation",
                                    check_iri(abbreviation)))

        indices_measure = row[1]["indices_measure"]
        if isinstance(indices_measure, str) and \
                indices_measure not in exclude_list:
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures.measure[measures["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("ssn:observes",
                                            check_iri(objectRDF)))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "m3-lite:MeasurementType"))
        predicates_list.append(("rdfs:label", measure_label))

        indices_measure_category = row[1]["indices_measure_category"]
        if isinstance(indices_measure_category, str):
            indices = [np.int(x) for x in
                       indices_measure_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures[measures["index"] ==
                                     index]["measure"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF)))

        for predicates in predicates_list:
            statements = add_if(
                measure_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_claims(claims_xls, domains_xls, measures_xls, references_xls,
                  statements={}):
    """
    Function to ingest claims spreadsheet

    Parameters
    ----------
    claims_xls: pandas ExcelFile

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

    claim_classes = claims_xls.parse("Classes")
    claim_properties = claims_xls.parse("Properties")
    sensors = measures_xls.parse("sensors")
    measures = measures_xls.parse("measures")
    domains = domains_xls.parse("domains")
    claims = claims_xls.parse("claims")
    references = references_xls.parse("references")

    statements = audience_statements(statements)

    # Classes worksheet
    for row in claim_classes.iterrows():

        claim_class_label = language_string(row[1]["ClassName"])
        claim_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", claim_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                claim_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in claim_properties.iterrows():

        claim_property_label = language_string(row[1]["property"])
        claim_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", claim_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
                claim_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # claims worksheet
    for row in claims.iterrows():

        claim_label = language_string(row[1]["claim"])
        claim_iri = mhdb_iri(row[1]["claim"])

        predicates_list = []
        predicates_list.append(("rdf:type", "mhdb:Claim"))
        predicates_list.append(("rdfs:label", claim_label))

        indices_domain = row[1]["indices_domain"]
        if isinstance(indices_domain, str) and \
                indices_domain not in exclude_list:
            indices = [np.int(x) for x in
                       indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains[domains["index"] ==
                                    index]["domain"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:makesClaimAbout",
                                            check_iri(objectRDF)))

        indices_measure = row[1]["indices_measure"]
        if isinstance(indices_measure, str) and \
                indices_measure not in exclude_list:
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures[measures["index"] ==
                                     index]["measure"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:makesClaimAbout",
                                            check_iri(objectRDF)))

        indices_sensor = row[1]["indices_sensor"]
        if isinstance(indices_sensor, str) and \
                indices_sensor not in exclude_list:
            indices = [np.int(x) for x in
                       indices_sensor.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sensors[sensors["index"] ==
                                     index]["sensor"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:makesClaimAbout",
                                            check_iri(objectRDF)))

        indices_reference = row[1]["indices_references"]
        if isinstance(indices_reference, str) and \
                indices_reference not in exclude_list:
            indices = [np.int(x) for x in
                       indices_reference.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = references[references["index"] ==
                                       index]["reference"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("dcterms:source",
                                            check_iri(objectRDF)))

        if isinstance(row[1]["link"], str) and \
                row[1]["link"] not in exclude_list:
            predicates_list.append(("dcterms:source",
                                    check_iri(row[1]["link"])))

        for predicates in predicates_list:
            statements = add_if(
                claim_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_references(references_xls, domains_xls, statements={}):
    """
    Function to ingest references spreadsheet

    Parameters
    ----------
    domains_xls: pandas ExcelFile

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

    reference_classes = references_xls.parse("Classes")
    reference_properties = references_xls.parse("Properties")
    references = references_xls.parse("references")
    reference_types = references_xls.parse("reference_types")
    studies_or_clinics = references_xls.parse("studies_or_clinics")
    age_groups = references_xls.parse("age_groups")
    informants = references_xls.parse("informants")
    domains = domains_xls.parse("domains")

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in reference_classes.iterrows():

        reference_class_label = language_string(row[1]["ClassName"])
        reference_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", reference_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and not \
                isinstance(row[1]["DefinitionReference_index"], float):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and not \
                    isinstance(row[1]["Definition"], float):
            predicates_list.append(("rdfs:comment",
                                    check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                reference_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in reference_properties.iterrows():

        reference_property_label = language_string(row[1]["property"])
        reference_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", reference_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and not \
                    isinstance(row[1]["propertyDomain"], float):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and not \
                    isinstance(row[1]["propertyRange"], float):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        # if row[1]["Definition"] not in exclude_list and not \
        #                     isinstance(index_source, float):
        #     predicates_list.append(("rdfs:comment",
        #                             check_iri(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and not \
                    isinstance(row[1]["sameAs"], float):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and not \
                    isinstance(row[1]["equivalentProperty"], float):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and not \
                    isinstance(row[1]["subPropertyOf"], float):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_if(
                reference_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # references worksheet
    for row in references.iterrows():

        source = row[1]["link"]
        if isinstance(source, float):
            source = mhdb_iri(row[1]["reference"])
        else:
            source = check_iri(source)

        predicates_list = []
        predicates_list.append(("rdfs:label",
                                language_string(row[1]["reference"])))
        predicates_list.append(("rdf:type", "dcterms:source"))

        PubMedID = row[1]["PubMedID"]
        abbreviation = row[1]["abbreviation"]
        description = row[1]["description"]
        link = row[1]["link"]
        number_of_questions = row[1]["number_of_questions"]
        minutes_to_complete = row[1]["minutes_to_complete"]
        age_min = row[1]["age_min"]
        age_max = row[1]["age_max"]

        if not isinstance(PubMedID, float) and PubMedID not in exclude_list:
            predicates_list.append(("fabio:hasPubMedId",
                                    check_iri(PubMedID)))
        if isinstance(abbreviation, str) and abbreviation not in exclude_list:
            predicates_list.append(("mhdb:hasAbbreviation",
                                    check_iri(abbreviation)))
        if isinstance(description, str) and description not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(description)))
        if isinstance(link, str) and link not in exclude_list:
            predicates_list.append(("schema:WebSite", check_iri(link)))
        # if isinstance(number_of_questions, str) and \
        #         number_of_questions not in exclude_list:
        #     predicates_list.append(("mhdb:hasNumberOfQuestions",
        #             language_string(number_of_questions)))
        # if not isinstance(minutes_to_complete, float) and \
        #         minutes_to_complete not in exclude_list:
        #     predicates_list.append(("mhdb:takesMinutesToComplete",
        #             language_string(minutes_to_complete)))
        # if not isinstance(age_min, float) and age_min not in exclude_list:
        #     predicates_list.append(("schema:requiredMinAge",
        #             language_string(age_min)))
        # if not isinstance(age_max, float) and age_max not in exclude_list:
        #     predicates_list.append(("schema:requiredMaxAge",
        #             language_string(age_max)))
        if isinstance(number_of_questions, str) and \
                number_of_questions not in exclude_list:
            if "-" in number_of_questions:
                predicates_list.append(("mhdb:hasNumberOfQuestions",
                    '"{0}"^^xsd:string'.format(number_of_questions)))
            else:
                predicates_list.append(("mhdb:hasNumberOfQuestions",
                    '"{0}"^^xsd:nonNegativeInteger'.format(number_of_questions)))
        if not isinstance(minutes_to_complete, float) and \
                minutes_to_complete not in exclude_list:
            if "-" in minutes_to_complete:
                predicates_list.append(("mhdb:takesMinutesToComplete",
                    '"{0}"^^xsd:string'.format(minutes_to_complete)))
            else:
                predicates_list.append(("mhdb:takesMinutesToComplete",
                    '"{0}"^^xsd:nonNegativeInteger'.format(minutes_to_complete)))
        if not isinstance(age_min, float) and age_min not in exclude_list:
            if "-" in age_min:
                predicates_list.append(("schema:requiredMinAge",
                    '"{0}"^^xsd:string'.format(age_min)))
            else:
                predicates_list.append(("schema:requiredMinAge",
                    '"{0}"^^xsd:nonNegativeInteger'.format(age_min)))
        if not isinstance(age_max, float) and age_max not in exclude_list:
            if "-" in age_max:
                predicates_list.append(("schema:requiredMaxAge",
                    '"{0}"^^xsd:string'.format(age_max)))
            else:
                predicates_list.append(("schema:requiredMaxAge",
                    '"{0}"^^xsd:nonNegativeInteger'.format(age_max)))

        indices_reference_type = row[1]["indices_reference_type"]
        indices_study_or_clinic = row[1]["indices_study_or_clinic"]
        indices_domain = row[1]["indices_domain"]
        indices_informants = row[1]["indices_informants"]
        indices_age_group = row[1]["indices_age_group"]
        indices_cited_references = row[1]["indices_cited_references"]
        # Access from other spreadsheets (e.g., projects->people->references)
        #indices_people = row[1]["indices_people"]
        #indices_sensor = row[1]["indices_sensor"]
        #indices_measure = row[1]["indices_measure"]

        if not np.isnan(indices_reference_type) and \
                indices_reference_type not in exclude_list:
            if isinstance(indices_reference_type, str):
                indices = [np.int(x) for x in
                           indices_reference_type.strip().split(',') if len(x)>0]
            elif isinstance(indices_reference_type, float):
                indices = [np.int(indices_reference_type)]
            for index in indices:
                objectRDF = reference_types[
                    reference_types["index"] == index]["reference_type"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:hasReferenceType",
                                            check_iri(objectRDF)))
        if isinstance(indices_study_or_clinic, str) and \
                indices_study_or_clinic not in exclude_list:
            if isinstance(indices_study_or_clinic, str):
                indices = [np.int(x) for x in
                           indices_study_or_clinic.strip().split(',') if len(x)>0]
            elif isinstance(indices_study_or_clinic, float):
                indices = [np.int(indices_study_or_clinic)]
            for index in indices:
                objectRDF = studies_or_clinics[
                    studies_or_clinics["index"] == index]["study_or_clinic"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:usedByStudyOrClinic",
                                            check_iri(objectRDF)))
        if isinstance(indices_domain, str) and \
                indices_domain not in exclude_list:
            indices = [np.int(x) for x in
                       indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains[domains["index"] == index]["domain"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:isAboutDomain",
                                            check_iri(objectRDF)))
        if isinstance(indices_informants, str) and \
                indices_informants not in exclude_list:
            indices = [np.int(x) for x in
                       indices_informants.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = informants[informants["index"] == index]["informant"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:isForInformant",
                                            check_iri(objectRDF)))
        if isinstance(indices_age_group, str) and \
                indices_age_group not in exclude_list:
            indices = [np.int(x) for x in
                       indices_age_group.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = age_groups[
                    age_groups["index"] == index]["age_group"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("mhdb:isForAgeGroup",
                                            check_iri(objectRDF)))
        if isinstance(indices_cited_references, str) and \
                indices_cited_references not in exclude_list:
            indices = [np.int(x) for x in
                       indices_cited_references.strip().split(',') if len(x)>0]
            for index in indices:
                ref_source = references[
                    references["index"] == index]["link"].values[0]
                if isinstance(ref_source, float):
                    ref_source = references[
                        references["index"] == index]["reference"].values[0]
                    ref_source = mhdb_iri(ref_source)
                else:
                    ref_source = mhdb_iri(ref_source)
                predicates_list.append(("dcterms:isReferencedBy", ref_source))

        for predicates in predicates_list:
            statements = add_if(
                source,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # reference_types worksheet
    for row in reference_types.iterrows():

        reference_type_label = language_string(row[1]["reference_type"])

        if isinstance(row[1]["IRI"], str) and \
            row[1]["IRI"] not in exclude_list:
            reference_type_iri = check_iri(row[1]["IRI"])
        elif isinstance(row[1]["reference_type"], str) and \
            row[1]["reference_type"] not in exclude_list:
            reference_type_iri = check_iri(row[1]["reference_type"])

        predicates_list = []
        predicates_list.append(("rdfs:label",
                                language_string(reference_type_label)))
        predicates_list.append(("rdf:type", "mhdb:ReferenceType"))

        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_if(
                reference_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    #statements = projects(statements=statements)

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

    statements = audience_statements(statements)

    # Classes worksheet
    for row in dsm_classes.iterrows():

        dsm_class_label = language_string(row[1]["ClassName"])
        dsm_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", dsm_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))

        source = None
        if row[1]["DefinitionReference_index"] not in exclude_list and \
            not math.isnan(row[1]["DefinitionReference_index"]):
            index_source = references[
                    references["index"] == row[1]["DefinitionReference_index"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and \
                not math.isnan(index_source):
                source = references[references["index"] ==
                                    index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)
            #predicates_list.append(("dcterms:source", source))
            predicates_list.append(("rdfs:isDefinedBy", source))

        if row[1]["Definition"] not in exclude_list and \
            isinstance(row[1]["Definition"], str):
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["Definition"])))
        if row[1]["sameAs"] not in exclude_list and \
            isinstance(row[1]["Definition"], str):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and \
            isinstance(row[1]["equivalentClass_2"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
                dsm_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in dsm_properties.iterrows():

        dsm_property_label = language_string(row[1]["property"])
        dsm_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", dsm_property_label))
        predicates_list.append(("rdf:type", "rdf:Property"))

        if row[1]["propertyDomain"] not in exclude_list and \
            isinstance(row[1]["propertyDomain"], str):
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list and \
            isinstance(row[1]["propertyRange"], str):
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        if row[1]["sameAs"] not in exclude_list and \
            isinstance(row[1]["sameAs"], str):
            predicates_list.append(("owl:sameAs",
                                    check_iri(row[1]["sameAs"])))
        if row[1]["equivalentProperty"] not in exclude_list and \
            isinstance(row[1]["equivalentProperty"], str):
            predicates_list.append(("rdfs:equivalentProperty",
                                    check_iri(row[1]["equivalentProperty"])))
        if row[1]["subPropertyOf"] not in exclude_list and \
            isinstance(row[1]["subPropertyOf"], str):
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        if row[1]["health-lifesci_codingSystem"] not in exclude_list and \
            isinstance(row[1]["health-lifesci_codingSystem"], str):
            predicates_list.append(("health-lifesci:codingSystem",
                                    check_iri(row[1]["health-lifesci_codingSystem"])))
        for predicates in predicates_list:
            statements = add_if(
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
        if row[1]["index_reference"] not in exclude_list and \
            not math.isnan(row[1]["index_reference"]):
            source = references[
                    references["index"] == row[1]["index_reference"]
                ]["link"].values[0]
            if source not in exclude_list and \
                isinstance(source, str):
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)

        symptom_label = language_string(row[1]["sign_or_symptom"])
        symptom_iri = check_iri(row[1]["sign_or_symptom"])

        predicates_list = []
        predicates_list.append(("rdfs:label", symptom_label))
        predicates_list.append(("rdf:type", sign_or_symptom))
        predicates_list.append(("dcterms:source", source))

        # specific to females/males?
        import math
        if row[1]["index_gender"] not in exclude_list and \
            not math.isnan(row[1]["index_gender"]):
            if np.int(row[1]["index_gender"]) == 1:  # female
                predicates_list.append(
                    ("schema:audience", "mhdb:FemaleAudience"))
                predicates_list.append(
                    ("schema:epidemiology", "mhdb:FemaleAudience"))
            elif np.int(row[1]["index_gender"]) == 2:  # male
                predicates_list.append(
                    ("schema:audience", "mhdb:MaleAudience"))
                predicates_list.append(
                    ("schema:epidemiology", "mhdb:MaleAudience"))

        # indices for disorders
        indices_disorder = row[1]["indices_disorder"]
        if indices_disorder not in exclude_list and \
            not math.isnan(indices_disorder):
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
        if indices_sign_or_symptom not in exclude_list and \
            not math.isnan(indices_sign_or_symptom):
            if isinstance(indices_sign_or_symptom, str):
                indices_sign_or_symptom = [np.int(x) for x in
                           indices_sign_or_symptom.strip().split(',') if len(x) > 0]
            elif isinstance(indices_sign_or_symptom, int):
                indices_sign_or_symptom = [indices_sign_or_symptom]
            for index in indices_sign_or_symptom:
                super_sign = sign_or_symptoms[sign_or_symptoms["index"] == index
                                             ]["sign_or_symptom"].values[0]
                if isinstance(super_sign, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(super_sign)))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:ExampleSignOrSymptom"))

        indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
        if indices_sign_or_symptom not in exclude_list and \
            not math.isnan(indices_sign_or_symptom):
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
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:DisorderSeverity"))

        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:DiagnosticSpecifier"))

        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:DiagnosticCriterion"))

        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and \
            isinstance(row[1]["equivalentClass_2"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:Disorder"))

        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        if row[1]["subClassOf_2"] not in exclude_list and \
            isinstance(row[1]["subClassOf_2"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf_2"])))
        if row[1]["note"] not in exclude_list and \
            isinstance(row[1]["note"], str):
            predicates_list.append(("mhdb:hasNote",
                                    language_string(row[1]["note"])))

        if row[1]["ICD9_code"] not in exclude_list and \
            not math.isnan(row[1]["ICD9_code"]):
            ICD9_code = str(row[1]["ICD9_code"])
            predicates_list.append(("mhdb:hasICD9Code",
                                    check_iri('ICD9_' + ICD9_code)))
            disorder_label += "; ICD9:{0}".format(ICD9_code)
            disorder_iri_label += " ICD9 {0}".format(ICD9_code)
        if row[1]["ICD10_code"] not in exclude_list and \
            not math.isnan(row[1]["ICD10_code"]):
            ICD10_code = row[1]["ICD10_code"]
            predicates_list.append(("mhdb:hasICD10Code",
                                    check_iri('ICD10_' + ICD10_code)))
            disorder_label += "; ICD10:{0}".format(ICD10_code)
            disorder_iri_label += " ICD10 {0}".format(ICD10_code)
        if row[1]["index_diagnostic_specifier"] not in exclude_list and \
            not math.isnan(row[1]["index_diagnostic_specifier"]):
            diagnostic_specifier = diagnostic_specifiers[
            diagnostic_specifiers["index"] == int(row[1]["index_diagnostic_specifier"])
            ]["diagnostic_specifier"].values[0]
            if isinstance(diagnostic_specifier, str):
                predicates_list.append(("mhdb:hasDiagnosticSpecifier",
                                        check_iri(diagnostic_specifier)))
                disorder_label += "; specifier: {0}".format(diagnostic_specifier)
                disorder_iri_label += " specifier {0}".format(diagnostic_specifier)

        if row[1]["index_diagnostic_inclusion_criterion"] not in exclude_list and \
            not math.isnan(row[1]["index_diagnostic_inclusion_criterion"]):
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

        if row[1]["index_diagnostic_inclusion_criterion2"] not in exclude_list and \
            not math.isnan(row[1]["index_diagnostic_inclusion_criterion2"]):
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

        if row[1]["index_diagnostic_exclusion_criterion"] not in exclude_list and \
            not math.isnan(row[1]["index_diagnostic_exclusion_criterion"]):
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

        if row[1]["index_diagnostic_exclusion_criterion2"] not in exclude_list and \
            not math.isnan(row[1]["index_diagnostic_exclusion_criterion2"]):
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

        if row[1]["index_severity"] not in exclude_list and \
            not math.isnan(row[1]["index_severity"]):
            severity = severities[
            severities["index"] == int(row[1]["index_severity"])
            ]["severity"].values[0]
            if isinstance(severity, str) and severity not in exclude_list and not \
                    isinstance(severity, float):
                predicates_list.append(("mhdb:hasSeverity",
                                        check_iri(severity)))
                disorder_label += \
                    "; severity: {0}".format(severity)
                disorder_iri_label += \
                    " severity {0}".format(severity)

        if row[1]["index_disorder_subsubsubcategory"] not in exclude_list and \
            not math.isnan(row[1]["index_disorder_subsubsubcategory"]):
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
            statements = add_if(
                check_iri(disorder_subsubsubcategory),
                "rdfs:subClassOf",
                check_iri(disorder_subsubcategory),
                statements,
                exclude_list
            )
            if disorder_subsubcategory not in exclude_categories:
                statements = add_if(
                    check_iri(disorder_subsubcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_subcategory),
                    statements,
                    exclude_list
                )
                statements = add_if(
                    check_iri(disorder_subcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_category),
                    statements,
                    exclude_list
                )
                exclude_categories.append(disorder_subsubcategory)
        elif row[1]["index_disorder_subsubcategory"] not in exclude_list and \
            not math.isnan(row[1]["index_disorder_subsubcategory"]):
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
            statements = add_if(
                check_iri(disorder_subsubcategory),
                "rdfs:subClassOf",
                check_iri(disorder_subcategory),
                statements,
                exclude_list
            )
            if disorder_subcategory not in exclude_categories:
                statements = add_if(
                    check_iri(disorder_subcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_category),
                    statements,
                    exclude_list
                )
                exclude_categories.append(disorder_subcategory)
        elif row[1]["index_disorder_subcategory"] not in exclude_list and \
            not math.isnan(row[1]["index_disorder_subcategory"]):
            disorder_subcategory = disorder_subcategories[
                disorder_subcategories["index"] == int(row[1]["index_disorder_subcategory"])
            ]["disorder_subcategory"].values[0]
            disorder_category = disorder_categories[
                disorder_categories["index"] == int(row[1]["index_disorder_category"])
            ]["disorder_category"].values[0]
            predicates_list.append(("mhdb:hasDisorderCategory",
                                    check_iri(disorder_subcategory)))
            if disorder_category not in exclude_categories:
                statements = add_if(
                    check_iri(disorder_subcategory),
                    "rdfs:subClassOf",
                    check_iri(disorder_category),
                    statements,
                    exclude_list
                )
                exclude_categories.append(disorder_category)
        elif row[1]["index_disorder_category"] not in exclude_list and \
            not math.isnan(row[1]["index_disorder_category"]):
            disorder_category = disorder_categories[
                disorder_categories["index"] == int(row[1]["index_disorder_category"])
            ]["disorder_category"].values[0]
            predicates_list.append(("mhdb:hasDisorderCategory",
                                    check_iri(disorder_category)))

        disorder_label = language_string(disorder_label)
        disorder_iri = check_iri(disorder_iri_label)
        predicates_list.append(("rdfs:label", disorder_label))
        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and \
            isinstance(row[1]["equivalentClass_2"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list and \
                isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
                isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_if(
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
        predicates_list.append(("rdf:type", "mhdb:DisorderCategory"))

        if row[1]["equivalentClass"] not in exclude_list and \
            isinstance(row[1]["equivalentClass"], str):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
            isinstance(row[1]["subClassOf"], str):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))

        for predicates in predicates_list:
            statements = add_if(
                disorder_subsubsubcategory_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements



