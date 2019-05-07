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


def ingest_dsm5(dsm5_xls, behaviors_xls, references_xls, technologies_xls,
                statements={}):
    """
    Function to ingest dsm5 spreadsheet

    Parameters
    ----------
    dsm5_xls: pandas ExcelFile

    behaviors_xls: pandas ExcelFile

    references_xls: pandas ExcelFile

    technologies_xls: pandas ExcelFile

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
    severities = dsm5_xls.parse("severities")
    disorders = dsm5_xls.parse("disorders")
    disorder_categories = dsm5_xls.parse("disorder_categories")
    disorder_subcategories = dsm5_xls.parse("disorder_subcategories")
    disorder_subsubcategories = dsm5_xls.parse("disorder_subsubcategories")
    disorder_subsubsubcategories = dsm5_xls.parse("disorder_subsubsubcategories")
    diagnostic_specifiers = dsm5_xls.parse("diagnostic_specifiers")
    diagnostic_criteria = dsm5_xls.parse("diagnostic_criteria")
    references = references_xls.parse("references")
    links = technologies_xls.parse("links")

    statements = audience_statements(statements)

    # sign_or_symptoms worksheet
    for row in sign_or_symptoms.iterrows():
        sign_or_symptom = "health-lifesci:MedicalSign" \
            if (row[1]["index_sign_or_symptom_index"]) == 1 \
            else "health-lifesci:MedicalSymptom" \
            if (row[1]["index_sign_or_symptom_index"] == 2) \
            else "health-lifesci:MedicalSignOrSymptom"

        source = None
        if row[1]["index_reference"] not in exclude_list and not \
                isinstance(row[1]["index_reference"], float):
            index_source = references[
                    references["index"] == row[1]["index_reference"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = links[links["index"] == index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)

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
        #             statements,
        #             exclude_list
        #         )

        symptom_label = language_string(row[1]["sign_or_symptom"])
        symptom_iri = check_iri(row[1]["sign_or_symptom"])

        predicates_list = []
        predicates_list.append(("rdfs:label", symptom_label))
        predicates_list.append(("rdfs:subClassOf", sign_or_symptom))
        predicates_list.append(("dcterms:source", source))
        indices_disorder = row[1]["indices_disorder"]
        if indices_disorder not in exclude_list:
            if not isinstance(indices_disorder, list):
                indices_disorder = [indices_disorder]
            for index in indices_disorder:
                disorder = disorders[disorders["index"] == index]["disorder"].values[0]
                if isinstance(disorder, str):
                    predicates_list.append(("rdfs:disorderBLOOP",
                                            check_iri(disorder)))
        index_super_sign = row[1]["sign_or_symptom_index"]
        if index_super_sign not in exclude_list and not np.isnan(index_super_sign):
            super_sign = sign_or_symptoms[sign_or_symptoms["index"] ==
                                          np.int(index_super_sign)
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

    # severities worksheet
    for row in severities.iterrows():

        severity_label = language_string(row[1]["severity"])
        severity_iri = check_iri(row[1]["severity"])

        predicates_list = []
        predicates_list.append(("rdfs:label", severity_label))
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
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
        if row[1]["equivalentClass"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list:
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
            statements = add_if(
                diagnostic_criterion_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # disorders worksheet
    for row in disorders.iterrows():

        disorder_label = language_string(row[1]["disorder"])
        disorder_iri = check_iri(row[1]["disorder"])

        predicates_list = []
        predicates_list.append(("rdfs:label", disorder_label))
        if row[1]["equivalentClass"] not in exclude_list and \
                not isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
                not isinstance(row[1]["subClassOf"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        if row[1]["subClassOf_2"] not in exclude_list and \
                not isinstance(row[1]["subClassOf_2"], float):
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf_2"])))
        # if row[1]["disorder_full_name"] not in exclude_list and \
        #         not isinstance(row[1]["disorder_full_name"], float):
        #     predicates_list.append(("disorder_full_nameBLOOP",
        #                             language_string(row[1]["disorder_full_name"])))
        if row[1]["ICD9_code"] not in exclude_list and \
                row[1]["ICD9_code"] != np.nan:
            predicates_list.append(("hasICD10Code",
                                    check_iri('ICD9_' + str(row[1]["ICD9_code"]))))
        if row[1]["ICD10_code"] not in exclude_list and \
                row[1]["ICD10_code"] != np.nan:
            predicates_list.append(("hasICD10Code",
                                    check_iri('ICD10_' + str(row[1]["ICD10_code"]))))
        if row[1]["note"] not in exclude_list and \
                not isinstance(row[1]["note"], float):
            predicates_list.append(("noteBLOOP",
                                    language_string(row[1]["note"])))

        if row[1]["index_disorder_category"] not in exclude_list and \
                row[1]["index_disorder_category"] != np.nan:
            disorder_category = disorder_categories[
            disorder_categories["index"] == row[1]["index_disorder_category"]
            ]["disorder_category"].values[0]
            if isinstance(disorder_category, str):
                predicates_list.append(("rdfs:disorder_categoryBLOOP",
                                        check_iri(disorder_category)))

        if row[1]["index_disorder_subcategory"] not in exclude_list and \
                not np.isnan(row[1]["index_disorder_subcategory"]):
            disorder_subcategory = disorder_subcategories[
                disorder_subcategories["index"] == int(row[1]["index_disorder_subcategory"])
            ]["disorder_subcategory"].values[0]
            if isinstance(disorder_subcategory, str):
                predicates_list.append(("rdfs:disorder_subcategoryBLOOP",
                                        check_iri(disorder_subcategory)))

        if row[1]["index_disorder_subsubcategory"] not in exclude_list and \
                not np.isnan(row[1]["index_disorder_subsubcategory"]):
            disorder_subsubcategory = disorder_subsubcategories[
            disorder_subsubcategories["index"] == int(row[1]["index_disorder_subsubcategory"])
            ]["disorder_subsubcategory"].values[0]
            if isinstance(disorder_subsubcategory, str):
                predicates_list.append(("rdfs:disorder_subsubcategoryBLOOP",
                                        check_iri(disorder_subsubcategory)))

        if row[1]["index_disorder_subsubsubcategory"] not in exclude_list and \
                not np.isnan(row[1]["index_disorder_subsubsubcategory"]):
            disorder_subsubsubcategory = disorder_subsubsubcategories[
            disorder_subsubsubcategories["index"] == int(row[1]["index_disorder_subsubsubcategory"])
            ]["disorder_subsubsubcategory"].values[0]
            if isinstance(disorder_subsubsubcategory, str):
                predicates_list.append(("rdfs:disorder_subsubsubcategoryBLOOP",
                                        check_iri(disorder_subsubsubcategory)))

        if row[1]["index_diagnostic_specifier"] not in exclude_list and \
                not np.isnan(row[1]["index_diagnostic_specifier"]):
            diagnostic_specifier = diagnostic_specifiers[
            diagnostic_specifiers["index"] == int(row[1]["index_diagnostic_specifier"])
            ]["diagnostic_specifier"].values[0]
            if isinstance(diagnostic_specifier, str):
                predicates_list.append(("rdfs:hasDiagnosticSpecifier",
                                        check_iri(diagnostic_specifier)))

        if row[1]["index_diagnostic_inclusion_criterion"] not in exclude_list and \
                not np.isnan(row[1]["index_diagnostic_inclusion_criterion"]):
            diagnostic_inclusion_criterion = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_inclusion_criterion"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_inclusion_criterion, str):
                predicates_list.append(("rdfs:hasInclusionCriterion",
                                        check_iri(diagnostic_inclusion_criterion)))

        if row[1]["index_diagnostic_inclusion_criterion2"] not in exclude_list and \
                not np.isnan(row[1]["index_diagnostic_inclusion_criterion2"]):
            diagnostic_inclusion_criterion2 = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_inclusion_criterion2"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_inclusion_criterion2, str):
                predicates_list.append(("rdfs:hasInclusionCriterion",
                                        check_iri(diagnostic_inclusion_criterion2)))

        if row[1]["index_diagnostic_exclusion_criterion"] not in exclude_list and \
                not np.isnan(row[1]["index_diagnostic_exclusion_criterion"]):
            diagnostic_exclusion_criterion = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_exclusion_criterion"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_exclusion_criterion, str):
                predicates_list.append(("rdfs:hasExclusionCriterion",
                                        check_iri(diagnostic_exclusion_criterion)))

        if row[1]["index_diagnostic_exclusion_criterion2"] not in exclude_list and \
                not np.isnan(row[1]["index_diagnostic_exclusion_criterion2"]):
            diagnostic_exclusion_criterion2 = diagnostic_criteria[
            diagnostic_criteria["index"] == int(row[1]["index_diagnostic_exclusion_criterion2"])
            ]["diagnostic_criterion"].values[0]
            if isinstance(diagnostic_exclusion_criterion2, str):
                predicates_list.append(("rdfs:hasExclusionCriterion",
                                        check_iri(diagnostic_exclusion_criterion2)))

        if row[1]["index_severity"] not in exclude_list and \
                not np.isnan(row[1]["index_severity"]):
            severity = severities[
            severities["index"] == int(row[1]["index_severity"])
            ]["severity"].values[0]
            if isinstance(severity, str) and severity not in exclude_list:
                predicates_list.append(("rdfs:hasSeverity",
                                        check_iri(severity)))

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
        if row[1]["equivalentClass"] not in exclude_list and \
                not isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["equivalentClass_2"] not in exclude_list and \
                not isinstance(row[1]["equivalentClass_2"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass_2"])))
        if row[1]["subClassOf"] not in exclude_list and \
                not isinstance(row[1]["subClassOf"], float):
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
        if row[1]["equivalentClass"] not in exclude_list and \
                not isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
                not isinstance(row[1]["subClassOf"], float):
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
        if row[1]["equivalentClass"] not in exclude_list and \
                not isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
                not isinstance(row[1]["subClassOf"], float):
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
        if row[1]["equivalentClass"] not in exclude_list and \
                not isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and \
                not isinstance(row[1]["subClassOf"], float):
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


def ingest_assessments(assessments_xls, behaviors_xls,
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
    task_groups = assessments_xls.parse("task_groups")
    presentations = assessments_xls.parse("presentations")
    domains = behaviors_xls.parse("domains")
    projects = technologies_xls.parse("projects")
    references = references_xls.parse("references")
    links = technologies_xls.parse("links")

    #statements = audience_statements(statements)

    # questions worksheet
    for row in questions.iterrows():

        source = None
        if row[1]["index_reference"] not in exclude_list and not \
                isinstance(row[1]["index_reference"], float):
            index_source = references[
                    references["index"] == row[1]["index_reference"]
                ]["indices_link"].values[0]
            if index_source not in exclude_list and not \
                    isinstance(index_source, float):
                source = links[links["index"] == index_source]["link"].values[0]
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
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
                statements,
                exclude_list
            )

        predicates_list = []

        instructions = row[1]["instructions"]
        group_instructions = row[1]["question_group_instructions"]
        response_options = row[1]["response_options"]

        if instructions not in exclude_list and \
                isinstance(instructions, str):
            predicates_list.append(("rdfs:instructionsBLOOP",
                                    language_string(instructions)))
        if group_instructions not in exclude_list and \
                isinstance(group_instructions, str):
            predicates_list.append(("rdfs:grpinstructionsBLOOP",
                                    language_string(group_instructions)))
        if response_options not in exclude_list and \
                isinstance(response_options, str):
            predicates_list.append(("rdfs:responseoptionsBLOOP",
                                    language_string(response_options)))

        response_type = response_types.response_type[
            response_types["index"] == row[1]["index_response_type"]
        ]
        if response_type not in exclude_list and \
                isinstance(response_type, str):
            predicates_list.append(("rdfs:responsetypeBLOOP",
                                    language_string(response_type)))

        for predicates in predicates_list:
            statements = add_if(
                question_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
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
                statements,
                exclude_list
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
        if isinstance(respond_to_advance, str):
            predicates_list.append(("rdfs:respond_to_advanceBLOOP",
                                    language_string(respond_to_advance)))
        if isinstance(response_affects_presentation, str):
            predicates_list.append(("rdfs:response_affects_presentationBLOOP",
                                    language_string(response_affects_presentation)))
        if isinstance(response_based_feedback, str):
            predicates_list.append(("rdfs:response_based_feedbackBLOOP",
                                    language_string(response_based_feedback)))
        if isinstance(animation, str):
            predicates_list.append(("rdfs:animationBLOOP",
                                    language_string(animation)))

        indices_domain = row[1]["indices_domain"]
        indices_project = row[1]["indices_project"]
        indices_task_groups = row[1]["indices_task_groups"]
        indices_presentations = row[1]["indices_presentations"]

        if isinstance(indices_domain, str):
            indices = [np.int(x) for x in indices_domain.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = domains.domain[domains["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:domainBLOOP",
                                            language_string(objectRDF)))
        if isinstance(indices_project, str):
            indices = [np.int(x) for x in indices_project.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = projects[projects["index"] == index]
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

        for predicates in predicates_list:
            statements = add_if(
                task_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements

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
                statements,
                exclude_list
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
                technology_type_label = technology_types.technology_type[
                    technology_types["index"] == index]
                if isinstance(technology_type_label, str):
                    predicates_list.append(("rdfs:technology_typeBLOOP",
                                            language_string(technology_type_label)))
                technology_type_iri = technology_types.IRI[technology_types["index"] == index]
                if isinstance(technology_type_iri, float):
                    technology_type_iri = technology_type_label
                if isinstance(technology_type_iri, str):
                    predicates_list.append(("rdfs:technology_type_iriBLOOP",
                                            check_iri(technology_type_iri)))
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
            statements,
            exclude_list
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
                statements,
                exclude_list
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
                    statements,
                    exclude_list
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
                        statements,
                        exclude_list
                    )

                statements = add_if(
                    person_iri,
                    "dcterms:contributor",
                    affiliate_iri,
                    statements,
                    exclude_list
                )

    return statements


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
                statements,
                exclude_list
            )

        predicates_list = []

        adverb_prefix = behavior_question_prepends.question_prepend[
            behavior_question_prepends["index"] == row[1]["index_abverb_prefix"]
        ]
        if isinstance(adverb_prefix, str):
            predicates_list.append(("rdfs:adverb_prefixBLOOP",
                                    language_string(adverb_prefix)))
        adverb_suffix = behavior_question_prepends.question_prepend[
            behavior_question_prepends["index"] == row[1]["index_adverb_suffix"]
        ]
        if isinstance(adverb_suffix, str):
            predicates_list.append(("rdfs:adverb_suffixBLOOP",
                                    language_string(adverb_suffix)))

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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
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
                statements,
                exclude_list
            )

    return statements


def projects(statements={}):
    """
    Function to create rdf statements about projects

    Parameters
    ----------
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
    """

    for subject in [
        "schema:Book",
        "schema:Article"
    ]:
        statements = add_if(
            subject,
            "rdfs:subClassOf",
            "mhdb:BookOrArticle",
            statements,
            exclude_list
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
            statements,
            exclude_list
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
            statements,
            exclude_list
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
            statements,
            exclude_list
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
            statements,
            exclude_list
        )

    for pred in [
        ("rdfs:subClassOf", "schema:Book"),
        ("rdfs:label", language_string("Resource Guide"))
    ]:
        statements = add_if(
            "mhdb:ResourceGuide",
            pred[0],
            pred[1],
            statements,
            exclude_list
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
            statements,
            exclude_list
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
            statements,
            exclude_list
        )

        for pred in [
            ("rdfs:subClassOf", "ssn:Device"),
            ("rdfs:label", language_string("Tablet"))
        ]:
            statements = add_if(
                "mhdb:Tablet",
                pred[0],
                pred[1],
                statements,
                exclude_list
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
            statements,
            exclude_list
        )

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
            statements,
            exclude_list
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
            statements,
            exclude_list
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
            statements,
            exclude_list
        )

    for pred in [
        ("rdfs:label", language_string("Competition")),
        ("rdfs:subClassOf", "schema:Event")
    ]:
        statements = add_if(
            "mhdb:Competition",
            pred[0],
            pred[1],
            statements,
            exclude_list
        )

    for pred in [
        ("rdfs:label", language_string("Science Contest")),
        ("rdfs:subClassOf", "mhdb:Competition")
    ]:
        statements = add_if(
            "mhdb:ScienceContest",
            pred[0],
            pred[1],
            statements,
            exclude_list
        )

    for pred in [
        ("rdfs:label", language_string("Massive Open Online Course")),
        ("rdfs:subClassOf", "schema:Course")
    ]:
        statements = add_if(
            "mhdb:MOOC",
            pred[0],
            pred[1],
            statements,
            exclude_list
        )

    return statements


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
                statements,
                exclude_list
            )

        predicates_list = []

        abbreviation = row[1]["abbreviation"]
        description = row[1]["description"]
        cited_reference = row[1]["cited_reference"]
        number_of_questions = row[1]["number_of_questions"]
        minutes_to_complete = row[1]["minutes_to_complete"]
        age_min = row[1]["age_min"]
        age_max = row[1]["age_max"]

        if isinstance(age_min, str):
            predicates_list.append(("rdfs:age_minBLOOP",
                                    language_string(age_min)))
        if isinstance(age_max, str):
            predicates_list.append(("rdfs:age_maxBLOOP",
                                    language_string(age_max)))

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
        index_age_group = row[1]["index_age_group"]

        if isinstance(index_reference_type, str):
            objectRDF = reference_types.reference_type[
                reference_types["index"] == index_reference_type]
            if isinstance(objectRDF, str):
                predicates_list.append(("rdfs:reference_typeBLOOP",
                                        language_string(objectRDF)))
        if isinstance(index_age_group, str):
            objectRDF = age_groups.age_group[
                age_groups["index"] == index_age_group]
            if isinstance(objectRDF, str):
                predicates_list.append(("rdfs:age_groupBLOOP",
                                        language_string(objectRDF)))

        indices_study_or_clinic = row[1]["indices_study_or_clinic"]
        indices_domain = row[1]["indices_domain"]
        indices_domain_category = row[1]["indices_domain_category"]
        indices_informants = row[1]["indices_informants"]
        indices_keywords = row[1]["indices_keywords"]

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
                statements,
                exclude_list
            )

    # reference_types worksheet
    for row in reference_types.iterrows():

        reference_type_label = language_string(row[1]["reference_type"])
        reference_type_iri = check_iri(row[1]["reference_type"])

        for predicates in [
            ("rdfs:label", reference_type_label)
        ]:
            statements = add_if(
                reference_type_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

        predicates_list = []
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

    statements = projects(statements=statements)

    return statements



