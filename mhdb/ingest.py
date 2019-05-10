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


def ingest_dsm5(dsm5_xls, behaviors_xls, references_xls, projects_xls,
                statements={}):
    """
    Function to ingest dsm5 spreadsheet

    Parameters
    ----------
    dsm5_xls: pandas ExcelFile

    behaviors_xls: pandas ExcelFile

    references_xls: pandas ExcelFile

    projects_xls: pandas ExcelFile

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

    dsm_classes = dsm5_xls.parse("Classes")
    dsm_properties = dsm5_xls.parse("Properties")
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

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in dsm_classes.iterrows():

        dsm_class_label = language_string(row[1]["ClassName"])
        dsm_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", dsm_class_label))
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
        if row[1]["health-lifesci_codingSystem"] not in exclude_list and not \
                    isinstance(row[1]["health-lifesci_codingSystem"], float):
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
        sign_or_symptom = "health-lifesci:MedicalSign" \
            if (row[1]["index_sign_or_symptom_index"]) == 1 \
            else "health-lifesci:MedicalSymptom" \
            if (row[1]["index_sign_or_symptom_index"] == 2) \
            else "health-lifesci:MedicalSignOrSymptom"

        source = None
        if row[1]["index_reference"] not in exclude_list and not \
                isinstance(row[1]["index_reference"], float):
            source = references[
                    references["index"] == row[1]["index_reference"]
                ]["link"].values[0]
            if source not in exclude_list and not \
                    isinstance(source, float):
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
        predicates_list.append(("rdf:type", sign_or_symptom))

        predicates_list.append(("dcterms:source", source))
        indices_disorder = row[1]["indices_disorder"]
        if indices_disorder not in exclude_list:
            if not isinstance(indices_disorder, list):
                indices_disorder = [indices_disorder]
            for index in indices_disorder:
                disorder = disorders[disorders["index"] == index]["disorder"].values[0]
                if isinstance(disorder, str):
                    predicates_list.append(("isIndicatorOfBLOOP",
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
        predicates_list.append(("rdf:type", "mhdb:DisorderSeverity"))

        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
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

        if row[1]["equivalentClass"] not in exclude_list and not \
                    isinstance(row[1]["equivalentClass"], float):
            predicates_list.append(("rdfs:equivalentClass",
                                    check_iri(row[1]["equivalentClass"])))
        if row[1]["subClassOf"] not in exclude_list and not \
                    isinstance(row[1]["subClassOf"], float):
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
        predicates_list.append(("rdf:type", "mhdb:Disorder"))

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
        #     predicates_list.append(("",
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
            predicates_list.append(("hasNote",
                                    language_string(row[1]["note"])))

        if row[1]["index_disorder_category"] not in exclude_list and \
                not np.isnan(row[1]["index_disorder_category"]):
            disorder_category = disorder_categories[
            disorder_categories["index"] == row[1]["index_disorder_category"]
            ]["disorder_category"].values[0]
            if isinstance(disorder_category, str):
                predicates_list.append(("rdfs:hasDisorderCategory",
                                        check_iri(disorder_category)))

        if row[1]["index_disorder_subcategory"] not in exclude_list and \
                not np.isnan(row[1]["index_disorder_subcategory"]):
            disorder_subcategory = disorder_subcategories[
                disorder_subcategories["index"] == int(row[1]["index_disorder_subcategory"])
            ]["disorder_subcategory"].values[0]
            if isinstance(disorder_subcategory, str):
                predicates_list.append(("rdfs:hasDisorderSubcategory",
                                        check_iri(disorder_subcategory)))

        if row[1]["index_disorder_subsubcategory"] not in exclude_list and \
                not np.isnan(row[1]["index_disorder_subsubcategory"]):
            disorder_subsubcategory = disorder_subsubcategories[
            disorder_subsubcategories["index"] == int(row[1]["index_disorder_subsubcategory"])
            ]["disorder_subsubcategory"].values[0]
            if isinstance(disorder_subsubcategory, str):
                predicates_list.append(("rdfs:hasDisorderSubsubcategory",
                                        check_iri(disorder_subsubcategory)))

        if row[1]["index_disorder_subsubsubcategory"] not in exclude_list and \
                not np.isnan(row[1]["index_disorder_subsubsubcategory"]):
            disorder_subsubsubcategory = disorder_subsubsubcategories[
            disorder_subsubsubcategories["index"] == int(row[1]["index_disorder_subsubsubcategory"])
            ]["disorder_subsubsubcategory"].values[0]
            if isinstance(disorder_subsubsubcategory, str):
                predicates_list.append(("rdfs:hasDisorderSubsubsubcategory",
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
            if isinstance(severity, str) and severity not in exclude_list and not \
                    isinstance(severity, float):
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
        predicates_list.append(("rdf:type", "mhdb:DisorderCategory"))

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
        predicates_list.append(("rdf:type", "mhdb:DisorderSubcategory"))

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
        predicates_list.append(("rdf:type", "mhdb:DisorderSubsubcategory"))

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
        predicates_list.append(("rdf:type", "mhdb:DisorderSubsubsubcategory"))

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
                       projects_xls, references_xls, statements={}):
    """
    Function to ingest assessments spreadsheet

    Parameters
    ----------
    assessments_xls: pandas ExcelFile

    dsm5_xls: pandas ExcelFile

    behaviors_xls: pandas ExcelFile

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

    ass_classes = assessments_xls.parse("Classes")
    ass_properties = assessments_xls.parse("Properties")
    tasks = assessments_xls.parse("tasks")
    task_categories = assessments_xls.parse("task_categories")
    presentations = assessments_xls.parse("presentations")
    questions = assessments_xls.parse("questions")
    response_types = assessments_xls.parse("response_types")
    domains = behaviors_xls.parse("domains")
    projects = projects_xls.parse("projects")
    references = references_xls.parse("references")

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in ass_classes.iterrows():

        ass_class_label = language_string(row[1]["ClassName"])
        ass_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", ass_class_label))
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
                ass_class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in ass_properties.iterrows():

        ass_property_label = language_string(row[1]["property"])
        ass_property_iri = check_iri(row[1]["property"])

        predicates_list = []
        predicates_list.append(("rdfs:label", ass_property_label))
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
                ass_property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # questions worksheet
    for row in questions.iterrows():

        source = None
        if row[1]["index_reference"] not in exclude_list and not \
                isinstance(row[1]["index_reference"], float):
            source = references[
                    references["index"] == row[1]["index_reference"]
                ]["link"].values[0]
            if source not in exclude_list and not \
                    isinstance(source, float):
                source = check_iri(source)
            else:
                source = references[
                        references["index"] == row[1]["index_reference"]
                    ]["reference"].values[0]
                source = check_iri(source)

        question_label = language_string(row[1]["question"])
        question_iri = check_iri(row[1]["question"])

        predicates_list = []
        predicates_list.append(("rdfs:label", question_label))
        predicates_list.append(("rdf:type", "Question"))
        predicates_list.append(("dcterms:source", source))

        instructions = row[1]["instructions"]
        group_instructions = row[1]["question_group_instructions"]
        digital_instructions = row[1]["digital_instructions"]
        digital_group_instructions = row[1]["digital_group_instructions"]
        response_options = row[1]["response_options"]
        indices_response_type = row[1]["indices_response_type"]

        if instructions not in exclude_list and \
                isinstance(instructions, str):
            predicates_list.append(("hasInstructions",
                                    language_string(instructions)))
        if group_instructions not in exclude_list and \
                isinstance(group_instructions, str):
            predicates_list.append(("hasGroupInstructions",
                                    language_string(group_instructions)))
        if digital_instructions not in exclude_list and \
                isinstance(digital_instructions, str):
            predicates_list.append(("hasDigitalInstructions",
                                    language_string(digital_instructions)))
        if digital_group_instructions not in exclude_list and \
                isinstance(digital_group_instructions, str):
            predicates_list.append(("hasDigitalGroupInstructions",
                                    language_string(digital_group_instructions)))
        if response_options not in exclude_list and \
                isinstance(response_options, str):
            predicates_list.append(("hasResponseOptions",
                                    language_string(response_options)))
        if indices_response_type not in exclude_list and \
                isinstance(indices_response_type, str):
            indices = [np.int(x) for x in
                       indices_response_type.strip().split(',') if len(x) > 0]
            for index in indices:
                objectRDF = response_types[response_types["index"] ==
                                           index]["response_type"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("hasResponseType",
                                            check_iri(objectRDF)))
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
        predicates_list.append(("rdf:type", "ResponseType"))

        for predicates in predicates_list:
            statements = add_if(
                response_type_iri,
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
        predicates_list.append(("rdf:type", "Task"))

        instructions = row[1]["instructions"]
        # respond_to_advance = row[1]["respond_to_advance"]
        # response_affects_presentation = row[1]["response_affects_presentation"]
        # response_based_feedback = row[1]["response_based_feedback"]
        # animation = row[1]["animation"]

        if isinstance(instructions, str):
            predicates_list.append(("hasInstructions",
                                    language_string(instructions)))
        # if isinstance(respond_to_advance, str):
        #     predicates_list.append(("respondToAdvance",
        #                             language_string(respond_to_advance)))
        # if isinstance(response_affects_presentation, str):
        #     predicates_list.append(("responseAffectsPresentation",
        #                             language_string(response_affects_presentation)))
        # if isinstance(response_based_feedback, str):
        #     predicates_list.append(("responseAffectsFeedback",
        #                             language_string(response_based_feedback)))
        # if isinstance(animation, str):
        #     predicates_list.append(("hasAnimation",
        #                             language_string(animation)))

        indices_task_categories = row[1]["indices_task_categories"]
        indices_domain = row[1]["indices_domain"]
        indices_project = row[1]["indices_project"]
        indices_presentations = row[1]["indices_presentations"]

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
                    predicates_list.append(("assessesDomain",
                                            language_string(objectRDF)))
        if isinstance(indices_project, str):
            indices = [np.int(x) for x in indices_project.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = projects[projects["index"] ==
                                     index]["project"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("hasProject",
                                            language_string(objectRDF)))
        if isinstance(indices_presentations, str):
            indices = [np.int(x) for x in indices_presentations.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = presentations[presentations["index"] ==
                                          index]["presentation"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("hasPresentation",
                                            language_string(objectRDF)))
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
        predicates_list.append(("rdf:type", "Task"))

        for predicates in predicates_list:
            statements = add_if(
                task_category_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # presentations worksheet
    for row in presentations.iterrows():

        presentation_label = language_string(row[1]["presentation"])
        presentation_iri = check_iri(row[1]["presentation"])

        predicates_list = []
        predicates_list.append(("rdfs:label", presentation_label))
        predicates_list.append(("rdf:type", "Presentation"))

        for predicates in predicates_list:
            statements = add_if(
                presentation_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    return statements


def ingest_projects(projects_xls, behaviors_xls, statements={}):
    """
    Function to ingest projects spreadsheet

    Parameters
    ----------
    projects_xls: pandas ExcelFile

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

    project_classes = projects_xls.parse("Classes")
    project_properties = projects_xls.parse("Properties")
    projects = projects_xls.parse("projects")
    project_types = projects_xls.parse("project_types")
    people = projects_xls.parse("people")
    sensors = behaviors_xls.parse("sensors")
    measures = behaviors_xls.parse("measures")
    domains = behaviors_xls.parse("domains")

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
        predicates_list.append(("rdf:type", "Project"))
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
                    predicates_list.append(("isForDomain",
                                            language_string(objectRDF)))
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
                    predicates_list.append(("hasProjectType",
                                            check_iri(project_type_iri)))
        if isinstance(indices_people, str):
            indices = [np.int(x) for x in
                       indices_people.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = people[people["index"] ==
                                        index]["person"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("hasActor",
                                            language_string(objectRDF)))
        if isinstance(indices_sensor, str):
            indices = [np.int(x) for x in
                       indices_sensor.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sensors[sensors["index"]  ==
                                        index]["sensor"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("hasSensor",
                                            language_string(objectRDF)))
        if isinstance(indices_measure, str):
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures[measures["index"]  ==
                                        index]["measure"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("takesMeasure",
                                            language_string(objectRDF)))
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
        predicates_list.append(("rdf:type", "ProjectType"))
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
                print(objectRDF, type(objectRDF))
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
        predicates_list.append(("rdf:type", "Actor"))
        predicates_list.append(("rdfs:label", person_label))
        if isinstance(row[1]["link"], str) and \
                row[1]["link"] not in exclude_list:
            predicates_list.append(("schema:WebSite",
                                    check_iri(row[1]["link"])))
        if isinstance(row[1]["affiliate"], str) and \
                row[1]["affiliate"] not in exclude_list:
            predicates_list.append(("hasAffiliate",
                                    language_string(row[1]["affiliate"])))
        if isinstance(row[1]["location"], str) and \
                row[1]["location"] not in exclude_list:
            predicates_list.append(("hasLocation",
                                    language_string(row[1]["location"])))
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

    assessments_xls: pandas ExcelFile

    dsm5_xls: pandas ExcelFile

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

    behavior_classes = behaviors_xls.parse("Classes")
    behavior_properties = behaviors_xls.parse("Properties")
    behaviors = behaviors_xls.parse("behaviors")
    question_preposts = behaviors_xls.parse("question_preposts")
    sensors = behaviors_xls.parse("sensors")
    measures = behaviors_xls.parse("measures")
    locations = behaviors_xls.parse("locations")
    domains = behaviors_xls.parse("domains")
    domain_types = behaviors_xls.parse("domain_types")
    claims = behaviors_xls.parse("claims")
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
        predicates_list.append(("rdf:type", "Behavior"))
        predicates_list.append(("rdfs:label", behavior_label))

        if row[1]["index_adverb_prefix"] not in exclude_list and \
                not np.isnan(row[1]["index_adverb_prefix"]):
            index_adverb_prefix = np.int(row[1]["index_adverb_prefix"])
            adverb_prefix = question_preposts[question_preposts["index"] ==
                index_adverb_prefix]["question_prepost"].values[0]
            if isinstance(adverb_prefix, str):
                predicates_list.append(("rdfs:adverb_prefixBLOOP",
                                        check_iri(adverb_prefix)))
        if row[1]["index_adverb_suffix"] not in exclude_list and \
                 not np.isnan(row[1]["index_adverb_suffix"]):
            index_adverb_suffix = np.int(row[1]["index_adverb_suffix"])
            adverb_suffix = question_preposts[question_preposts["index"] ==
                index_adverb_suffix]["question_prepost"].values[0]
            if isinstance(adverb_suffix, str):
                predicates_list.append(("rdfs:adverb_suffixBLOOP",
                                        check_iri(adverb_suffix)))

        if row[1]["index_gender"] not in exclude_list and \
                not np.isnan(row[1]["index_gender"]):
            gender_index = np.int(row[1]["index_gender"])
            #gender = genders[genders["index"] == gender_index]["gender"].values[0]
            if gender_index == 2:  # male
                predicates_list.append(
                    ("intendedAudience", "MaleAudience"))
            elif gender_index == 3: # female
                predicates_list.append(
                    ("intendedAudience", "FemaleAudience"))

        indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
        if isinstance(indices_sign_or_symptom, str) and \
                indices_sign_or_symptom not in exclude_list:
            indices = [np.int(x) for x in
                       indices_sign_or_symptom.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sign_or_symptoms[sign_or_symptoms["index"] ==
                                             index]["sign_or_symptom"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:sign_or_symptomBLOOP",
                                            check_iri(objectRDF)))
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

        predicates_list = []
        predicates_list.append(("rdf:type", "Sensor"))
        predicates_list.append(("rdfs:label", sensor_label))

        abbreviation = row[1]["abbreviation"]
        if isinstance(abbreviation, str)and \
                abbreviation not in exclude_list:
            predicates_list.append(("rdfs:abbreviationBLOOP",
                                    language_string(abbreviation)))

        indices_measure = row[1]["indices_measure"]
        if isinstance(indices_measure, str) and \
                indices_measure not in exclude_list:
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures.measure[measures["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("takesMeasure",
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

        predicates_list = []
        predicates_list.append(("rdf:type", "Measure"))
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

        indices_location = row[1]["indices_location"]
        if not np.isnan(indices_location) and \
                indices_location not in exclude_list:
            if isinstance(indices_location, str):
                indices = [np.int(x) for x in
                           indices_location.strip().split(',') if len(x)>0]
            elif isinstance(indices_location, float):
                indices = [np.int(indices_location)]
            for index in indices:
                objectRDF = locations[locations["index"] ==
                                      index]["IRI"].values[0]
                if isinstance(objectRDF, str) and objectRDF not in exclude_list:
                    predicates_list.append(("rdfs:locationBLOOP",
                                            check_iri(objectRDF)))
                else:
                    location = locations[locations["index"] ==
                                         index]["location"].values[0]
                    predicates_list.append(("rdfs:locationBLOOP",
                                            check_iri(location)))

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
        if isinstance(location_iri, float) or location_iri in exclude_list:
            location_iri = check_iri(location_label)
        else:
            location_iri = check_iri(location_iri)

        predicates_list = []
        predicates_list.append(("rdf:type", "BodyLocation"))
        predicates_list.append(("rdfs:label", location_label))

        for predicates in predicates_list:
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

        predicates_list = []
        predicates_list.append(("rdf:type", "Domain"))
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
                    predicates_list.append(("hasDomainType",
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
        predicates_list.append(("rdf:type", "DomainType"))
        predicates_list.append(("rdfs:label", domain_type_label))

        for predicates in predicates_list:
            statements = add_if(
                domain_type_iri,
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
        predicates_list.append(("rdf:type", "QuestionPrePost"))
        predicates_list.append(("rdfs:label", question_prepost_label))

        for predicates in predicates_list:
            statements = add_if(
                question_prepost_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # claims worksheet
    for row in claims.iterrows():

        claim_label = language_string(row[1]["claim"])
        claim_iri = check_iri(row[1]["claim"])

        predicates_list = []
        predicates_list.append(("rdf:type", "Claim"))
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
                    predicates_list.append(("makesClaimAbout",
                                            language_string(objectRDF)))

        indices_measure = row[1]["indices_measure"]
        if isinstance(indices_measure, str) and \
                indices_measure not in exclude_list:
            indices = [np.int(x) for x in
                       indices_measure.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = measures[measures["index"] ==
                                     index]["measure"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("makesClaimAbout",
                                            language_string(objectRDF)))

        indices_sensor = row[1]["indices_sensor"]
        if isinstance(indices_sensor, str) and \
                indices_sensor not in exclude_list:
            indices = [np.int(x) for x in
                       indices_sensor.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = sensors[sensors["index"] ==
                                     index]["sensor"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("makesClaimAbout",
                                            language_string(objectRDF)))

        indices_location = row[1]["indices_location"]
        if isinstance(indices_location, str) and \
                indices_location not in exclude_list:
            indices = [np.int(x) for x in
                       indices_location.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = locations[locations["index"] ==
                                     index]["location"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("makesClaimAbout",
                                            language_string(objectRDF)))

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

    reference_classes = references_xls.parse("Classes")
    reference_properties = references_xls.parse("Properties")
    references = references_xls.parse("references")
    reference_types = references_xls.parse("reference_types")
    domains = behaviors_xls.parse("domains")
    age_groups = references_xls.parse("age_groups")
    informants = references_xls.parse("informants")

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in behavior_classes.iterrows():

        behavior_class_label = language_string(row[1]["ClassName"])
        behavior_class_iri = check_iri(row[1]["ClassName"])

        predicates_list = []
        predicates_list.append(("rdfs:label", behavior_class_label))
        predicates_list.append(("rdf:type", "rdf:Class"))




    # references worksheet
    for row in references.iterrows():

        source = row[1]["link"]
        if isinstance(source, float):
            source = check_iri(row[1]["reference"])
        else:
            source = check_iri(source)

        predicates_list = []
        predicates_list.append(("rdfs:label",
                                language_string(row[1]["reference"])))
        predicates_list.append(("rdf:type", "Reference"))

        PubmedID = row[1]["PubmedID"]
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

        if isinstance(PubmedID, str):
            predicates_list.append(("rdfs:PubmedID",
                                    check_iri(PubmedID)))
        if isinstance(abbreviation, str):
            predicates_list.append(("rdfs:abbreviationBLOOP",
                                    language_string(abbreviation)))
        if isinstance(description, str):
            predicates_list.append(("rdfs:descriptionBLOOP",
                                    language_string(description)))
        if isinstance(cited_reference, str):
            predicates_list.append(("rdfs:cited_referenceBLOOP",
                                    check_iri(cited_reference)))
        if isinstance(number_of_questions, str):
            predicates_list.append(("rdfs:number_of_questionsBLOOP",
                                    language_string(number_of_questions)))
        if isinstance(minutes_to_complete, str):
            predicates_list.append(("rdfs:minutes_to_completeBLOOP",
                                    language_string(minutes_to_complete)))

        indices_reference_type = row[1]["indices_reference_type"]
        indices_age_group = row[1]["index_age_group"]

        if isinstance(indices_reference_type, str):
            indices = [np.int(x) for x in
                       indices_reference_type.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = reference_types.reference_type[
                    reference_types["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:reference_typeBLOOP",
                                            check_iri(objectRDF)))
        if isinstance(indices_age_group, str):
            indices = [np.int(x) for x in
                       indices_age_group.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = age_groups.age_group[
                    age_groups["index"] == index]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:age_groupBLOOP",
                                            check_iri(objectRDF)))

        indices_study_or_clinic = row[1]["indices_study_or_clinic"]
        indices_domain = row[1]["indices_domain"]
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



