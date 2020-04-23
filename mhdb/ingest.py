#!/usr/bin/env python3
"""
This script contains specific functions to interpret a specific set of
spreadsheets

Authors:
    - Arno Klein, 2019-2020 (arno@childmind.org)  http://binarybottle.com
    - Jon Clucas, 2017–2018 (jon.clucas@childmind.org)

Copyright 2020, Child Mind Institute (http://childmind.org), Apache v2.0 License

"""
try:
    from mhdb.spreadsheet_io import download_google_sheet
    from mhdb.write_ttl import check_iri, language_string
except:
    from mhdb.mhdb.spreadsheet_io import download_google_sheet
    from mhdb.mhdb.write_ttl import check_iri, language_string
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


def ingest_states(states_xls, statements={}):
    """
    Function to ingest states spreadsheet

    Parameters
    ----------
    states_xls: pandas ExcelFile

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

    # fill NANs with emptyValue
    state_classes = state_classes.fillna(emptyValue)
    state_properties = state_properties.fillna(emptyValue)
    states = states.fillna(emptyValue)
    state_types = state_types.fillna(emptyValue)

    statements = audience_statements(statements)

    # Classes worksheet
    for row in states_classes.iterrows():
        class_iri = check_iri(row[1]["ClassName"])
        class_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Class"))
        predicates_list.append(("rdfs:label", class_label))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs", row[1]["sameAs"]))
        if row[1]["equivalentClasses"] not in exclude_list:
            equivalentClasses = row[1]["equivalentClasses"]
            equivalentClasses = [x.strip() for x in
                             equivalentClasses.strip().split(',') if len(x) > 0]
            for equivalentClass in equivalentClasses:
                if equivalentClass not in exclude_list:
                    predicates_list.append(("rdfs:equivalentClass",
                                            equivalentClass))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in states_properties.iterrows():
        property_iri = check_iri(row[1]["property"])
        property_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Property"))
        predicates_list.append(("rdfs:label", property_label))
        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    row[1]["sameAs"]))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    row[1]["equivalentProperty"]))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # states worksheet
    for row in states.iterrows():

        state_label = language_string(row[1]["state"])
        state_iri = check_iri(row[1]["state"], 'PascalCase')

        predicates_list = []
        predicates_list.append(("rdfs:subClassOf", "m3-lite:DomainOfInterest"))
        predicates_list.append(("rdfs:label", state_label))

        indices_state_type = row[1]["indices_state_type"]
        if indices_state_type not in exclude_list:
            indices = [np.int(x) for x in
                       indices_state_type.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = state_types[state_types["index"] ==
                                         index]["state_type"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append((":hasDomainType",
                                            check_iri(objectRDF, 'PascalCase')))
        indices_state_category = row[1]["indices_state_category"]
        if indices_state_category not in exclude_list:
            indices = [np.int(x) for x in
                       indices_state_category.strip().split(',') if len(x)>0]
            for index in indices:
                objectRDF = states[states["index"] ==
                                         index]["state"].values[0]
                if isinstance(objectRDF, str):
                    predicates_list.append(("rdfs:subClassOf",
                                            check_iri(objectRDF, 'PascalCase')))

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
        state_type_iri = check_iri(row[1]["state_type"], 'PascalCase')

        predicates_list = []
        predicates_list.append(("rdfs:subClassOf", ":DomainType"))
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


def ingest_disorders(disorders_xls, statements={}):
    """
    Function to ingest disorders spreadsheet

    Parameters
    ----------
    disorders_xls: pandas ExcelFile

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
    ...     disordersFILE = download_google_sheet(
    ...         'data/disorders.xlsx',
    ...         "13a0w3ouXq5sFCa0fBsg9xhWx67RGJJJqLjD_Oy1c3b0"
    ...     )
    ... except:
    ...     disordersFILE = 'data/disorders.xlsx'
    >>> disorders_xls = pd.ExcelFile(disordersFILE)
    >>> behaviors_xls = pd.ExcelFile(behaviorsFILE)
    >>> statements = ingest_disorders(disorders_xls, statements={})
    >>> print(turtle_from_dict({
    ...     statement: statements[
    ...         statement
    ...     ] for statement in statements if statement == ":despair"
    ... }).split("\\n\\t")[0])
    #mhdb:despair rdfs:label "despair"@en ;
    """
    import math

    # load worksheets as pandas dataframes
    disorders_classes = disorders_xls.parse("Classes")
    disorders_properties = disorders_xls.parse("Properties")
    disorders = disorders_xls.parse("disorders")
    sign_or_symptoms = disorders_xls.parse("sign_or_symptoms")
    examples_sign_or_symptoms = disorders_xls.parse("examples_sign_or_symptoms")
    severities = disorders_xls.parse("severities")
    diagnostic_specifiers = disorders_xls.parse("diagnostic_specifiers")
    diagnostic_criteria = disorders_xls.parse("diagnostic_criteria")
    disorder_categories = disorders_xls.parse("disorder_categories")
    disorder_subcategories = disorders_xls.parse("disorder_subcategories")
    disorder_subsubcategories = disorders_xls.parse("disorder_subsubcategories")
    disorder_subsubsubcategories = disorders_xls.parse("disorder_subsubsubcategories")
    references = disorders_xls.parse("references")

    # fill NANs with emptyValue
    disorders_classes = disorders_classes.fillna(emptyValue)
    disorders_properties = disorders_properties.fillna(emptyValue)
    disorders = disorders.fillna(emptyValue)
    sign_or_symptoms = sign_or_symptoms.fillna(emptyValue)
    examples_sign_or_symptoms = examples_sign_or_symptoms.fillna(emptyValue)
    severities = severities.fillna(emptyValue)
    diagnostic_specifiers = diagnostic_specifiers.fillna(emptyValue)
    diagnostic_criteria = diagnostic_criteria.fillna(emptyValue)
    disorder_categories = disorder_categories.fillna(emptyValue)
    disorder_subcategories = disorder_subcategories.fillna(emptyValue)
    disorder_subsubcategories = disorder_subsubcategories.fillna(emptyValue)
    disorder_subsubsubcategories = disorder_subsubsubcategories.fillna(emptyValue)
    references = references.fillna(emptyValue)

    # Classes worksheet
    for row in disorders_classes.iterrows():
        class_iri = check_iri(row[1]["ClassName"])
        class_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Class"))
        predicates_list.append(("rdfs:label", class_label))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs", row[1]["sameAs"]))
        if row[1]["equivalentClasses"] not in exclude_list:
            equivalentClasses = row[1]["equivalentClasses"]
            equivalentClasses = [x.strip() for x in
                             equivalentClasses.strip().split(',') if len(x) > 0]
            for equivalentClass in equivalentClasses:
                if equivalentClass not in exclude_list:
                    predicates_list.append(("rdfs:equivalentClass",
                                            equivalentClass))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in disorders_properties.iterrows():
        property_iri = check_iri(row[1]["property"])
        property_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Property"))
        predicates_list.append(("rdfs:label", property_label))
        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    row[1]["sameAs"]))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    row[1]["equivalentProperty"]))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # sign_or_symptoms worksheet
    for row in sign_or_symptoms.iterrows():
        sign_or_symptom = row[1]["sign_or_symptom"].strip()
        if sign_or_symptom not in exclude_list:

            # sign or symptom?
            sign_or_symptom_number = np.int(row[1]["sign_or_symptom_number"])
            symptom_label = language_string(sign_or_symptom)
            symptom_iri = check_iri(sign_or_symptom, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", symptom_label))

            # reference
            if row[1]["index_reference"] not in exclude_list:
                source = references[references["index"] == row[1]["index_reference"]
                    ]["title"].values[0]
                source_iri = check_iri(source)
                predicates_list.append((":isReferencedBy", source_iri))

            # specific to females/males?
            if row[1]["index_gender"] not in exclude_list:
                if np.int(row[1]["index_gender"]) == 1:  # female
                    predicates_list.append(
                        ("schema:epidemiology", ":Female"))
                elif np.int(row[1]["index_gender"]) == 2:  # male
                    predicates_list.append(
                        ("schema:epidemiology", ":Male"))

            # indices for disorders
            indices_disorder = row[1]["indices_disorder"]
            if indices_disorder not in exclude_list:
                if isinstance(indices_disorder, float) or \
                        isinstance(indices_disorder, int):
                    indices_disorder = [np.int(indices_disorder)]
                else:
                    indices_disorder = [np.int(x) for x in
                               indices_disorder.strip().split(',') if len(x)>0]
                for index in indices_disorder:
                    disorder = disorders[disorders["index"] == index
                                        ]["disorder"].values[0]
                    if isinstance(disorder, str):
                        if sign_or_symptom_number == 1:
                            predicates_list.append((":isMedicalSignOf",
                                                    check_iri(disorder, 'PascalCase')))
                        elif sign_or_symptom_number == 2:
                            predicates_list.append((":isMedicalSymptomOf",
                                                    check_iri(disorder, 'PascalCase')))
                        else:
                            predicates_list.append((":isMedicalSignOrSymptomOf",
                                                    check_iri(disorder, 'PascalCase')))

            # Is the sign/symptom a subclass of other another sign/symptom?
            indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
            if indices_sign_or_symptom not in exclude_list:
                if isinstance(indices_sign_or_symptom, float) or \
                        isinstance(indices_sign_or_symptom, int):
                    indices_sign_or_symptom1 = [np.int(indices_sign_or_symptom)]
                else:
                    indices_sign_or_symptom1 = [np.int(x) for x in
                               indices_sign_or_symptom.strip().split(',') if len(x)>0]
                for index in indices_sign_or_symptom1:
                    super_sign = sign_or_symptoms[sign_or_symptoms["index"] ==
                                                  index]["sign_or_symptom"].values[0]
                    if isinstance(super_sign, str):
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(super_sign, 'PascalCase')))
            if sign_or_symptom_number == 1:
               predicates_list.append(("rdfs:subClassOf", ":MedicalSign"))
            elif sign_or_symptom_number == 2:
               predicates_list.append(("rdfs:subClassOf", ":MedicalSymptom"))
            else:
               predicates_list.append(("rdfs:subClassOf", ":MedicalSignOrSymptom"))

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
        examples_sign_or_symptoms = row[1]["examples_sign_or_symptoms"].strip()
        if examples_sign_or_symptoms not in exclude_list:

            example_symptom_label = language_string(examples_sign_or_symptoms)
            example_symptom_iri = check_iri(examples_sign_or_symptoms)

            predicates_list = []
            predicates_list.append(("rdfs:label", example_symptom_label))

            indices_sign_or_symptom = row[1]["indices_sign_or_symptom"]
            if indices_sign_or_symptom not in exclude_list:
                if isinstance(indices_sign_or_symptom, float) or \
                        isinstance(indices_sign_or_symptom, int):
                    indices_sign_or_symptom2 = [np.int(indices_sign_or_symptom)]
                else:
                    indices_sign_or_symptom2 = [np.int(x) for x in
                               indices_sign_or_symptom.strip().split(',') if len(x)>0]
                for index in indices_sign_or_symptom2:
                    objectRDF = sign_or_symptoms[sign_or_symptoms["index"] ==
                                                 index]["sign_or_symptom"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":isExampleOf",
                                                check_iri(objectRDF, 'PascalCase')))

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
        severity = row[1]["severity"].strip()
        if severity not in exclude_list:

            severity_label = language_string(severity)
            severity_iri = check_iri(severity, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", severity_label))

            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf", ":DisorderSeverity"))

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
        diagnostic_specifier = row[1]["diagnostic_specifier"].strip()
        if diagnostic_specifier not in exclude_list:

            diagnostic_specifier_label = language_string(diagnostic_specifier)
            diagnostic_specifier_iri = check_iri(diagnostic_specifier, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", diagnostic_specifier_label))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if
                                     len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf",
                                        ":DiagnosticSpecifier"))

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
        diagnostic_criterion = row[1]["diagnostic_criterion"].strip()
        if diagnostic_criterion not in exclude_list:

            diagnostic_criterion_label = language_string(diagnostic_criterion)
            diagnostic_criterion_iri = check_iri(diagnostic_criterion, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", diagnostic_criterion_label))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if
                                     len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf",
                                        ":DiagnosticCriterion"))

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
        if row[1]["disorder"] not in exclude_list:

            disorder_label = row[1]["disorder"]
            disorder_iri_label = disorder_label

            predicates_list = []

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if
                                     len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            if row[1]["note"] not in exclude_list:
                predicates_list.append((":hasNote",
                                        language_string(row[1]["note"])))
            if row[1]["ICD9CM"] not in exclude_list:
                ICD9 = str(row[1]["ICD9CM"])
                predicates_list.append((":hasICD9Code", "ICD9CM:" + ICD9))
                disorder_label += "; ICD9CM:{0}".format(ICD9)
                disorder_iri_label += " ICD9 {0}".format(ICD9)
            if row[1]["ICD10CM"] not in exclude_list:
                ICD10 = row[1]["ICD10CM"]
                predicates_list.append((":hasICD10Code", "ICD10CM:" + ICD10))
                disorder_label += "; ICD10CM:{0}".format(ICD10)
                disorder_iri_label += " ICD10 {0}".format(ICD10)
            if row[1]["index_diagnostic_specifier"] not in exclude_list:
                diagnostic_specifier = diagnostic_specifiers[
                diagnostic_specifiers["index"] == int(row[1]["index_diagnostic_specifier"])
                ]["diagnostic_specifier"].values[0]
                if isinstance(diagnostic_specifier, str):
                    predicates_list.append((":hasDiagnosticSpecifier",
                                            check_iri(diagnostic_specifier, 'PascalCase')))
                    disorder_label += "; specifier: {0}".format(diagnostic_specifier)
                    disorder_iri_label += " specifier {0}".format(diagnostic_specifier)

            if row[1]["index_diagnostic_inclusion_criterion"] not in exclude_list:
                diagnostic_inclusion_criterion = diagnostic_criteria[
                diagnostic_criteria["index"] == int(row[1]["index_diagnostic_inclusion_criterion"])
                ]["diagnostic_criterion"].values[0]
                if isinstance(diagnostic_inclusion_criterion, str):
                    predicates_list.append((":hasInclusionCriterion",
                                            check_iri(diagnostic_inclusion_criterion, 'PascalCase')))
                    disorder_label += \
                        "; inclusion: {0}".format(diagnostic_inclusion_criterion)
                    disorder_iri_label += \
                        " inclusion {0}".format(diagnostic_inclusion_criterion)

            if row[1]["index_diagnostic_inclusion_criterion2"] not in exclude_list:
                diagnostic_inclusion_criterion2 = diagnostic_criteria[
                diagnostic_criteria["index"] == int(row[1]["index_diagnostic_inclusion_criterion2"])
                ]["diagnostic_criterion"].values[0]
                if isinstance(diagnostic_inclusion_criterion2, str):
                    predicates_list.append((":hasInclusionCriterion",
                                            check_iri(diagnostic_inclusion_criterion2, 'PascalCase')))
                    disorder_label += \
                        ", {0}".format(diagnostic_inclusion_criterion2)
                    disorder_iri_label += \
                        " {0}".format(diagnostic_inclusion_criterion2)

            if row[1]["index_diagnostic_exclusion_criterion"] not in exclude_list:
                diagnostic_exclusion_criterion = diagnostic_criteria[
                diagnostic_criteria["index"] == int(row[1]["index_diagnostic_exclusion_criterion"])
                ]["diagnostic_criterion"].values[0]
                if isinstance(diagnostic_exclusion_criterion, str):
                    predicates_list.append((":hasExclusionCriterion",
                                            check_iri(diagnostic_exclusion_criterion, 'PascalCase')))
                    disorder_label += \
                        "; exclusion: {0}".format(diagnostic_exclusion_criterion)
                    disorder_iri_label += \
                        " exclusion {0}".format(diagnostic_exclusion_criterion)

            if row[1]["index_diagnostic_exclusion_criterion2"] not in exclude_list:
                diagnostic_exclusion_criterion2 = diagnostic_criteria[
                diagnostic_criteria["index"] == int(row[1]["index_diagnostic_exclusion_criterion2"])
                ]["diagnostic_criterion"].values[0]
                if isinstance(diagnostic_exclusion_criterion2, str):
                    predicates_list.append((":hasExclusionCriterion",
                                            check_iri(diagnostic_exclusion_criterion2, 'PascalCase')))
                    disorder_label += \
                        ", {0}".format(diagnostic_exclusion_criterion2)
                    disorder_iri_label += \
                        " {0}".format(diagnostic_exclusion_criterion2)

            if row[1]["index_severity"] not in exclude_list:
                severity = severities[
                severities["index"] == int(row[1]["index_severity"])
                ]["severity"].values[0]
                if isinstance(severity, str) and severity not in exclude_list:
                    predicates_list.append((":hasSeverity",
                                            check_iri(severity, 'PascalCase')))
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
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(disorder_subsubsubcategory, 'PascalCase')))
                statements = add_to_statements(
                    check_iri(disorder_subsubsubcategory, 'PascalCase'),
                    "rdfs:subClassOf",
                    check_iri(disorder_subsubcategory, 'PascalCase'),
                    statements,
                    exclude_list
                )
                if disorder_subsubcategory not in exclude_categories and \
                    disorder_subcategory not in exclude_categories:
                    statements = add_to_statements(
                        check_iri(disorder_subsubcategory, 'PascalCase'),
                        "rdfs:subClassOf",
                        check_iri(disorder_subcategory, 'PascalCase'),
                        statements,
                        exclude_list
                    )
                    statements = add_to_statements(
                        check_iri(disorder_subcategory, 'PascalCase'),
                        "rdfs:subClassOf",
                        check_iri(disorder_category, 'PascalCase'),
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
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(disorder_subsubcategory, 'PascalCase')))
                statements = add_to_statements(
                    check_iri(disorder_subsubcategory, 'PascalCase'),
                    "rdfs:subClassOf",
                    check_iri(disorder_subcategory, 'PascalCase'),
                    statements,
                    exclude_list
                )
                if disorder_subcategory not in exclude_categories and \
                    disorder_category not in exclude_categories:
                    statements = add_to_statements(
                        check_iri(disorder_subcategory, 'PascalCase'),
                        "rdfs:subClassOf",
                        check_iri(disorder_category, 'PascalCase'),
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
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(disorder_subcategory, 'PascalCase')))
                if disorder_category not in exclude_categories:
                    statements = add_to_statements(
                        check_iri(disorder_subcategory, 'PascalCase'),
                        "rdfs:subClassOf",
                        check_iri(disorder_category, 'PascalCase'),
                        statements,
                        exclude_list
                    )
                    exclude_categories.append(disorder_category)
            elif row[1]["index_disorder_category"] not in exclude_list:
                disorder_category = disorder_categories[
                    disorder_categories["index"] == int(row[1]["index_disorder_category"])
                ]["disorder_category"].values[0]
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(disorder_category, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Disorder"))

            disorder_label = language_string(disorder_label)
            disorder_iri = check_iri(disorder_iri_label, 'PascalCase')
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
        disorder_category = row[1]["disorder_category"].strip()
        if disorder_category not in exclude_list:

            disorder_category_label = language_string(disorder_category)
            disorder_category_iri = check_iri(disorder_category, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", disorder_category_label))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if
                                     len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Disorder"))

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
        disorder_subcategory = row[1]["disorder_subcategory"].strip()
        if disorder_subcategory not in exclude_list:

            disorder_subcategory_label = language_string(disorder_subcategory)
            disorder_subcategory_iri = check_iri(disorder_subcategory, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", disorder_subcategory_label))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if
                                     len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Disorder"))

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
        disorder_subsubcategory = row[1]["disorder_subsubcategory"].strip()
        if disorder_subsubcategory not in exclude_list:

            disorder_subsubcategory_label = language_string(disorder_subsubcategory)
            disorder_subsubcategory_iri = check_iri(disorder_subsubcategory, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", disorder_subsubcategory_label))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if
                                     len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Disorder"))

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
        disorder_subsubsubcategory = row[1]["disorder_subsubsubcategory"].strip()
        if disorder_subsubsubcategory not in exclude_list:

            disorder_subsubsubcategory_label = language_string(disorder_subsubsubcategory)
            disorder_subsubsubcategory_iri = check_iri(disorder_subsubsubcategory, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", disorder_subsubsubcategory_label))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if
                                     len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Disorder"))

            for predicates in predicates_list:
                statements = add_to_statements(
                    disorder_subsubsubcategory_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # references worksheet
    for row in references.iterrows():
        title = row[1]["title"]
        if title not in exclude_list:

            predicates_list = []

            # reference IRI
            reference_iri = check_iri(title)
            predicates_list.append(("a", ":BibliographicResource"))
            predicates_list.append(("rdfs:label", language_string(title)))
            predicates_list.append((":hasTitle", language_string(title)))

            # general columns
            link = row[1]["link"]
            if link not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(link.strip())))
            entry_date = row[1]["entry_date"]
            if entry_date not in exclude_list:
                predicates_list.append((":hasDateLastUpdated",
                                        language_string(entry_date)))

            # research article-specific columns
            authors = row[1]["authors"]
            year = row[1]["year"]
            PubMedID = row[1]["PubMedID"]
            if authors not in exclude_list:
                predicates_list.append((":hasAuthorList",
                                        language_string(authors)))
            if year not in exclude_list:
                predicates_list.append((":hasPublicationYear",
                                        '"{0}"^^xsd:gyear'.format(int(year))))
            if PubMedID not in exclude_list:
                predicates_list.append((":hasPubMedID",
                                        '"{0}"^^xsd:nonNegativeInteger'.format(int(PubMedID))))

            for predicates in predicates_list:
                statements = add_to_statements(
                    reference_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    return statements


def ingest_resources(resources_xls, measures_xls, states_xls, statements={}):
    """
    Function to ingest resources spreadsheet

    Parameters
    ----------
    resources_xls: pandas ExcelFile

    states_xls: pandas ExcelFile

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

    # load worksheets as pandas dataframes
    resources_classes = resources_xls.parse("Classes")
    resources_properties = resources_xls.parse("Properties")
    # guides worksheets
    guide_types = resources_xls.parse("guide_types")
    guides = resources_xls.parse("guides")
    # treatments, medications worksheets
    treatments = resources_xls.parse("treatments")
    medications = resources_xls.parse("medications")
    # projects worksheets
    project_types = resources_xls.parse("project_types")
    projects = resources_xls.parse("projects")
    groups = resources_xls.parse("groups")
    # guides and projects worksheets
    references = resources_xls.parse("references")
    # worksheets shared across mhdb
    people = resources_xls.parse("people")
    languages = resources_xls.parse("languages")
    licenses = resources_xls.parse("licenses")
    # imported (non-resources) worksheets
    #measures = measures_xls.parse("measures")
    #sensors = measures_xls.parse("sensors")
    #states = states_xls.parse("states")

    # fill NANs with emptyValue
    resources_classes = resources_classes.fillna(emptyValue)
    resources_properties = resources_properties.fillna(emptyValue)
    guide_types = guide_types.fillna(emptyValue)
    guides = guides.fillna(emptyValue)
    treatments = treatments.fillna(emptyValue)
    medications = medications.fillna(emptyValue)
    project_types = project_types.fillna(emptyValue)
    projects = projects.fillna(emptyValue)
    groups = groups.fillna(emptyValue)
    references = references.fillna(emptyValue)
    people = people.fillna(emptyValue)
    languages = languages.fillna(emptyValue)
    licenses = licenses.fillna(emptyValue)
    #measures = measures.fillna(emptyValue)
    #sensors = sensors.fillna(emptyValue)
    #states = states_xls.parse("states")

    # Classes worksheet
    for row in resources_classes.iterrows():
        class_iri = check_iri(row[1]["ClassName"])
        class_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Class"))
        predicates_list.append(("rdfs:label", class_label))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs", row[1]["sameAs"]))
        if row[1]["equivalentClasses"] not in exclude_list:
            equivalentClasses = row[1]["equivalentClasses"]
            equivalentClasses = [x.strip() for x in
                             equivalentClasses.strip().split(',') if len(x) > 0]
            for equivalentClass in equivalentClasses:
                if equivalentClass not in exclude_list:
                    predicates_list.append(("rdfs:equivalentClass",
                                            equivalentClass))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in resources_properties.iterrows():
        property_iri = check_iri(row[1]["property"])
        property_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Property"))
        predicates_list.append(("rdfs:label", property_label))
        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    row[1]["sameAs"]))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    row[1]["equivalentProperty"]))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # guide_types worksheet
    for row in guide_types.iterrows():
        guide_type = row[1]["guide_type"]
        if guide_type not in exclude_list:
            predicates_list = []

            guide_type_iri = check_iri(guide_type, 'PascalCase')
            predicates_list.append(("rdfs:label", language_string(guide_type)))

            if row[1]["subClassOf"] not in exclude_list:
                predicates_list.append(("rdfs:subClassOf",
                                        check_iri(row[1]["subClassOf"])))
            else:
                predicates_list.append(("rdfs:subClassOf", ":ReferenceType"))

            for predicates in predicates_list:
                statements = add_to_statements(
                    guide_type_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # guides worksheet
    for row in guides.iterrows():
        title = row[1]["title"]
        if title not in exclude_list:
            predicates_list = []

            # guide IRI
            guide_iri = check_iri(title)
            predicates_list.append(("a", ":BibliographicResource"))
            predicates_list.append(("rdfs:label", language_string(title)))
            predicates_list.append((":hasTitle", language_string(title)))

            # link, entry date
            link = row[1]["link"]
            entry_date = row[1]["entry_date"]
            if link not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(link.strip())))
            if entry_date not in exclude_list:
                predicates_list.append((":hasDateLastUpdated",
                                        language_string(entry_date)))

            # research article-specific columns: authors, publisher, pubdate
            authors = row[1]["authors"]
            publisher = row[1]["publisher"]
            pubdate = row[1]["pubdate"]
            if authors not in exclude_list:
                predicates_list.append((":hasAuthorList",
                                        language_string(authors)))
            if publisher not in exclude_list:
                predicates_list.append((":hasPublisher",
                                        check_iri(publisher)))
            if pubdate not in exclude_list:
                predicates_list.append((":hasPublicationDate",
                                        language_string(pubdate)))

            # guide type
            indices_guide_type = row[1]["indices_guide_type"]
            if indices_guide_type not in exclude_list:
                if isinstance(indices_guide_type, float) or \
                        isinstance(indices_guide_type, int):
                    indices = [np.int(indices_guide_type)]
                else:
                    indices = [np.int(x) for x in
                               indices_guide_type.strip().split(',') if len(x)>0]
                if indices not in exclude_list:
                    for index in indices:
                        objectRDF = guide_types[
                            guide_types["index"] == index]["guide_type"].values[0]
                        if objectRDF not in exclude_list:
                            predicates_list.append((":hasReferenceType",
                                                    check_iri(objectRDF, 'PascalCase')))
            # specific to females/males?
            index_gender = row[1]["index_gender"]
            if index_gender not in exclude_list:
                if np.int(index_gender) == 1:  # female
                    predicates_list.append((":isAbout", ":Female"))
                elif np.int(index_gender) == 2:  # male
                    predicates_list.append((":isAbout", ":Male"))

            # audience, subject, language, license
            indices_audience = row[1]["indices_audience"]
            indices_subject = row[1]["indices_subject"]
            indices_language = row[1]["indices_language"]
            index_license = row[1]["index_license"]
            # if indices_audience not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_audience.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = people[
            #             people["index"] == index]["person"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append((":hasAudienceType",
            #                                     check_iri(objectRDF, 'PascalCase')))
            # if indices_subject not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_subject.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = people[
            #             people["index"] == index]["person"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append((":isAbout",
            #                                     check_iri(objectRDF, 'PascalCase')))
            if indices_language not in exclude_list:
                indices = [np.int(x) for x in
                           indices_language.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = languages[
                        languages["index"] == index]["language"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append((":hasLanguage",
                                                check_iri(objectRDF, 'PascalCase')))
            if index_license not in exclude_list:
                objectRDF = shared[licenses["index"] ==
                                       index_license]["license"].values[0]
                if objectRDF not in exclude_list:
                    predicates_list.append((":hasLicense", check_iri(objectRDF, 'PascalCase')))

            # indices to other worksheets about content of the shared
            #indices_state = row[1]["indices_state"]
            #indices_disorder = row[1]["indices_disorder"]
            #indices_disorder_category = row[1]["indices_disorder_category"]
            # if indices_state not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_state.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = states[states["index"] == index]["state"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append((":isAboutDomain",
            #                                     check_iri(objectRDF, 'PascalCase')))
            # if indices_disorder not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_disorder.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = disorders[disorders["index"] ==
            #                               index]["disorder"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))
            # if indices_disorder_category not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_disorder_category.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = disorder_categories[disorder_categories["index"] ==
            #                          index]["disorder_category"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))

            for predicates in predicates_list:
                statements = add_to_statements(
                    guide_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # treatments worksheet
    for row in treatments.iterrows():
        treatment = row[1]["treatment"]
        if treatment not in exclude_list:

            predicates_list = []
            predicates_list.append(("rdfs:label", language_string(treatment)))
            treatment_iri = check_iri(treatment, 'PascalCase')

            # indices to parent classes
            if row[1]["indices_treatment"] not in exclude_list:
                indices_treatment = row[1]["indices_treatment"]
                if isinstance(indices_treatment, float) or \
                        isinstance(indices_treatment, int):
                    indices = [np.int(indices_treatment)]
                else:
                    indices = [np.int(x) for x in
                               indices_treatment.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = treatments[treatments["index"] ==
                                           index]["treatment"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Treatment"))

            # aliases
            if row[1]["aliases"] not in exclude_list:
                aliases = row[1]["aliases"].split(',')
                for alias in aliases:
                    predicates_list.append(("rdfs:label", language_string(alias)))

            # definition
            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))

            # equivalentClasses
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))

            for predicates in predicates_list:
                statements = add_to_statements(
                    treatment_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # medications worksheet
    for row in medications.iterrows():
        medication = row[1]["medication"]
        if medication not in exclude_list:

            predicates_list = []
            predicates_list.append(("rdfs:label",
                                    language_string(row[1]["medication"])))
            medication_iri = check_iri(row[1]["medication"], 'PascalCase')

            # indices to parent classes
            if row[1]["indices_medication"] not in exclude_list:
                indices = [np.int(x) for x in
                           row[1]["indices_medication"].strip().split(',')
                           if len(x)>0]
                for index in indices:
                    objectRDF = medications[medications["index"] ==
                                           index]["medication"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Medication"))

            # aliases
            if row[1]["aliases"] not in exclude_list:
                aliases = row[1]["aliases"].split(',')
                for alias in aliases:
                    predicates_list.append(("rdfs:label", language_string(alias)))

            # equivalentClasses
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))

            for predicates in predicates_list:
                statements = add_to_statements(
                    medication_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # project_types worksheet
    for row in project_types.iterrows():
        project_type = row[1]["project_type"]
        if project_type not in exclude_list:

            project_type_iri = check_iri(project_type, 'PascalCase')
            predicates_list = []
            predicates_list.append(("rdfs:label", language_string(project_type)))
            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))
            # aliases
            if row[1]["aliases"] not in exclude_list:
                aliases = row[1]["aliases"].split(',')
                for alias in aliases:
                    predicates_list.append(("rdfs:label", language_string(alias)))

            # equivalentClasses
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            # subClassOf
            if row[1]["indices_project_type"] not in exclude_list:
                indices = [np.int(x) for x in
                           row[1]["indices_project_type"].strip().split(',')
                           if len(x)>0]
                for index in indices:
                    objectRDF = project_types[project_types["index"] ==
                                              index]["project_type"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":ProjectCategory"))

            for predicates in predicates_list:
                statements = add_to_statements(
                    project_type_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # projects worksheet
    for row in projects.iterrows():
        project = row[1]["project"]
        if project not in exclude_list:

            project_iri = check_iri(project)
            project_label = language_string(project)

            predicates_list = []
            predicates_list.append(("a", ":Project"))
            predicates_list.append(("rdfs:label", project_label))
            if row[1]["description"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["description"])))
            if row[1]["link"] not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(row[1]["link"].strip())))

            indices_project_type = row[1]["indices_project_type"]
            indices_group = row[1]["indices_group"]
            indices_sensor = row[1]["indices_sensor"]
            #indices_measure = row[1]["indices_measure"]

            # project types
            if indices_project_type not in exclude_list:
                indices = [np.int(x) for x in
                           indices_project_type.strip().split(',') if len(x)>0]
                for index in indices:
                    project_type = project_types[project_types["index"] ==
                                            index]["project_type"].values[0]
                    predicates_list.append((":hasProjectCategory",
                                            check_iri(project_type, 'PascalCase')))
            # groups
            if indices_group not in exclude_list:
                indices = [np.int(x) for x in
                           indices_group.strip().split(',') if len(x)>0]
                for index in indices:

                    group_org_iri = None
                    if groups[groups["index"] == index]["group"].values[0] not in exclude_list:
                        group_org_iri = groups[groups["index"] == index]["group"].values[0]
                    if groups[groups["index"] == index]["organization"].values[0] not in exclude_list:
                        orgname = groups[groups["index"] == index]["organization"].values[0]
                        if group_org_iri not in exclude_list and orgname not in exclude_list:
                            group_org_iri = group_org_iri + "_" + orgname
                        else:
                            group_org_iri = orgname
                    if group_org_iri not in exclude_list:
                        predicates_list.append((":isMaintainedByGroup",
                                                check_iri(group_org_iri)))
            # # sensors and measures
            # if indices_sensor not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_sensor.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = sensors[sensors["index"] == index]["sensor"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append((":hasSubSystem",
            #                                     check_iri(objectRDF, 'PascalCase')))
            # if indices_measure not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_measure.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = measures[measures["index"] ==
            #                              index]["measure"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append((":observes",
            #                                     check_iri(objectRDF, 'PascalCase')))

            # references
            if row[1]["indices_reference"] not in exclude_list:
                indices = [np.int(x) for x in
                           row[1]["indices_reference"].strip().split(',') if len(x)>0]
                for index in indices:
                    source = references[references["index"] == index]["title"].values[0]
                    source_iri = check_iri(source)
                    predicates_list.append((":isReferencedBy", source_iri))

            for predicates in predicates_list:
                statements = add_to_statements(
                    project_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # groups worksheet: require group or organization
    for row in groups.iterrows():

        predicates_list = []

        subject_iri = None
        if row[1]["group"] not in exclude_list:
            group_name = row[1]["group"]
            group_iri = check_iri(group_name)
            group_label = language_string(group_name)
            predicates_list.append(("a", ":Group"))
            predicates_list.append(("rdfs:label", group_label))
            subject_iri = group_iri

        if row[1]["organization"] not in exclude_list:
            org_name = row[1]["organization"]
            organization_iri = check_iri(org_name)
            statements = add_to_statements(organization_iri, "a",
                                           ":Organization", statements,
                                           exclude_list)
            statements = add_to_statements(organization_iri, "rdfs:label",
                                           language_string(
                                               row[1]["organization"]),
                                           statements, exclude_list)
            if subject_iri:
                subject_iri = check_iri(group_name + "_" + org_name)
                predicates_list.append(
                    (":isGroupMemberOf", organization_iri))
            else:
                subject_iri = organization_iri

        if subject_iri:
            if row[1]["link"] not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(row[1]["link"].strip())))
            if row[1]["abbreviation"] not in exclude_list:
                predicates_list.append((":hasAbbreviation",
                                        check_iri(row[1]["abbreviation"])))
            if row[1]["member"] not in exclude_list:
                member_iri = check_iri(row[1]["member"])
                member_label = language_string(row[1]["member"])
                statements = add_to_statements(member_iri, "a", ":Person",
                                               statements, exclude_list)
                statements = add_to_statements(member_iri, ":hasName",
                                               member_label, statements,
                                               exclude_list)
                predicates_list.append((":hasMember", member_iri))

            for predicates in predicates_list:
                statements = add_to_statements(
                    subject_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # people worksheet
    for row in people.iterrows():
        person = row[1]["person"]
        if person not in exclude_list:

            predicates_list = []
            predicates_list.append(("rdfs:label", language_string(person)))
            person_iri = check_iri(person, 'PascalCase')

            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))
            # aliases
            if row[1]["aliases"] not in exclude_list:
                aliases = row[1]["aliases"].split(',')
                for alias in aliases:
                    predicates_list.append(("rdfs:label", language_string(alias)))

            # equivalentClasses
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))

            # indices to parent classes
            if row[1]["indices_person"] not in exclude_list:
                indices_person = row[1]["indices_person"]
                if isinstance(indices_person, float) or \
                        isinstance(indices_person, int):
                    indices = [np.int(indices_person)]
                else:
                    indices = [np.int(x) for x in
                               indices_person.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = people[people["index"] ==
                                       index]["person"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":PersonType"))

            for predicates in predicates_list:
                statements = add_to_statements(
                    person_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # languages worksheet
    for row in languages.iterrows():
        language = row[1]["language"]
        if language not in exclude_list:

            predicates_list = []
            predicates_list.append(("rdfs:label", language_string(language)))
            language_iri = check_iri(language, 'PascalCase')

            # indices to parent classes
            if row[1]["indices_language"] not in exclude_list:
                indices = [np.int(x) for x in
                           row[1]["indices_language"].strip().split(',')
                           if len(x)>0]
                for index in indices:
                    objectRDF = languages[languages["index"] ==
                                           index]["language"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Language"))

            # equivalentClasses
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))

            for predicates in predicates_list:
                statements = add_to_statements(
                    language_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # licenses worksheet
    for row in licenses.iterrows():
        license = row[1]["license"]
        if license not in exclude_list:

            predicates_list = []
            predicates_list.append(("rdfs:label", language_string(license)))
            license_iri = check_iri(license, 'PascalCase')

            # equivalentClasses
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            # indices to parent classes
            if row[1]["indices_license"] not in exclude_list:
                indices_license = row[1]["indices_license"]
                if isinstance(indices_license, float) or \
                        isinstance(indices_license, int):
                    indices = [np.int(indices_license)]
                else:
                    indices = [np.int(x) for x in
                               indices_license.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = licenses[licenses["index"] ==
                                           index]["license"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":License"))

            for predicates in predicates_list:
                statements = add_to_statements(
                    license_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # references worksheet
    for row in references.iterrows():
        title = row[1]["title"]
        if title not in exclude_list:
            predicates_list = []

            # reference IRI
            reference_iri = check_iri(title)
            predicates_list.append(("a", ":BibliographicResource"))
            predicates_list.append(("rdfs:label", language_string(title)))
            predicates_list.append((":hasTitle", language_string(title)))

            # general columns
            link = row[1]["link"]
            if link not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(row[1]["link"].strip())))
            entry_date = row[1]["entry_date"]
            if entry_date not in exclude_list:
                predicates_list.append((":hasDateLastUpdated",
                                        language_string(entry_date)))

            # research article-specific columns
            authors = row[1]["authors"]
            year = row[1]["year"]
            PubMedID = row[1]["PubMedID"]
            if authors not in exclude_list:
                predicates_list.append((":hasAuthorList",
                                        language_string(authors)))
            if year not in exclude_list:
                predicates_list.append((":hasPublicationYear",
                                        '"{0}"^^xsd:gyear'.format(int(year))))
            if PubMedID not in exclude_list:
                predicates_list.append((":hasPubMedID",
                                        '"{0}"^^xsd:nonNegativeInteger'.format(int(PubMedID))))

            for predicates in predicates_list:
                statements = add_to_statements(
                    reference_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    return statements


def ingest_assessments(assessments_xls, resources_xls, statements={}):
    """
    Function to ingest assessments spreadsheet

    Parameters
    ----------
    assessments_xls: pandas ExcelFile

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
    assessments_classes = assessments_xls.parse("Classes")
    assessments_properties = assessments_xls.parse("Properties")
    # questions
    questionnaires = assessments_xls.parse("questionnaires")
    questions = assessments_xls.parse("questions")
    response_types = assessments_xls.parse("response_types")
    # tasks
    tasks = assessments_xls.parse("tasks")
    implementations = assessments_xls.parse("task_implementations")
    indicators = assessments_xls.parse("task_indicators")
    conditions = assessments_xls.parse("task_conditions")
    contrasts = assessments_xls.parse("task_contrasts")
    assertions_indices = assessments_xls.parse("task_assertions_indices")
    references = assessments_xls.parse("references")
    projects = resources_xls.parse("projects")

    # fill NANs with emptyValue
    assessments_classes = assessments_classes.fillna(emptyValue)
    assessments_properties = assessments_properties.fillna(emptyValue)
    questionnaires = questionnaires.fillna(emptyValue)
    questions = questions.fillna(emptyValue)
    response_types = response_types.fillna(emptyValue)
    tasks = tasks.fillna(emptyValue)
    implementations = implementations.fillna(emptyValue)
    indicators = indicators.fillna(emptyValue)
    conditions = conditions.fillna(emptyValue)
    contrasts = contrasts.fillna(emptyValue)
    assertions_indices = assertions_indices.fillna(emptyValue)
    references = references.fillna(emptyValue)
    projects = projects.fillna(emptyValue)

    #statements = audience_statements(statements)

    # Classes worksheet
    for row in assessments_classes.iterrows():
        class_iri = check_iri(row[1]["ClassName"])
        class_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Class"))
        predicates_list.append(("rdfs:label", class_label))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs", row[1]["sameAs"]))
        if row[1]["equivalentClasses"] not in exclude_list:
            equivalentClasses = row[1]["equivalentClasses"]
            equivalentClasses = [x.strip() for x in
                             equivalentClasses.strip().split(',') if len(x) > 0]
            for equivalentClass in equivalentClasses:
                if equivalentClass not in exclude_list:
                    predicates_list.append(("rdfs:equivalentClass",
                                            equivalentClass))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in assessments_properties.iterrows():
        property_iri = check_iri(row[1]["property"])
        property_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Property"))
        predicates_list.append(("rdfs:label", property_label))
        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    row[1]["sameAs"]))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    row[1]["equivalentProperty"]))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # questionnaires worksheet
    for row in questionnaires.iterrows():
        title = row[1]["title"]
        if title not in exclude_list:
            predicates_list = []

            # reference IRI
            questionnaire_iri = check_iri(title)
            predicates_list.append(("a", ":Questionnaire"))
            predicates_list.append(("rdfs:label", language_string(title)))
            predicates_list.append((":hasTitle", language_string(title)))

            # general columns
            abbreviation = row[1]["abbreviation"]
            description = row[1]["description"]
            link = row[1]["link"]
            if abbreviation not in exclude_list:
                predicates_list.append((":hasAbbreviation",
                                        language_string(abbreviation)))
            if description not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(description)))
            if link not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(link.strip())))
            #entry_date = row[1]["entry_date"]
            #if entry_date not in exclude_list:
            #    predicates_list.append((":hasDateLastUpdated",
            #                            language_string(entry_date)))

            # # specific to females/males?
            # index_gender = row[1]["index_gender"]
            # if index_gender not in exclude_list:
            #     if np.int(index_gender) == 1:  # female
            #         predicates_list.append(
            #             ("schema:audienceType", "schema:Female"))
            #         predicates_list.append(
            #             ("schema:epidemiology", "schema:Female"))
            #     elif np.int(index_gender) == 2:  # male
            #         predicates_list.append(
            #             ("schema:audienceType", "schema:Male"))
            #         predicates_list.append(
            #             ("schema:epidemiology", "schema:Male"))

            # research article-specific columns
            authors = row[1]["authors"]
            year = row[1]["year"]
            if authors not in exclude_list:
                predicates_list.append((":hasAuthorList",
                                        language_string(authors)))
            if year not in exclude_list:
                predicates_list.append((":hasPublicationYear",
                                        '"{0}"^^xsd:gyear'.format(int(year))))

            # questionnaire-specific columns
            use_with_assessments = row[1]["use_with_assessments"]
            number_of_questions = row[1]["number_of_questions"]
            minutes_to_complete = row[1]["minutes_to_complete"]
            age_min = row[1]["age_min"]
            age_max = row[1]["age_max"]
            if use_with_assessments not in exclude_list:
                indices = [np.int(x) for x in
                           use_with_assessments.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = questionnaires[
                        questionnaires["index"] == index]["title"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append((":useWith",
                                                check_iri(objectRDF)))
            if number_of_questions not in exclude_list and \
                    isinstance(number_of_questions, str):
                #if "-" in number_of_questions:
                #    predicates_list.append((":hasNumberOfQuestions",
                #                            '"{0}"^^xsd:string'.format(
                #                                number_of_questions)))
                predicates_list.append((":hasNumberOfQuestions",
                                        '"{0}"^^xsd:nonNegativeInteger'.format(
                                            number_of_questions)))
            if minutes_to_complete not in exclude_list and \
                    isinstance(minutes_to_complete, str):
                #if "-" in minutes_to_complete:
                #    predicates_list.append((":takesMinutesToComplete",
                #        '"{0}"^^xsd:string'.format(minutes_to_complete)))
                predicates_list.append((":takesMinutesToComplete",
                    '"{0}"^^xsd:decimal'.format(minutes_to_complete)))
            if age_min not in exclude_list and isinstance(age_min, str):
                predicates_list.append(("schema:requiredMinAge",
                    '"{0}"^^xsd:decimal'.format(age_min)))
            if age_max not in exclude_list and isinstance(age_max, str):
                predicates_list.append(("schema:requiredMaxAge",
                    '"{0}"^^xsd:decimal'.format(age_max)))

            # indices to other worksheets about who uses the shared
            indices_respondent = row[1]["indices_respondent"]
            indices_subject = row[1]["indices_subject"]
            indices_reference = row[1]["indices_reference"]
            index_license = row[1]["index_license"]
            indices_language = row[1]["indices_language"]
            # if indices_respondent not in exclude_list:
            #     if isinstance(indices_respondent, float):
            #         indices = [np.int(indices_respondent)]
            #     else:
            #         indices = [np.int(x) for x in
            #                    indices_respondent.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = respondents_or_subjects[
            #             respondents_or_subjects["index"] ==
            #             index]["respondent_or_subject"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append(("schema:audienceType",
            #                                     check_iri(objectRDF, 'PascalCase')))
            # if indices_subject not in exclude_list:
            #     if isinstance(indices_subject, float):
            #         indices = [np.int(indices_subject)]
            #     else:
            #         indices = [np.int(x) for x in
            #                    indices_subject.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = respondents_or_subjects[
            #             respondents_or_subjects["index"] ==
            #             index]["respondent_or_subject"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append(("schema:about",
            #                                     check_iri(objectRDF, 'PascalCase')))
            # if indices_reference not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_reference.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         # cited reference IRI
            #         title_cited = questionnaires[
            #             questionnaires["index"] == index]["title"].values[0]
            #         if title_cited not in exclude_list:
            #             predicates_list.append((":isReferencedBy",
            #                                     check_iri(title_cited)))
            if index_license not in exclude_list:
                objectRDF = shared[licenses["index"] ==
                                       index_license]["license"].values[0]
                if objectRDF not in exclude_list:
                    predicates_list.append((":hasLicense", check_iri(objectRDF, 'PascalCase')))
            # if indices_language not in exclude_list:
            #     indices = [np.int(x) for x in
            #                indices_language.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         objectRDF = languages[
            #             languages["index"] == index]["language"].values[0]
            #         if objectRDF not in exclude_list:
            #             predicates_list.append((":hasLanguage",
            #                                     check_iri(objectRDF, 'PascalCase')))

            for predicates in predicates_list:
                statements = add_to_statements(
                    questionnaire_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # questions worksheet
    qnum = 1
    old_questionnaires = []
    for row in questions.iterrows():
        question = row[1]["question"].strip()
        if question not in exclude_list:

            questionnaire = questionnaires[questionnaires["index"] ==
                                row[1]["index_questionnaire"]]["title"].values[0].strip()
            if questionnaire not in old_questionnaires:
                qnum = 1
                old_questionnaires.append(questionnaire)
            else:
                qnum += 1

            question_label = language_string(question)
            question_iri = check_iri("{0}_Q{1}".format(questionnaire, qnum))

            predicates_list = []
            predicates_list.append(("a", ":Question"))
            predicates_list.append(("rdfs:label", question_label))
            predicates_list.append((":hasQuestionText", question_label))
            predicates_list.append((":isReferencedBy", check_iri(questionnaire)))

            paper_instructions_preamble = row[1]["paper_instructions_preamble"].strip()
            paper_instructions = row[1]["paper_instructions"].strip()
            digital_instructions_preamble = row[1]["digital_instructions_preamble"].strip()
            digital_instructions = row[1]["digital_instructions"].strip()
            response_options = row[1]["response_options"]

            if digital_instructions_preamble not in exclude_list:
                predicates_list.append((":hasInstructionsPreamble",
                                        check_iri(digital_instructions_preamble)))
                statements = add_to_statements(
                    check_iri(digital_instructions_preamble),
                    ":hasInstructionsPreambleText",
                    language_string(digital_instructions_preamble),
                    statements,
                    exclude_list
                )
            if digital_instructions not in exclude_list:
                predicates_list.append((":hasInstructions",
                                        language_string(digital_instructions)))
                statements = add_to_statements(
                    check_iri(digital_instructions),
                    ":hasInstructionsText",
                    language_string(digital_instructions),
                    statements,
                    exclude_list
                )
            if paper_instructions_preamble not in exclude_list and \
                paper_instructions_preamble != digital_instructions_preamble:

                predicates_list.append((":hasPaperInstructionsPreamble",
                                        check_iri(paper_instructions_preamble)))
                statements = add_to_statements(
                    check_iri(paper_instructions_preamble),
                    ":hasPaperInstructionsPreambleText",
                    language_string(paper_instructions_preamble),
                    statements,
                    exclude_list
                )
            if paper_instructions not in exclude_list and \
                paper_instructions != digital_instructions:

                predicates_list.append((":hasPaperInstructions",
                                        check_iri(paper_instructions)))
                statements = add_to_statements(
                    check_iri(paper_instructions),
                    ":hasPaperInstructionsText",
                    language_string(paper_instructions),
                    statements,
                    exclude_list
                )

            if response_options not in exclude_list:
                response_options = response_options.strip('-')
                response_options = response_options.replace("\n", "")
                response_options_iri = check_iri(response_options)
                if '"' in response_options:
                    response_options = re.findall('[-+]?[0-9]+=".*?"',
                                                  response_options)
                else:
                    response_options = response_options.split(",")
                #print(row[1]["index"], ' response options: ', response_options)

                statements = add_to_statements(
                    question_iri,
                    ":hasResponseOptions",
                    response_options_iri,
                    statements,
                    exclude_list
                )
                statements = add_to_statements(response_options_iri,
                                               "a", "rdf:Seq",
                                               statements, exclude_list)
                for iresponse, response_option in enumerate(response_options):
                    response = response_option.split("=")[1].strip()
                    if response in exclude_list:
                        response_iri = ":Empty"
                    else:
                        response_iri = check_iri(response)
                        statements = add_to_statements(
                            response_iri,
                            ":hasResponseOptionText",
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

            indices_response_type = row[1]["indices_response_type"]
            if indices_response_type not in exclude_list:
                if isinstance(indices_response_type, float) or \
                        isinstance(indices_response_type, int):
                    indices = [np.int(indices_response_type)]
                else:
                    indices = [np.int(x) for x in
                               indices_response_type.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = response_types[response_types["index"] ==
                                               index]["response_type"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasResponseType",
                                                check_iri(objectRDF, 'PascalCase')))
            # index_scale_type = row[1]["scale_type"]
            # index_value_type = row[1]["value_type"]
            # num_options = row[1]["num_options"]
            # index_neutral = row[1]["index_neutral"]
            # index_min = row[1]["index_min_extreme_oo_unclear_na_none"]
            # index_max = row[1]["index_max_extreme_oo_unclear_na_none"]
            # index_dontknow = row[1]["index_dontknow_na"]
            # if index_scale_type not in exclude_list:
            #     scale_type_iri = scale_types[scale_types["index"] ==
            #                                  index_scale_type]["IRI"].values[0]
            #     if scale_type_iri in exclude_list:
            #         scale_type_iri = check_iri(scale_types[scale_types["index"] ==
            #                                   index_scale_type]["scale_type"].values[0], 'PascalCase')
            #     if scale_type_iri not in exclude_list:
            #         predicates_list.append((":hasScaleType", check_iri(scale_type_iri, 'PascalCase')))
            # if index_value_type not in exclude_list:
            #     value_type_iri = value_types[value_types["index"] ==
            #                                  index_value_type]["IRI"].values[0]
            #     if value_type_iri in exclude_list:
            #         value_type_iri = check_iri(value_types[value_types["index"] ==
            #                                   index_value_type]["value_type"].values[0], 'PascalCase')
            #     if value_type_iri not in exclude_list:
            #         predicates_list.append((":hasValueType", check_iri(value_type_iri, 'PascalCase')))
            # if num_options not in exclude_list:
            #     predicates_list.append((":hasNumberOfOptions",
            #                             '"{0}"^^xsd:nonNegativeInteger'.format(
            #                                 num_options)))
            # if index_neutral not in exclude_list:
            #     if index_neutral not in ['oo', 'n/a']:
            #         predicates_list.append((":hasNeutralValueForResponseIndex",
            #                                 '"{0}"^^xsd:integer'.format(
            #                                     index_neutral)))
            # if index_min not in exclude_list:
            #     if index_min not in ['oo', 'n/a']:
            #         predicates_list.append((":hasExtremeValueForResponseIndex",
            #                                 '"{0}"^^xsd:integer'.format(
            #                                     index_min)))
            # if index_max not in exclude_list:
            #     if index_max not in ['oo', 'n/a']:
            #         predicates_list.append((":hasExtremeValueForResponseIndex",
            #                                 '"{0}"^^xsd:integer'.format(
            #                                     index_max)))
            # if index_dontknow not in exclude_list:
            #     predicates_list.append((":hasDontKnowOrNanForResponseIndex",
            #                             '"{0}"^^xsd:integer'.format(
            #                                 index_dontknow)))

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
        response_type = row[1]["response_type"].strip()
        if response_type not in exclude_list:

            response_type_iri = check_iri(response_type, 'PascalCase')
            response_type_label = language_string(response_type)
            statements = add_to_statements(
                response_type_iri, "rdfs:subClassOf", ":ResponseType",
                statements, exclude_list)
            statements = add_to_statements(
                response_type_iri, "rdfs:label", response_type_label,
                statements, exclude_list)

            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))
            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                     equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))

    # tasks worksheet
    for row in tasks.iterrows():
        name = row[1]["name"].strip()
        if name not in exclude_list:

            task_label = language_string(name)
            task_iri = check_iri(name, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:subClassOf", ":Task"))
            predicates_list.append(("rdfs:label", task_label))

            if row[1]["description"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["description"])))
            if row[1]["aliases"] not in exclude_list:
                aliases = row[1]["aliases"].split(',')
                for alias in aliases:
                    predicates_list.append(("rdfs:label", language_string(alias)))

            # # Cognitive Atlas-specific column
            # cogatlas_node_id = check_iri(row[1]["cogatlas_node_id"])
            # if cogatlas_node_id not in exclude_list:
            #     predicates_list.append((":hasCognitiveAtlasNodeID",
            #                             cogatlas_node_id))

            for predicates in predicates_list:
                statements = add_to_statements(
                    task_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # task_implementations worksheet
    for row in implementations.iterrows():
        implementation = row[1]["implementation"].strip()
        if implementation not in exclude_list:

            implementation_label = language_string(implementation)
            implementation_iri = check_iri(implementation)

            predicates_list = []
            predicates_list.append(("a", ":TaskImplementation"))
            predicates_list.append(("rdfs:label", implementation_label))
            if row[1]["description"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["description"])))
            if row[1]["link"] not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(row[1]["link"].strip())))

            # indices to other worksheets
            indices_task = row[1]["indices_task"]
            indices_project = row[1]["indices_project"]
            if indices_task not in exclude_list:
                if isinstance(indices_task, float) or \
                        isinstance(indices_task, int):
                    indices = [np.int(indices_task)]
                else:
                    indices = [np.int(x) for x in
                               indices_task.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = tasks[tasks["index"] == index]["name"].values[0]
                    if isinstance(objectRDF, str):
                        #predicates_list.append(("rdfs:subClassOf",
                        #                        check_iri(objectRDF, 'PascalCase')))
                        statements = add_to_statements(
                            check_iri(objectRDF, 'PascalCase'), ":hasTaskImplementation",
                            implementation_iri, statements, exclude_list)
            if indices_project not in exclude_list:
                if isinstance(indices_project, float) or \
                        isinstance(indices_project, int):
                    indices = [np.int(indices_project)]
                else:
                    indices = [np.int(x) for x in
                               indices_project.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = projects[projects["index"] ==
                                         index]["project"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasProject",
                                                "mhdb-resources" + check_iri(objectRDF)))

            # # Cognitive Atlas-specific column
            # cogatlas_node_id = row[1]["cogatlas_node_id"]
            # if cogatlas_node_id not in exclude_list:
            #     predicates_list.append((":hasCognitiveAtlasNodeID",
            #                             check_iri(cogatlas_node_id)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    implementation_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # task_conditions worksheet
    for row in conditions.iterrows():
        condition = row[1]["condition"].strip()
        if condition not in exclude_list:

            condition_label = language_string(condition)
            condition_iri = check_iri(condition)

            predicates_list = []
            predicates_list.append(("a", ":TaskCondition"))
            predicates_list.append(("rdfs:label", condition_label))
            if row[1]["description"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["description"])))

            # # Cognitive Atlas-specific column
            # cogatlas_node_id = row[1]["cogatlas_node_id"]
            # if cogatlas_node_id not in exclude_list:
            #     predicates_list.append((":hasCognitiveAtlasNodeID",
            #                             check_iri(cogatlas_node_id)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    condition_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # task_contrasts worksheet
    for row in contrasts.iterrows():
        contrast = row[1]["contrast"].strip()
        if contrast not in exclude_list:

            contrast_label = language_string(contrast)
            contrast_iri = check_iri(contrast)

            predicates_list = []
            predicates_list.append(("a", ":TaskContrast"))
            predicates_list.append(("rdfs:label", contrast_label))

            # # Cognitive Atlas-specific column
            # cogatlas_node_id = row[1]["cogatlas_node_id"]
            # if cogatlas_node_id not in exclude_list:
            #     predicates_list.append((":hasCognitiveAtlasNodeID",
            #                             check_iri(cogatlas_node_id)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    contrast_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # task_indicators worksheet
    for row in indicators.iterrows():
        indicator = row[1]["indicator"].strip()
        if indicator not in exclude_list:

            indicator_label = language_string(indicator)
            indicator_iri = check_iri(indicator)

            predicates_list = []
            predicates_list.append(("a", ":TaskIndicator"))
            predicates_list.append(("rdfs:label", indicator_label))

            # # Cognitive Atlas-specific column
            # cogatlas_node_id = row[1]["cogatlas_node_id"]
            # if cogatlas_node_id not in exclude_list:
            #     predicates_list.append((":hasCognitiveAtlasNodeID",
            #                             check_iri(cogatlas_node_id)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    indicator_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # task_assertions_indices worksheet
    for row in assertions_indices.iterrows():

        reln_type = str(row[1]["cogatlas_reln_type"])
        startNode = int(row[1]["cogatlas_startNode"])
        endNode = int(row[1]["cogatlas_endNode"])
        subject = ""
        object = ""

        # Find subject and object from the different worksheets

        # tasks worksheet
        if subject in exclude_list:
            subject_task = tasks[tasks['cogatlas_node_id'] == startNode]["name"]
            if not subject_task.empty:
                subject = tasks[tasks['cogatlas_node_id'] == startNode]["name"].values[0]
            subject_label_type = 'PascalCase'
        if object in exclude_list:
            object_task = tasks[tasks['cogatlas_node_id'] == endNode]["name"]
            if not object_task.empty:
                object = tasks[tasks['cogatlas_node_id'] == endNode]["name"].values[0]
            object_label_type = 'PascalCase'

        # task_implementations worksheet
        if subject in exclude_list:
            subject_implementation = implementations[implementations['cogatlas_node_id'] == startNode]["implementation"]
            if not subject_implementation.empty:
                subject = implementations[implementations['cogatlas_node_id'] == startNode]["implementation"].values[0]
            subject_label_type = 'delimited'
        if object in exclude_list:
            object_implementation = implementations[implementations['cogatlas_node_id'] == endNode]["implementation"]
            if not object_implementation.empty:
                object = implementations[implementations['cogatlas_node_id'] == endNode]["implementation"].values[0]
            object_label_type = 'delimited'

        # task_indicators worksheet
        if subject in exclude_list:
            subject_indicator = indicators[indicators['cogatlas_node_id'] == startNode]["indicator"]
            if not subject_indicator.empty:
                subject = indicators[indicators['cogatlas_node_id'] == startNode]["indicator"].values[0]
            subject_label_type = 'delimited'
        if object in exclude_list:
            object_indicator = indicators[indicators['cogatlas_node_id'] == endNode]["indicator"]
            if not object_indicator.empty:
                object = indicators[indicators['cogatlas_node_id'] == endNode]["indicator"].values[0]
            object_label_type = 'delimited'

        # task_conditions worksheet
        if subject in exclude_list:
            subject_condition = conditions[conditions['cogatlas_node_id'] == startNode]["condition"]
            if not subject_condition.empty:
                subject = conditions[conditions['cogatlas_node_id'] == startNode]["condition"].values[0]
            subject_label_type = 'delimited'
        if object in exclude_list:
            object_condition = conditions[conditions['cogatlas_node_id'] == endNode]["condition"]
            if not object_condition.empty:
                object = conditions[conditions['cogatlas_node_id'] == endNode]["condition"].values[0]
            object_label_type = 'delimited'

        # task_contrasts worksheet
        if subject in exclude_list:
            subject_contrast = contrasts[contrasts['cogatlas_node_id'] == startNode]["contrast"]
            if not subject_contrast.empty:
                subject = contrasts[contrasts['cogatlas_node_id'] == startNode]["contrast"].values[0]
            subject_label_type = 'delimited'
        if object in exclude_list:
            object_contrast = contrasts[contrasts['cogatlas_node_id'] == endNode]["contrast"]
            if not object_contrast.empty:
                object = contrasts[contrasts['cogatlas_node_id'] == endNode]["contrast"].values[0]
            object_label_type = 'delimited'

        if subject not in exclude_list and object not in exclude_list and not subject == object:

            # Build subject - predicate - object triple
            subject_iri = check_iri(subject, subject_label_type)
            object_iri = check_iri(object, object_label_type)

            if reln_type == "ASSERTS":
                object_iri = check_iri(object, 'PascalCase')
                reln_type = ":assertsCognitiveAtlasConcept"
                # task -> asserts -> concept (identify concept)
                statements = add_to_statements(
                    object_iri, "rdfs:subClassOf", ":CognitiveAtlasConcept",
                    statements, exclude_list
                )
                statements = add_to_statements(
                    object_iri, "rdfs:label", language_string(object),
                    statements, exclude_list
                )
            elif reln_type == "HASCITATION":
                predicate_iri = ":hasBibliographicCitation"
                object_iri = check_iri(object, object_label_type)
            elif reln_type == "HASCONDITION":
                predicate_iri = ":hasTaskCondition"
                object_iri = check_iri(object, object_label_type)
            elif reln_type == "HASCONTRAST":
                predicate_iri = ":hasTaskContrast"
                object_iri = check_iri(object)
            elif reln_type == "HASIMPLEMENTATION":
                predicate_iri = ":hasTaskImplementation"
                object_iri = check_iri(object)
            elif reln_type == "HASINDICATOR":
                predicate_iri = ":hasTaskIndicator"
                object_iri = check_iri(object)
            elif reln_type == "KINDOF":
                predicate_iri = ":isKindOf"
                object_iri = check_iri(object, 'PascalCase')
            elif reln_type == "MEASUREDBY":
                predicate_iri = ":measuredBy"
                object_iri = check_iri(object)
            elif reln_type == "PARTOF":
                predicate_iri = ":isPartOf"
                object_iri = check_iri(object, 'PascalCase')
            else:
                predicate_iri = ""
            # if reln_type == "HASDIFFERENCE":
            # if reln_type == "CLASSIFIEDUNDER":
            # if reln_type == "ISA":
            # if reln_type == "PREDICATE":
            # if reln_type == "PREDICATE_DEF":
            # if reln_type == "SUBJECT":

            if predicate_iri not in exclude_list:
                #print('"{0}", {1}, "{2}"'.format(subject, predicate_iri, object))

                statements = add_to_statements(
                    subject_iri, predicate_iri, object_iri,
                    statements, exclude_list
                )

    # references worksheet
    for row in references.iterrows():
        title = row[1]["title"]
        if title not in exclude_list:
            predicates_list = []

            # reference IRI
            reference_iri = check_iri(title)
            predicates_list.append(("a", ":BibliographicResource"))
            predicates_list.append(("rdfs:label", language_string(title)))
            predicates_list.append((":hasTitle", language_string(title)))

            # general columns
            link = row[1]["link"]
            if link not in exclude_list:
                predicates_list.append((":hasWebsite",
                                        '"{0}"^^xsd:anyURI'.format(link.strip())))
            entry_date = row[1]["entry_date"]
            if entry_date not in exclude_list:
                predicates_list.append((":hasDateLastUpdated",
                                        language_string(entry_date)))

            # research article-specific columns
            authors = row[1]["authors"]
            pubdate = row[1]["pubdate"]
            PubMedID = row[1]["PubMedID"]
            if authors not in exclude_list:
                predicates_list.append((":hasAuthorList",
                                        language_string(authors)))
            if pubdate not in exclude_list:
                predicates_list.append((":hasPublicationDate",
                                        language_string(pubdate)))
            if PubMedID not in exclude_list:
                predicates_list.append((":hasPubMedID",
                                        '"{0}"^^xsd:nonNegativeInteger'.format(int(PubMedID))))

            # # Cognitive Atlas-specific column
            # cogatlas_node_id = row[1]["cogatlas_node_id"]
            # if cogatlas_node_id not in exclude_list:
            #     predicates_list.append((":hasCognitiveAtlasNodeID",
            #                             check_iri(cogatlas_node_id)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    reference_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    return statements


def ingest_measures(measures_xls, statements={}):
    """
    Function to ingest measures spreadsheet

    Parameters
    ----------
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

    # load worksheets as pandas dataframes
    measures_classes = measures_xls.parse("Classes")
    measures_properties = measures_xls.parse("Properties")
    sensors = measures_xls.parse("sensors")
    measures = measures_xls.parse("measures")
    scales = measures_xls.parse("scales")

    # fill NANs with emptyValue
    measures_classes = measures_classes.fillna(emptyValue)
    measures_properties = measures_properties.fillna(emptyValue)
    sensors = sensors.fillna(emptyValue)
    measures = measures.fillna(emptyValue)
    scales = scales.fillna(emptyValue)

    # Classes worksheet
    for row in measures_classes.iterrows():
        class_iri = check_iri(row[1]["ClassName"])
        class_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Class"))
        predicates_list.append(("rdfs:label", class_label))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs", row[1]["sameAs"]))
        if row[1]["equivalentClasses"] not in exclude_list:
            equivalentClasses = row[1]["equivalentClasses"]
            equivalentClasses = [x.strip() for x in
                             equivalentClasses.strip().split(',') if len(x) > 0]
            for equivalentClass in equivalentClasses:
                if equivalentClass not in exclude_list:
                    predicates_list.append(("rdfs:equivalentClass",
                                            equivalentClass))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in measures_properties.iterrows():
        property_iri = check_iri(row[1]["property"])
        property_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Property"))
        predicates_list.append(("rdfs:label", property_label))
        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    row[1]["sameAs"]))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    row[1]["equivalentProperty"]))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # sensors worksheet
    for row in sensors.iterrows():
        sensor = row[1]["sensor"].strip()
        if sensor not in exclude_list:

            sensor_label = language_string(sensor)
            sensor_iri = check_iri(sensor, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", sensor_label))

            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))

            aliases = row[1]["aliases"]
            if aliases not in exclude_list:
                aliases = [x for x in aliases.strip().split(',') if len(x)>0]
                for alias in aliases:
                    if alias not in exclude_list:
                        if isinstance(alias, str):
                            predicates_list.append(("rdfs:label", language_string(alias)))

            indices_sensor = row[1]["indices_sensor"]
            if indices_sensor not in exclude_list:
                if isinstance(indices_sensor, float) or \
                        isinstance(indices_sensor, int):
                    indices = [np.int(indices_sensor)]
                else:
                    indices = [np.int(x) for x in
                               indices_sensor.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = sensors[sensors["index"]  ==
                                              index]["sensor"].values[0]
                    if objectRDF not in exclude_list:
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":SensingDevice"))

            indices_measure = row[1]["indices_measure"]
            if indices_measure not in exclude_list:
                if isinstance(indices_measure, float) or \
                        isinstance(indices_measure, int):
                    indices = [np.int(indices_measure)]
                else:
                    indices = [np.int(x) for x in
                               indices_measure.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = measures[measures["index"] ==
                                         index]["measure"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":measuresQuantityKind",
                                                check_iri(objectRDF, 'PascalCase')))

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
        measure = row[1]["measure"].strip()
        if measure not in exclude_list:

            measure_label = language_string(measure)
            measure_iri = check_iri(measure, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", measure_label))

            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            aliases = row[1]["aliases"]
            if aliases not in exclude_list:
                aliases = [x for x in aliases.strip().split(',') if len(x)>0]
                for alias in aliases:
                    if alias not in exclude_list:
                        if isinstance(alias, str):
                            predicates_list.append(("rdfs:label", language_string(alias)))

            indices_measure = row[1]["indices_measure"]
            if indices_measure not in exclude_list:
                if isinstance(indices_measure, float) or \
                        isinstance(indices_measure, int):
                    indices = [np.int(indices_measure)]
                else:
                    indices = [np.int(x) for x in
                               indices_measure.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = measures[measures["index"] ==
                                         index]["measure"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":QuantityKind"))

            for predicates in predicates_list:
                statements = add_to_statements(
                    measure_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # scales worksheet
    for row in scales.iterrows():
        scale = row[1]["scale"].strip()
        if scale not in exclude_list:

            scale_label = language_string(scale)
            scale_iri = check_iri(scale, 'PascalCase')

            predicates_list = []
            predicates_list.append(("rdfs:label", scale_label))

            if row[1]["definition"] not in exclude_list:
                predicates_list.append(("rdfs:comment",
                                        language_string(row[1]["definition"])))

            if row[1]["equivalentClasses"] not in exclude_list:
                equivalentClasses = row[1]["equivalentClasses"]
                equivalentClasses = [x.strip() for x in
                                 equivalentClasses.strip().split(',') if len(x) > 0]
                for equivalentClass in equivalentClasses:
                    if equivalentClass not in exclude_list:
                        predicates_list.append(("rdfs:equivalentClass",
                                                equivalentClass))
            aliases = row[1]["aliases"]
            if aliases not in exclude_list:
                aliases = [x for x in aliases.strip().split(',') if len(x)>0]
                for alias in aliases:
                    if alias not in exclude_list:
                        if isinstance(alias, str):
                            predicates_list.append(("rdfs:label", language_string(alias)))

            indices_scale = row[1]["indices_scale"]
            if indices_scale not in exclude_list:
                if isinstance(indices_scale, float) or \
                        isinstance(indices_scale, int):
                    indices = [np.int(indices_scale)]
                else:
                    indices = [np.int(x) for x in
                               indices_scale.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = scales[scales["index"] ==
                                         index]["scale"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append(("rdfs:subClassOf",
                                                check_iri(objectRDF, 'PascalCase')))
            else:
                predicates_list.append(("rdfs:subClassOf", ":Scale"))

            for predicates in predicates_list:
                statements = add_to_statements(
                    scale_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    return statements

def ingest_chills(chills_xls, statements={}):
    """
    Function to ingest chills spreadsheet

    Parameters
    ----------
    chills_xls: pandas ExcelFile

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
    chills_classes = chills_xls.parse("Classes")
    chills_properties = chills_xls.parse("Properties")
    papers = chills_xls.parse("Index")
    article_types = chills_xls.parse("ArticleType")
    researchers = chills_xls.parse("ChillsPeople")
    studies = chills_xls.parse("ResearchStudyOnProjectLink1")
    stimulus_categories = chills_xls.parse("StimulusCategory")
    units = chills_xls.parse("unit")
    subjective_sensors = chills_xls.parse("SubjectiveSensor")
    subjective_measures = chills_xls.parse("SubjectiveMeasure")
    inferences = chills_xls.parse("Inference")
    claims = chills_xls.parse("claims")
    brain_areas = chills_xls.parse("BrainAreas")
    definitions_of_chills = chills_xls.parse("DefinitionOfChills_index")
    sensors = chills_xls.parse("Sensors")
    measures = chills_xls.parse("Measure")
    #measure_categories = chills_xls.parse("MeasureCategories")

    # fill NANs with emptyValue
    chills_classes = chills_classes.fillna(emptyValue)
    chills_properties = chills_properties.fillna(emptyValue)
    papers = papers.fillna(emptyValue)
    article_types = article_types.fillna(emptyValue)
    researchers = researchers.fillna(emptyValue)
    studies = studies.fillna(emptyValue)
    stimulus_categories = stimulus_categories.fillna(emptyValue)
    units = units.fillna(emptyValue)
    subjective_sensors = subjective_sensors.fillna(emptyValue)
    subjective_measures = subjective_measures.fillna(emptyValue)
    inferences = inferences.fillna(emptyValue)
    claims = claims.fillna(emptyValue)
    brain_areas = brain_areas.fillna(emptyValue)
    definitions_of_chills = definitions_of_chills.fillna(emptyValue)
    sensors = sensors.fillna(emptyValue)
    measures = measures.fillna(emptyValue)
    #measure_categories = measure_categories.fillna(emptyValue)

    print("beginnign")
    print(type(sensors["index"][0]))
    print(type(measures["index"][0]))
    print("end of beginning")


    # Classes worksheet
    for row in chills_classes.iterrows():
        class_iri = check_iri(row[1]["ClassName"])
        class_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Class"))
        predicates_list.append(("rdfs:label", class_label))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs", row[1]["sameAs"]))
        if row[1]["equivalentClasses"] not in exclude_list:
            equivalentClasses = row[1]["equivalentClasses"]
            equivalentClasses = [x.strip() for x in
                             equivalentClasses.strip().split(',') if len(x) > 0]
            for equivalentClass in equivalentClasses:
                if equivalentClass not in exclude_list:
                    predicates_list.append(("rdfs:equivalentClass",
                                            equivalentClass))
        if row[1]["subClassOf"] not in exclude_list:
            predicates_list.append(("rdfs:subClassOf",
                                    check_iri(row[1]["subClassOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                class_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # Properties worksheet
    for row in chills_properties.iterrows():
        property_iri = check_iri(row[1]["property"])
        property_label = language_string(row[1]["label"])
        predicates_list = []
        predicates_list.append(("a", "rdf:Property"))
        predicates_list.append(("rdfs:label", property_label))
        if row[1]["propertyDomain"] not in exclude_list:
            predicates_list.append(("rdfs:domain",
                                    check_iri(row[1]["propertyDomain"])))
        if row[1]["propertyRange"] not in exclude_list:
            predicates_list.append(("rdfs:range",
                                    check_iri(row[1]["propertyRange"])))
        if row[1]["definition"] not in exclude_list:
            predicates_list.append(("rdfs:comment",
                                    language_string(row[1]["definition"])))
        if row[1]["sameAs"] not in exclude_list:
            predicates_list.append(("owl:sameAs",
                                    row[1]["sameAs"]))
        if row[1]["equivalentProperty"] not in exclude_list:
            predicates_list.append(("rdfs:equivalentProperty",
                                    row[1]["equivalentProperty"]))
        if row[1]["subPropertyOf"] not in exclude_list:
            predicates_list.append(("rdfs:subPropertyOf",
                                    check_iri(row[1]["subPropertyOf"])))
        for predicates in predicates_list:
            statements = add_to_statements(
                property_iri,
                predicates[0],
                predicates[1],
                statements,
                exclude_list
            )

    # papers worksheet
    for row in papers.iterrows():
        paper = row[1]["Reseach study (research paper tilte)"].strip()
        if paper not in exclude_list:

            paper_label = language_string(paper)
            paper_iri = check_iri(paper, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":Paper"))
            predicates_list.append(("rdfs:label", paper_label))

            # if row[1]["definition"] not in exclude_list:
            #     predicates_list.append(("rdfs:comment",
            #                             language_string(row[1]["definition"])))

            # if row[1]["equivalentClasses"] not in exclude_list:
            #     equivalentClasses = row[1]["equivalentClasses"]
            #     equivalentClasses = [x.strip() for x in
            #                      equivalentClasses.strip().split(',') if len(x) > 0]
            #     for equivalentClass in equivalentClasses:
            #         if equivalentClass not in exclude_list:
            #             predicates_list.append(("rdfs:equivalentClass",
            #                                     equivalentClass))

            # aliases = row[1]["aliases"]
            # if aliases not in exclude_list:
            #     aliases = [x for x in aliases.strip().split(',') if len(x)>0]
            #     for alias in aliases:
            #         if alias not in exclude_list:
            #             if isinstance(alias, str):
            #                 predicates_list.append(("rdfs:label", language_string(alias)))

            indices_article_type = row[1]["ArticleType"]
            if indices_article_type not in exclude_list:
                if isinstance(indices_article_type, float) or \
                        isinstance(indices_article_type, int):
                    indices = [np.int(indices_article_type)]
                else:
                    indices = [np.int(x) for x in
                               indices_article_type.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = article_types[article_types["index"]  ==
                                              index]["ArticleType"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasArticleType",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_primary_researchers = row[1]["ChillsPeople_index"]
            if indices_primary_researchers not in exclude_list:
                if isinstance(indices_primary_researchers, float) or \
                        isinstance(indices_primary_researchers, int):
                    indices = [np.int(indices_primary_researchers)]
                else:
                    indices = [np.int(x) for x in
                               indices_primary_researchers.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = researchers[researchers["index"] ==
                                         index]["Affiliate1"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasPrimaryResearcher",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_secondary_researchers = row[1]["ChillsPeople_secondary_index"]
            if indices_secondary_researchers not in exclude_list:
                if isinstance(indices_secondary_researchers, float) or \
                        isinstance(indices_secondary_researchers, int):
                    indices = [np.int(indices_secondary_researchers)]
                else:
                    indices = [np.int(x) for x in
                               indices_secondary_researchers.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(researchers[researchers["index"] == index]["Affiliate1"])
                    objectRDF = researchers[researchers["index"] ==
                                         index]["Affiliate1"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasSecondaryResearcher",
                                                check_iri(objectRDF, 'PascalCase')))

            # indices_studies = row[1]["ResearchStudyOnProjectLink1"]
            # if indices_studies not in exclude_list:
            #     if isinstance(indices_studies, float) or \
            #             isinstance(indices_studies, int):
            #         indices = [np.int(indices_studies)]
            #     else:
            #         indices = [np.int(x) for x in
            #                    indices_studies.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         url = studies[studies["index"] ==
            #                              index]["ResearchStudies"].values[0]
            #         if isinstance(url, str):
            #             predicates_list.append((":hasStudy",
            #                                     '"{0}"^^xsd:anyURI'.format(url)))

            indices_stimulus_categories = row[1]["StimulusCategory"]
            if indices_stimulus_categories not in exclude_list:
                if isinstance(indices_stimulus_categories, float) or \
                        isinstance(indices_stimulus_categories, int):
                    indices = [np.int(indices_stimulus_categories)]
                else:
                    indices = [np.int(x) for x in
                               indices_stimulus_categories.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = stimulus_categories[stimulus_categories["index"] ==
                                         index]["StimulusCategory"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasStimulusCategory",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_units = row[1]["unit_index"]
            if indices_units not in exclude_list:
                if isinstance(indices_units, float) or \
                        isinstance(indices_units, int):
                    indices = [np.int(indices_units)]
                else:
                    indices = [np.int(x) for x in
                               indices_units.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = units[units["index"] ==
                                         index]["unit"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasUnit",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_subjective_sensors = row[1]["SubjectiveSensor_index"]
            if indices_subjective_sensors not in exclude_list:
                if isinstance(indices_subjective_sensors, float) or \
                        isinstance(indices_subjective_sensors, int):
                    indices = [np.int(indices_subjective_sensors)]
                else:
                    indices = [np.int(x) for x in
                               indices_subjective_sensors.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = subjective_sensors[subjective_sensors["index"] ==
                                         index]["SubjectiveData"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasSubjectiveSensor",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_subjective_measures = row[1]["SubjectiveMeasure_index"]
            if indices_subjective_measures not in exclude_list:
                if isinstance(indices_subjective_measures, float) or \
                        isinstance(indices_subjective_measures, int):
                    indices = [np.int(indices_subjective_measures)]
                else:
                    indices = [np.int(x) for x in
                               indices_subjective_measures.strip().split(',') if len(x)>0]
                for index in indices:
                    objectRDF = subjective_measures[subjective_measures["index"] ==
                                         index]["SubjectiveMeasure"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasSubjectiveMeasure",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_inferences = row[1]["Inference_index"]
            if indices_inferences not in exclude_list:
                if isinstance(indices_inferences, float) or \
                        isinstance(indices_inferences, int):
                    indices = [np.int(indices_inferences)]
                else:
                    indices = [np.int(x) for x in
                               indices_inferences.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(inferences[inferences["index"] == index])
                    objectRDF = inferences[inferences["index"] ==
                                         index]["inference"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasInference",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_claims = row[1]["claims_index"]
            if indices_claims not in exclude_list:
                if isinstance(indices_claims, float) or \
                        isinstance(indices_claims, int):
                    indices = [np.int(indices_claims)]
                else:
                    indices = [np.int(x) for x in
                               indices_claims.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(claims[claims["index"] == index])
                    objectRDF = claims[claims["index"] ==
                                         index]["claims"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasClaim",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_brain_areas = row[1]["Brain areas"]
            if indices_brain_areas not in exclude_list:
                if isinstance(indices_brain_areas, float) or \
                        isinstance(indices_brain_areas, int):
                    indices = [np.int(indices_brain_areas)]
                else:
                    indices = [np.int(x) for x in
                               indices_brain_areas.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(claims[claims["index"] == index])
                    objectRDF = brain_areas[brain_areas["index"] ==
                                         index]["BrainAreas"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasBrainArea",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_definitions_of_chills = row[1]["Definition of chills"]
            if indices_definitions_of_chills not in exclude_list:
                if isinstance(indices_definitions_of_chills, float) or \
                        isinstance(indices_definitions_of_chills, int):
                    indices = [np.int(indices_definitions_of_chills)]
                else:
                    indices = [np.int(x) for x in
                               indices_definitions_of_chills.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(claims[claims["index"] == index])
                    objectRDF = definitions_of_chills[definitions_of_chills["index"] ==
                                         index]["DefinitionOfChills"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasDefinitionOfChills",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_sensors = row[1]["sensor_index"]
            if indices_sensors not in exclude_list:
                if isinstance(indices_sensors, float) or \
                        isinstance(indices_sensors, int):
                    indices = [np.int(indices_sensors)]
                else:
                    indices = [np.int(x) for x in
                               indices_sensors.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(sensors["index"] == index)
                    #print(sensors["index"][23])
                    #print(sensors[23])
                    #print(sensors[sensors["index"] == index])
                    #print(sensors)
                    #print(sensors["index"][index-1] == index)
                    #print(type(sensors["index"][index-1]))
                    #rint(type(index))
                    objectRDF = sensors[sensors["index"] ==
                                         index]["sensor"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasSensor",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_measures = row[1]["measure_index"]
            if indices_measures not in exclude_list:
                if isinstance(indices_measures, float) or \
                        isinstance(indices_measures, int):
                    indices = [np.int(indices_measures)]
                    #print("float")
                    #print(type(indices[0]))
                    #print("\n")
                else:
                    indices = [np.int(x) for x in
                               indices_measures.strip().split(',') if len(x)>0]
                    #print("not float")
                    #print(type(indices[0]))
                    #print("\n")
                for index in indices:
                    print(index)
                    #print(measures)
                    #print(measures["measure"][1])
                    #print(measures["index"][index-1])
                    #print(measures["index"][index-1] == index)
                    #print(type(measures["index"][index-1]))
                    #print(type(index))
                    #print(measures["index"] == index)
                    #print(measures[measures["index"] == index])
                    objectRDF = measures[measures["index"] ==
                                         index]["measure"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasMeasure",
                                                check_iri(objectRDF, 'PascalCase')))

            number_of_subjects = row[1]["N subjects"]
            if number_of_subjects not in exclude_list:
                predicates_list.append((":hasNumberOfSubjects",
                                        '"{0}"^^xsd:int'.format(number_of_subjects)))

            modulator = row[1]["Modulator"]
            if modulator not in exclude_list:
                predicates_list.append((":hasModulator",
                                        language_string(modulator)))

            url = row[1]["URL"]
            if url not in exclude_list:
                predicates_list.append((":hasURL",
                                        '"{0}"^^xsd:anyURI'.format(url.strip())))

            publication_year = row[1]["publication_year"]
            if publication_year not in exclude_list:
                predicates_list.append((":hasPublicationYear",
                                        '"{0}"^^xsd:gyear'.format(int(publication_year))))

            abstract = row[1]["abstract"]
            if abstract not in exclude_list:
                predicates_list.append((":hasAbstract",
                                        language_string(abstract)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    paper_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # article_type worksheet
    for row in article_types.iterrows():
        article_type = row[1]["ArticleType"].strip()
        if article_type not in exclude_list:

            article_type_label = language_string(article_type)
            article_type_iri = check_iri(article_type, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":ArticleType"))
            predicates_list.append(("rdfs:label", article_type_label))

            # if row[1]["definition"] not in exclude_list:
            #     predicates_list.append(("rdfs:comment",
            #                             language_string(row[1]["definition"])))

            # if row[1]["equivalentClasses"] not in exclude_list:
            #     equivalentClasses = row[1]["equivalentClasses"]
            #     equivalentClasses = [x.strip() for x in
            #                      equivalentClasses.strip().split(',') if len(x) > 0]
            #     for equivalentClass in equivalentClasses:
            #         if equivalentClass not in exclude_list:
            #             predicates_list.append(("rdfs:equivalentClass",
            #                                     equivalentClass))
            # aliases = row[1]["aliases"]
            # if aliases not in exclude_list:
            #     aliases = [x for x in aliases.strip().split(',') if len(x)>0]
            #     for alias in aliases:
            #         if alias not in exclude_list:
            #             if isinstance(alias, str):
            #                 predicates_list.append(("rdfs:label", language_string(alias)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    article_type_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # researchers worksheet
    for row in researchers.iterrows():
        researcher = row[1]["Affiliate1"].strip()
        if researcher not in exclude_list:

            researcher_label = language_string(researcher)
            researcher_iri = check_iri(researcher, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":Researcher"))
            predicates_list.append(("rdfs:label", researcher_label))

            discipline = row[1]["Discipline"] 
            if row[1]["Discipline"] not in exclude_list:
                predicates_list.append((":hasDiscipline",
                                        language_string(discipline)))

            lab = row[1]["Lab"] 
            if lab not in exclude_list:
                predicates_list.append((":hasLab",
                                        language_string(lab)))

            site = row[1]["Site"] 
            if site not in exclude_list:
                predicates_list.append((":hasSite",
                                        language_string(site)))

            url = row[1]["URL"] 
            if url not in exclude_list:
                predicates_list.append((":hasURL",
                                        '"{0}"^^xsd:anyURI'.format(url.strip())))

            contact = row[1]["Contact"] 
            if contact not in exclude_list:
                predicates_list.append((":hasContact",
                                        '"{0}"^^xsd:string'.format(contact)))                                

            # if row[1]["equivalentClasses"] not in exclude_list:
            #     equivalentClasses = row[1]["equivalentClasses"]
            #     equivalentClasses = [x.strip() for x in
            #                      equivalentClasses.strip().split(',') if len(x) > 0]
            #     for equivalentClass in equivalentClasses:
            #         if equivalentClass not in exclude_list:
            #             predicates_list.append(("rdfs:equivalentClass",
            #                                     equivalentClass))
            # aliases = row[1]["aliases"]
            # if aliases not in exclude_list:
            #     aliases = [x for x in aliases.strip().split(',') if len(x)>0]
            #     for alias in aliases:
            #         if alias not in exclude_list:
            #             if isinstance(alias, str):
            #                 predicates_list.append(("rdfs:label", language_string(alias)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    researcher_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # studies worksheet
    # for row in studies.iterrows():
    #     study = row[1]["ResearchStudies"].strip()
    #     if study not in exclude_list:

    #         study_label = language_string(study)
    #         study_iri = check_iri(study, 'PascalCase')

    #         predicates_list = []
    #         predicates_list.append(("a", ":Study"))
    #         predicates_list.append(("rdfs:label", study_label))

    #         year = row[1]["Year"]
    #         if year not in exclude_list:
    #             predicates_list.append((":hasPublicationYear",
    #                                     '"{0}"^^xsd:gyear'.format(int(year))))

    #         # if row[1]["equivalentClasses"] not in exclude_list:
    #         #     equivalentClasses = row[1]["equivalentClasses"]
    #         #     equivalentClasses = [x.strip() for x in
    #         #                      equivalentClasses.strip().split(',') if len(x) > 0]
    #         #     for equivalentClass in equivalentClasses:
    #         #         if equivalentClass not in exclude_list:
    #         #             predicates_list.append(("rdfs:equivalentClass",
    #         #                                     equivalentClass))
    #         # aliases = row[1]["aliases"]
    #         # if aliases not in exclude_list:
    #         #     aliases = [x for x in aliases.strip().split(',') if len(x)>0]
    #         #     for alias in aliases:
    #         #         if alias not in exclude_list:
    #         #             if isinstance(alias, str):
    #         #                 predicates_list.append(("rdfs:label", language_string(alias)))

    #         for predicates in predicates_list:
    #             statements = add_to_statements(
    #                 study_iri,
    #                 predicates[0],
    #                 predicates[1],
    #                 statements,
    #                 exclude_list
    #             )

    # stimulus categories worksheet
    for row in stimulus_categories.iterrows():
        stimulus_category = row[1]["StimulusCategory"].strip()
        if stimulus_category not in exclude_list:

            stimulus_category_label = language_string(stimulus_category)
            stimulus_category_iri = check_iri(stimulus_category, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":StimulusCategory"))
            predicates_list.append(("rdfs:label", stimulus_category_label))

            # if row[1]["equivalentClasses"] not in exclude_list:
            #     equivalentClasses = row[1]["equivalentClasses"]
            #     equivalentClasses = [x.strip() for x in
            #                      equivalentClasses.strip().split(',') if len(x) > 0]
            #     for equivalentClass in equivalentClasses:
            #         if equivalentClass not in exclude_list:
            #             predicates_list.append(("rdfs:equivalentClass",
            #                                     equivalentClass))
            # aliases = row[1]["aliases"]
            # if aliases not in exclude_list:
            #     aliases = [x for x in aliases.strip().split(',') if len(x)>0]
            #     for alias in aliases:
            #         if alias not in exclude_list:
            #             if isinstance(alias, str):
            #                 predicates_list.append(("rdfs:label", language_string(alias)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    stimulus_category_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )
        
    # units worksheet
    for row in units.iterrows():
        unit = row[1]["unit"].strip()
        if unit not in exclude_list:

            unit_label = language_string(unit)
            unit_iri = check_iri(unit, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":Unit"))
            predicates_list.append(("rdfs:label", unit_label))

            # if row[1]["equivalentClasses"] not in exclude_list:
            #     equivalentClasses = row[1]["equivalentClasses"]
            #     equivalentClasses = [x.strip() for x in
            #                      equivalentClasses.strip().split(',') if len(x) > 0]
            #     for equivalentClass in equivalentClasses:
            #         if equivalentClass not in exclude_list:
            #             predicates_list.append(("rdfs:equivalentClass",
            #                                     equivalentClass))
            # aliases = row[1]["aliases"]
            # if aliases not in exclude_list:
            #     aliases = [x for x in aliases.strip().split(',') if len(x)>0]
            #     for alias in aliases:
            #         if alias not in exclude_list:
            #             if isinstance(alias, str):
            #                 predicates_list.append(("rdfs:label", language_string(alias)))

            for predicates in predicates_list:
                statements = add_to_statements(
                    unit_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )
        
    # subjective_sensor worksheet
    for row in subjective_sensors.iterrows():
        subjective_sensor = row[1]["SubjectiveData"].strip()
        if subjective_sensor not in exclude_list:

            subjective_sensor_label = language_string(subjective_sensor)
            subjective_sensor_iri = check_iri(subjective_sensor, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":SubjectiveSensor"))
            predicates_list.append(("rdfs:label", subjective_sensor_label))

            for predicates in predicates_list:
                statements = add_to_statements(
                    subjective_sensor_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # subjective_measure worksheet
    for row in subjective_measures.iterrows():
        subjective_measure = row[1]["SubjectiveMeasure"].strip()
        if subjective_measure not in exclude_list:

            subjective_measure_label = language_string(subjective_measure)
            subjective_measure_iri = check_iri(subjective_measure, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":SubjectiveMeasure"))
            predicates_list.append(("rdfs:label", subjective_measure_label))

            for predicates in predicates_list:
                statements = add_to_statements(
                    subjective_measure_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # inferences worksheet
    for row in inferences.iterrows():
        inference = row[1]["inference"].strip()
        if inference not in exclude_list:

            inference_label = language_string(inference)
            inference_iri = check_iri(inference, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":Inference"))
            predicates_list.append(("rdfs:label", inference_label))

            for predicates in predicates_list:
                statements = add_to_statements(
                    inference_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # claims worksheet
    for row in claims.iterrows():
        claim = row[1]["claims"].strip()
        if claim not in exclude_list:

            claim_label = language_string(claim)
            claim_iri = check_iri(claim, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":Claim"))
            predicates_list.append(("rdfs:label", claim_label))

            for predicates in predicates_list:
                statements = add_to_statements(
                    claim_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # brain_areas worksheet
    for row in brain_areas.iterrows():
        brain_area = row[1]["BrainAreas"].strip()
        if brain_area not in exclude_list:

            brain_area_label = language_string(brain_area)
            brain_area_iri = check_iri(brain_area, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":BrainArea"))
            predicates_list.append(("rdfs:label", brain_area_label))

            for predicates in predicates_list:
                statements = add_to_statements(
                    brain_area_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

     # definitions_of_chills worksheet
    for row in definitions_of_chills.iterrows():
        definition_of_chills = row[1]["DefinitionOfChills"].strip()
        if definition_of_chills not in exclude_list:

            definition_of_chills_label = language_string(definition_of_chills)
            definition_of_chills_iri = check_iri(definition_of_chills, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":DefinitionOfChills"))
            predicates_list.append(("rdfs:label", definition_of_chills_label))

            for predicates in predicates_list:
                statements = add_to_statements(
                    definition_of_chills_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # sensors worksheet
    for row in sensors.iterrows():
        sensor = row[1]["sensor"].strip()
        if sensor not in exclude_list:

            sensor_label = language_string(sensor)
            sensor_iri = check_iri(sensor, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":Sensor"))
            predicates_list.append(("rdfs:label", sensor_label))

            indices_measures = row[1]["measure_index"]
            if indices_measures not in exclude_list:
                if isinstance(indices_measures, float) or \
                        isinstance(indices_measures, int):
                    indices = [np.int(indices_measures)]
                else:
                    indices = [np.int(x) for x in
                               indices_measures.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(claims[claims["index"] == index])
                    objectRDF = measures[measures["index"] ==
                                         index]["measure"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasMeasure",
                                                check_iri(objectRDF, 'PascalCase')))

            indices_related_sensors = row[1]["related_sensor_index"]
            if indices_related_sensors not in exclude_list:
                if isinstance(indices_related_sensors, float) or \
                        isinstance(indices_related_sensors, int):
                    indices = [np.int(indices_related_sensors)]
                else:
                    indices = [np.int(x) for x in
                               indices_related_sensors.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(claims[claims["index"] == index])
                    objectRDF = sensors[sensors["index"] ==
                                         index]["sensor"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasRelatedSensor",
                                                check_iri(objectRDF, 'PascalCase')))

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
        measure = row[1]["measure"].strip()
        if measure not in exclude_list:

            measure_label = language_string(measure)
            measure_iri = check_iri(measure, 'PascalCase')

            predicates_list = []
            predicates_list.append(("a", ":Measure"))
            predicates_list.append(("rdfs:label", measure_label))

            # indices_applications = row[1]["application_index"]
            # if indices_applications not in exclude_list:
            #     if isinstance(indices_applications, float) or \
            #             isinstance(indices_applications, int):
            #         indices = [np.int(indices_applications)]
            #     else:
            #         indices = [np.int(x) for x in
            #                    indices_applications.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         #print(index)
            #         #print(claims[claims["index"] == index])
            #         objectRDF = applications[applications["index"] ==
            #                              index]["application"].values[0]
            #         if isinstance(objectRDF, str):
            #             predicates_list.append((":hasApplication",
            #                                     check_iri(objectRDF, 'PascalCase')))

            # indices_measure_categories = row[1]["MeasureCategory_index"]
            # if indices_measure_categories not in exclude_list:
            #     if isinstance(indices_measure_categories, float) or \
            #             isinstance(indices_measure_categories, int):
            #         indices = [np.int(indices_measure_categories)]
            #     else:
            #         indices = [np.int(x) for x in
            #                    indices_measure_categories.strip().split(',') if len(x)>0]
            #     for index in indices:
            #         #print(index)
            #         #print(claims[claims["index"] == index])
            #         objectRDF = measure_categories[measure_categories["index"] ==
            #                              index]["measureCategory"].values[0]
            #         if isinstance(objectRDF, str):
            #             predicates_list.append((":hasMeasureCategory",
            #                                     check_iri(objectRDF, 'PascalCase')))

            indices_related_measures = row[1]["related_measure_index"]
            if indices_related_measures not in exclude_list:
                if isinstance(indices_related_measures, float) or \
                        isinstance(indices_related_measures, int):
                    indices = [np.int(indices_related_measures)]
                else:
                    indices = [np.int(x) for x in
                               indices_related_measures.strip().split(',') if len(x)>0]
                for index in indices:
                    #print(index)
                    #print(claims[claims["index"] == index])
                    objectRDF = measures[measures["index"] ==
                                         index]["measure"].values[0]
                    if isinstance(objectRDF, str):
                        predicates_list.append((":hasRelatedMeasure",
                                                check_iri(objectRDF, 'PascalCase')))

            for predicates in predicates_list:
                statements = add_to_statements(
                    measure_iri,
                    predicates[0],
                    predicates[1],
                    statements,
                    exclude_list
                )

    # measure_categories worksheet
    # for row in measure_categories.iterrows():
    #     measure_category = row[1]["measureCategory"].strip()
    #     if measure_category not in exclude_list:

    #         measure_category_label = language_string(measure_category)
    #         measure_category_iri = check_iri(measure_category, 'PascalCase')

    #         predicates_list = []
    #         predicates_list.append(("a", ":MeasureCategory"))
    #         predicates_list.append(("rdfs:label", measure_category_label))

    #         for predicates in predicates_list:
    #             statements = add_to_statements(
    #                 measure_category_iri,
    #                 predicates[0],
    #                 predicates[1],
    #                 statements,
    #                 exclude_list
    #             )

    return statements


# indices to other worksheets about content of the shared
# indices_state = row[1]["indices_state"]
# comorbidity_indices_disorder = row[1]["comorbidity_indices_disorder"]
# medication_indices = row[1]["medication_indices"]
# treatment_indices = row[1]["treatment_indices"]
# indices_disorder = row[1]["indices_disorder"]
# indices_disorder_category = row[1]["indices_disorder_category"]
# if indices_state not in exclude_list:
#     indices = [np.int(x) for x in
#                indices_state.strip().split(',') if len(x)>0]
#     for index in indices:
#         objectRDF = states[states["index"] == index]["state"].values[0]
#         if objectRDF not in exclude_list:
#             predicates_list.append((":isAboutDomain",
#                                     check_iri(objectRDF, 'PascalCase')))
# if comorbidity_indices_disorder not in exclude_list:
#     indices = [np.int(x) for x in
#                comorbidity_indices_disorder.strip().split(',') if len(x)>0]
#     for index in indices:
#         objectRDF = disorders[
#             disorders["index"] == index]["disorder"].values[0]
#         if objectRDF not in exclude_list:
#             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))
# if medication_indices not in exclude_list:
#     indices = [np.int(x) for x in
#                medication_indices.strip().split(',') if len(x)>0]
#     for index in indices:
#         objectRDF = medications[
#             medications["index"] == index]["medication"].values[0]
#         if objectRDF not in exclude_list:
#             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))
# if treatment_indices not in exclude_list:
#     indices = [np.int(x) for x in
#                treatment_indices.strip().split(',') if len(x)>0]
#     for index in indices:
#         objectRDF = treatments[treatments["index"] ==
#                                index]["treatment"].values[0]
#         if objectRDF not in exclude_list:
#             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))
# if indices_disorder not in exclude_list:
#     indices = [np.int(x) for x in
#                indices_disorder.strip().split(',') if len(x)>0]
#     for index in indices:
#         objectRDF = disorders[disorders["index"] ==
#                               index]["disorder"].values[0]
#         if objectRDF not in exclude_list:
#             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))
# if indices_disorder_category not in exclude_list:
#     indices = [np.int(x) for x in
#                indices_disorder_category.strip().split(',') if len(x)>0]
#     for index in indices:
#         objectRDF = disorder_categories[disorder_categories["index"] ==
#                          index]["disorder_category"].values[0]
#         if objectRDF not in exclude_list:
#             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))

# # Cognitive Atlas-specific columns
# cogatlas_node_id = row[1]["cogatlas_node_id"]
# cogatlas_prop_id = row[1]["cogatlas_prop_id"]
# if cogatlas_node_id not in exclude_list:
#     predicates_list.append((":hasCognitiveAtlasNodeID",
#                             "cognitiveatlas_node_id_" + check_iri(cogatlas_node_id)))
# if cogatlas_prop_id not in exclude_list:
#     predicates_list.append((":hasCognitiveAtlasPropID",
#                             "cognitiveatlas_prop_id_" + check_iri(cogatlas_prop_id)))


#     # reference_types worksheet
#     for row in reference_types.iterrows():
#
#         reference_type_label = language_string(row[1]["reference_type"])
#
#         if row[1]["IRI"] not in exclude_list:
#             reference_type_iri = check_iri(row[1]["IRI"], 'PascalCase')
#         else:
#             reference_type_iri = check_iri(row[1]["reference_type"], 'PascalCase')
#
#         predicates_list = []
#         predicates_list.append(("rdfs:label",
#                                 language_string(reference_type_label)))
#         predicates_list.append(("rdfs:subClassOf", ":ReferenceType"))
#
#         if row[1]["subClassOf"] not in exclude_list:
#             predicates_list.append(("rdfs:subClassOf",
#                                     check_iri(row[1]["subClassOf"])))
#
#         for predicates in predicates_list:
#             statements = add_to_statements(
#                 reference_type_iri,
#                 predicates[0],
#                 predicates[1],
#                 statements,
#                 exclude_list
#             )
#
#     # shared worksheet
#     for row in shared.iterrows():
#
#         predicates_list = []
#
#         # require title
#         title = row[1]["reference"]
#         if title not in exclude_list:
#
#             # reference IRI
#             reference_iri = check_iri(title)
#             predicates_list.append(("rdfs:label", language_string(title)))
#             predicates_list.append(("dcterms:title", language_string(title)))
#             predicates_list.append(("a", "dcterms:BibliographicResource"))
#
#             # general columns
#             link = row[1]["link"]
#             if link not in exclude_list:
#                 predicates_list.append(("foaf:homepage", check_iri(link)))
#             ingestion_date = row[1]["ingestion_date"]
#             if ingestion_date not in exclude_list:
#                 predicates_list.append((":entryDate", language_string(ingestion_date)))
#
#             # research article-specific columns
#             authors = row[1]["authors"]
#             pubdate = row[1]["pubdate"]
#             PubMedID = row[1]["PubMedID"]
#             if authors not in exclude_list:
#                 predicates_list.append(("bibo:authorList", language_string(authors)))
#             if pubdate not in exclude_list:
#                 predicates_list.append(("npg:publicationDate", language_string(pubdate)))
#                 # npg:publicationYear
#if PubMedID not in exclude_list:
#    predicates_list.append((":hasPubMedID",
#                            '"{0}"^^xsd:nonNegativeInteger'.format(int(PubMedID))))
#
#             # indices to other worksheets about who uses the shared
#             indices_reference_type = row[1]["indices_reference_type"]
#             if indices_reference_type not in exclude_list:
#                 if isinstance(indices_reference_type, str):
#                     indices = [np.int(x) for x in
#                                indices_reference_type.strip().split(',') if len(x)>0]
#                 elif isinstance(indices_reference_type, float):
#                     indices = [np.int(indices_reference_type)]
#                 else:
#                     indices = None
#                 if indices not in exclude_list:
#                     for index in indices:
#                         objectRDF = reference_types[
#                             reference_types["index"] == index]["reference_type"].values[0]
#                         if objectRDF not in exclude_list:
#                             predicates_list.append((":hasReferenceType",
#                                                     check_iri(objectRDF, 'PascalCase')))
#             # if indices_disorder not in exclude_list:
#             #     indices = [np.int(x) for x in
#             #                indices_disorder.strip().split(',') if len(x)>0]
#             #     for index in indices:
#             #         objectRDF = disorders[disorders["index"] ==
#             #                               index]["disorder"].values[0]
#             #         if objectRDF not in exclude_list:
#             #             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))
#             # if indices_disorder_category not in exclude_list:
#             #     indices = [np.int(x) for x in
#             #                indices_disorder_category.strip().split(',') if len(x)>0]
#             #     for index in indices:
#             #         objectRDF = disorder_categories[disorder_categories["index"] ==
#             #                          index]["disorder_category"].values[0]
#             #         if objectRDF not in exclude_list:
#             #             predicates_list.append(("schema:about", check_iri(objectRDF, 'PascalCase')))
#
#             # # Cognitive Atlas-specific columns
#             # cogatlas_node_id = row[1]["cogatlas_node_id"]
#             # cogatlas_prop_id = row[1]["cogatlas_prop_id"]
#             # if cogatlas_node_id not in exclude_list:
#             #     predicates_list.append((":hasCognitiveAtlasNodeID",
#             #                             "cognitiveatlas_node_id_" + check_iri(cogatlas_node_id)))
#             # if cogatlas_prop_id not in exclude_list:
#             #     predicates_list.append((":hasCognitiveAtlasPropID",
#             #                             "cognitiveatlas_prop_id_" + check_iri(cogatlas_prop_id)))
#
#             for predicates in predicates_list:
#                 statements = add_to_statements(
#                     reference_iri,
#                     predicates[0],
#                     predicates[1],
#                     statements,
#                     exclude_list
#                 )
#
#
#     # respondents_or_subjects worksheet
#     for row in respondents_or_subjects.iterrows():
#         if row[1]["IRI"] not in exclude_list:
#             respondent_or_subject_IRI = check_iri(row[1]["IRI"], 'PascalCase')
#         else:
#             respondent_or_subject_IRI = check_iri(row[1]["respondent_or_subject"], 'PascalCase')
#         statements = add_to_statements(respondent_or_subject_IRI, "a",
#                                        "foaf:Person",
#                                        statements, exclude_list)
#         statements = add_to_statements(respondent_or_subject_IRI, "rdfs:label",
#                             language_string(row[1]["respondent_or_subject"]),
#                             statements, exclude_list)
#
#     # genders worksheet
#     # for row in genders.iterrows():
#     #     if row[1]["IRI"] not in exclude_list:
#     #         gender_iri = check_iri(row[1]["IRI"], 'PascalCase')
#     #     else:
#     #         gender_iri = check_iri(row[1]["gender"], 'PascalCase')
#     #     statements = add_to_statements(gender_iri, "rdfs:label",
#     #                         language_string(row[1]["gender"]),
#     #                         statements, exclude_list)
#
#     # medications worksheet
#     for row in medications.iterrows():
#         if row[1]["IRI"] not in exclude_list:
#             medication_iri = check_iri(row[1]["IRI"], 'PascalCase')
#         else:
#             medication_iri = check_iri(row[1]["medication"])
#         statements = add_to_statements(medication_iri, "a",
#                             ":Medication", statements, exclude_list)
#         statements = add_to_statements(medication_iri, "rdfs:label",
#                             language_string(row[1]["medication"], 'PascalCase'),
#                                        statements, exclude_list)
#
#     # treatments worksheet
#     for row in treatments.iterrows():
#         if row[1]["IRI"] not in exclude_list:
#             treatment_iri = check_iri(row[1]["IRI"], 'PascalCase')
#         else:
#             treatment_iri = check_iri(row[1]["treatment"], 'PascalCase')
#         statements = add_to_statements(treatment_iri, "a",
#                             ":Treatment", statements, exclude_list)
#         statements = add_to_statements(treatment_iri, "rdfs:label",
#                             language_string(row[1]["treatment"]),
#                                        statements, exclude_list)
#
#     return statements
