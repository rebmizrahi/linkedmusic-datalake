CREATE TABLE flattened_data AS
SELECT
    a.name AS archive_name,
    cmw.role,
    cmw.certainty_of_attribution,
    cmw.date_range_year_only,
    cmw.contributed_to_work_id,
    cmw.location_id,
    cmw.person_id,
    ew.encoder_names,
    ew.workflow_text,
    ew.workflow_file,
    ew.notes,
    ew.encoding_software_id,
    es.title AS experimental_study_title,
    es.link AS experimental_study_link,
    es.authors AS experimental_study_authors,
    es.research_corpus_used_id,
    ef.value AS extracted_feature_value,
    ef.extracted_with_id AS extracted_feature_extracted_with_id,
    ef.feature_of_id AS extracted_feature_feature_of_id,
    ef.instance_of_feature_id AS extracted_feature_instance_of_feature_id,
    f.name AS feature_name,
    f.code AS feature_code,
    f.description AS feature_description,
    f.is_sequential AS feature_is_sequential,
    f.dimensions AS feature_dimensions,
    f.min_val AS feature_min_val,
    f.max_val AS feature_max_val,
    f.software_id AS feature_software_id,
    ff.file_format AS feature_file_format,
    ff.file AS feature_file_file,
    ff.feature_definition_file AS feature_file_feature_definition_file,
    ff.extracted_with_id AS feature_file_extracted_with_id,
    ff.features_from_file_id AS feature_file_features_from_file_id,
    fg_style.name AS genre_as_in_style_name,
    fg_type.name AS genre_as_in_type_name,
    ga.name AS geographic_area_name,
    i.name AS instrument_name,
    l.name AS language_name,
    mw.variant_titles AS musical_work_variant_titles,
    mw.sacred_or_secular AS musical_work_sacred_or_secular,
    mw.authority_control_url AS musical_work_authority_control_url,
    mw.search_document AS musical_work_search_document,
    mwrw.from_musicalwork_id AS musical_work_related_works_from_musicalwork_id,
    mwrw.to_musicalwork_id AS musical_work_related_works_to_musicalwork_id,
    p.given_name AS person_given_name,
    p.surname AS person_surname,
    p.birth_date_range_year_only AS person_birth_date_range_year_only,
    p.death_date_range_year_only AS person_death_date_range_year_only,
    p.authority_control_url AS person_authority_control_url,
    rc.title AS research_corpus_title,
    rc.doi_links AS research_corpus_doi_links,
    rcf.researchcorpus_id AS research_corpus_files_researchcorpus_id,
    rcf.file_id AS research_corpus_files_file_id,
    s.title AS section_title,
    s.ordering AS section_ordering,
    s.musical_work_id AS section_musical_work_id,
    s.parent_section_id AS section_parent_section_id,
    s.type_of_section_id AS section_type_of_section_id,
    srs.from_section_id AS section_related_sections_from_section_id,
    srs.to_section_id AS section_related_sections_to_section_id,
    sw.name AS software_name,
    sw.version AS software_version,
    v.validator_names AS validation_workflow_validator_names,
    v.notes AS validation_workflow_notes,
    v.workflow_text AS validation_workflow_workflow_text,
    v.workflow_file AS validation_workflow_workflow_file,
    v.validator_software_id AS validation_workflow_validator_software_id

-- need to make sure these id joins are correct and test how full outer vs left join acts    
FROM archive a
LEFT JOIN contribution_musical_work cmw ON a.id = cmw.archive_id
LEFT JOIN encoding_workflow ew ON a.id = ew.archive_id
LEFT JOIN experimental_study es ON a.id = es.archive_id
LEFT JOIN extracted_feature ef ON a.id = ef.archive_id
LEFT JOIN feature f ON a.id = f.archive_id
LEFT JOIN feature_file ff ON a.id = ff.archive_id
LEFT JOIN genre_as_in_style fg_style ON a.id = fg_style.archive_id
LEFT JOIN genre_as_in_type fg_type ON a.id = fg_type.archive_id
LEFT JOIN geographic_area ga ON a.id = ga.archive_id
LEFT JOIN instrument i ON a.id = i.archive_id
LEFT JOIN language l ON a.id = l.archive_id
LEFT JOIN musical_work mw ON a.id = mw.archive_id
LEFT JOIN musical_work_related_works mwrw ON a.id = mwrw.archive_id
LEFT JOIN person p ON a.id = p.archive_id
LEFT JOIN research_corpus rc ON a.id = rc.archive_id
LEFT JOIN research_corpus_files rcf ON a.id = rcf.archive_id
LEFT JOIN section s ON a.id = s.archive_id
LEFT JOIN section_related_sections srs ON a.id = srs.archive_id
LEFT JOIN software sw ON a.id = sw.archive_id
LEFT JOIN validation_workflow v ON a.id = v.archive_id;

-- Save the combined data to a CSV file
COPY flattened_data TO '/flattened_data.csv' CSV HEADER;

