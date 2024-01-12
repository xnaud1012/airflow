DECLARE
    table_exists INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO table_exists
    FROM user_tables
    WHERE table_name = 'NOTE';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLE NOTE (
    encoding_concept_id    INT,
    language_concept_id    INT,
    note_class_concept_id  INT,
    note_date              DATE,
    note_datetime          TIMESTAMP, -- Oracle에서는 TIMESTAMP 사용
    note_id                INT,
    note_source_value      VARCHAR2(50),
    note_text              CLOB, -- 매우 긴 텍스트를 위해 CLOB 사용
    note_title             VARCHAR2(250),
    note_type_concept_id   INT,
    person_id              INT,
    provider_id            INT,
    visit_detail_id        INT,
    visit_occurrence_id    INT
);';
    END IF;
END;
