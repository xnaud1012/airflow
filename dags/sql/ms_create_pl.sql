DECLARE
    table_exists INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO table_exists
    FROM user_tables
    WHERE table_name = 'MEASUREMENT';

    IF table_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLE MEASUREMENT (
            measurement_id NUMBER,
            person_id NUMBER,
            measurement_concept_id NUMBER,
            measurement_date DATE,
            measurement_datetime TIMESTAMP,
            measurement_time VARCHAR2(10),
            measurement_type_concept_id NUMBER,
            operator_concept_id NUMBER,
            value_as_number FLOAT,
            value_as_concept_id NUMBER,
            unit_concept_id NUMBER,
            range_low FLOAT,
            range_high FLOAT,
            provider_id NUMBER,
            visit_occurrence_id NUMBER,
            visit_detail_id NUMBER,
            measurement_source_value VARCHAR2(50),
            measurement_source_concept_id NUMBER,
            unit_source_value VARCHAR2(50),
            value_source_value VARCHAR2(50)
        )';
    END IF;
END;
