DECLARE
    -- 테이블 존재 여부를 확인하는 변수
    table_exists INTEGER;

BEGIN
    -- 테이블이 존재하는지 확인
    SELECT COUNT(0)
    INTO table_exists
    FROM user_tables
    WHERE table_name = 'MEASUREMENT'; -- 대문자로 테이블 이름을 적어야 합니다.

    -- 테이블이 존재하지 않으면 생성
    IF table_exists = 0 THEN

           CREATE TABLE MEASUREMENT (measurement_id NUMBER,person_id NUMBER,measurement_concept_id NUMBER,measurement_date DATE,measurement_datetime TIMESTAMP,
                                      measurement_time VARCHAR2(10),measurement_type_concept_id NUMBER,operator_concept_id NUMBER,value_as_number FLOAT,
                                      value_as_concept_id NUMBER,unit_concept_id NUMBER,range_low FLOAT,range_high FLOAT,provider_id NUMBER,visit_occurrence_id NUMBER,
                                      visit_detail_id NUMBER, measurement_source_value VARCHAR2(50),measurement_source_concept_id NUMBER,unit_source_value VARCHAR2(50),
                                      value_source_value VARCHAR2(50));
    END IF;

    -- 데이터 삽입

     INSERT INTO MEASUREMENT (
        measurement_id, person_id, measurement_concept_id, measurement_date,
        measurement_datetime, measurement_time, measurement_type_concept_id,
        operator_concept_id, value_as_number, value_as_concept_id,
        unit_concept_id, range_low, range_high, provider_id,
        visit_occurrence_id, visit_detail_id, measurement_source_value,
        measurement_source_concept_id, unit_source_value, value_source_value
    ) VALUES (
        :measurement_id, :person_id, :measurement_concept_id, :measurement_date,
        :measurement_datetime, :measurement_time, :measurement_type_concept_id,
        :operator_concept_id, :value_as_number, :value_as_concept_id,
        :unit_concept_id, :range_low, :range_high, :provider_id,
        :visit_occurrence_id, :visit_detail_id, :measurement_source_value,
        :measurement_source_concept_id, :unit_source_value, :value_source_value
    )
    

    -- 커밋(필요한 경우)
    COMMIT;
END;