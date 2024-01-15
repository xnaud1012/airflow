DECLARE
    table_exists INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO table_exists
    FROM user_tables
    WHERE table_name = 'MSEPMRID';

    IF table_exists = 0 THEN
    EXECUTE IMMEDIATE '
    CREATE TABLE xnaud.MSEPMRID (
    HSP_TP_CD VARCHAR2(100),           
    PTHL_NO VARCHAR2(100),             
    RSLT_SEQ NUMBER,                   
    PLRT_LDAT CLOB,                    
    ADD_RSLT_YN VARCHAR2(10),          
    ADD_RSLT_KND_CD VARCHAR2(100),     
    ADD_RSLT_KND_CNTE VARCHAR2(4000),  
    SGKY_NO VARCHAR2(100),             
    TH1_IPDR_STF_NO VARCHAR2(100),     
    TH2_IPDR_STF_NO VARCHAR2(100),     
    FSR_DTM DATE,                      
    FSR_STF_NO VARCHAR2(100),          
    FSR_PRGM_NM VARCHAR2(200),         
    FSR_IP_ADDR VARCHAR2(100),         
    LSH_DTM DATE,                      
    LSH_STF_NO VARCHAR2(100),          
    LSH_PRGM_NM VARCHAR2(200),         
    LSH_IP_ADDR VARCHAR2(100),         
    ADD_DGNS_RSN VARCHAR2(4000),       
    PTHL_OGN_CNTE VARCHAR2(4000),      
    MLT_DGNS_CNTE VARCHAR2(4000),      
    PTHL_DR_DGNS_CNTE VARCHAR2(4000), 
    IPTN_MLT_STF_NO VARCHAR2(100),
    ADD_IPTN_RSDT_NM VARCHAR2(200), 
    ADD_IPTN_SPCT_NM VARCHAR2(200),
    IPPR_ID VARCHAR2(100))';
    
    EXECUTE IMMEDIATE '
    INSERT INTO xnaud.MSEPMRID (
    HSP_TP_CD, PTHL_NO, RSLT_SEQ, PLRT_LDAT, ADD_RSLT_YN, 
    ADD_RSLT_KND_CD, ADD_RSLT_KND_CNTE, SGKY_NO, TH1_IPDR_STF_NO, 
    TH2_IPDR_STF_NO, FSR_DTM, FSR_STF_NO, FSR_PRGM_NM, FSR_IP_ADDR, 
    LSH_DTM, LSH_STF_NO, LSH_PRGM_NM, LSH_IP_ADDR, ADD_DGNS_RSN, 
    PTHL_OGN_CNTE, MLT_DGNS_CNTE, PTHL_DR_DGNS_CNTE, IPTN_MLT_STF_NO, 
    ADD_IPTN_RSDT_NM, ADD_IPTN_SPCT_NM, IPPR_ID)
     VALUES (
    ''Type2'', ''P2002'', 2, ''Large Text Data 2'', ''N'',
    ''CD200'', ''Kind Content 2'', ''S2002'', ''T3002'',
    ''T4002'', SYSDATE, ''ST2002'', ''Program3'', ''192.168.2.1'',
    SYSDATE, ''SL2002'', ''Program4'', ''192.168.2.2'', ''Diagnosis Reason 2'',
    ''Origin Content 2'', ''Multiple Diagnosis 2'', ''Doctor Diagnosis 2'', ''MTS2002'',
    ''Resident Name 2'', ''Specialist Name 2'', ''ID2002''
    )';
    END IF;

    
END;
