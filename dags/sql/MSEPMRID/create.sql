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
    IPPR_ID VARCHAR2(100));';
    END IF;
END;
