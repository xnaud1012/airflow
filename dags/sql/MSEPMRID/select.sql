SELECT   TO_SINGLE_BYTE(HSP_TP_CD) HSP_TP_CD
, TO_SINGLE_BYTE(PTHL_NO) PTHL_NO
, RSLT_SEQ
, DBMS_LOB.substr @esmartp1 (PLRT_LDAT, 2000 , 1) PLRT_LDAT
, TO_SINGLE_BYTE(ADD_RSLT_YN) ADD_RSLT_YN
, TO_SINGLE_BYTE(ADD_RSLT_KND_CD) ADD_RSLT_KND_CD
, TO_SINGLE_BYTE(ADD_RSLT_KND_CNTE) ADD_RSLT_KND_CNTE
, TO_SINGLE_BYTE(SGKY_NO) SGKY_NO
, TO_SINGLE_BYTE(TH1_IPDR_STF_NO) TH1_IPDR_STF_NO
, TO_SINGLE_BYTE(TH2_IPDR_STF_NO) TH2_IPDR_STF_NO
, FSR_DTM
, TO_SINGLE_BYTE(FSR_STF_NO) FSR_STF_NO
, TO_SINGLE_BYTE(FSR_PRGM_NM) FSR_PRGM_NM
, TO_SINGLE_BYTE(FSR_IP_ADDR) FSR_IP_ADDR
, LSH_DTM
, TO_SINGLE_BYTE(LSH_STF_NO) LSH_STF_NO
, TO_SINGLE_BYTE(LSH_PRGM_NM) LSH_PRGM_NM
, TO_SINGLE_BYTE(LSH_IP_ADDR) LSH_IP_ADDR
, TO_SINGLE_BYTE(ADD_DGNS_RSN) ADD_DGNS_RSN
, TO_SINGLE_BYTE(PTHL_OGN_CNTE) PTHL_OGN_CNTE
, TO_SINGLE_BYTE(MLT_DGNS_CNTE) MLT_DGNS_CNTE
, TO_SINGLE_BYTE(PTHL_DR_DGNS_CNTE) PTHL_DR_DGNS_CNTE
, TO_SINGLE_BYTE(IPTN_MLT_STF_NO) IPTN_MLT_STF_NO
, TO_SINGLE_BYTE(ADD_IPTN_RSDT_NM) ADD_IPTN_RSDT_NM
, TO_SINGLE_BYTE(ADD_IPTN_SPCT_NM) ADD_IPTN_SPCT_NM
, TO_SINGLE_BYTE(IPPR_ID) IPPR_ID
  FROM xnaud.MSEPMRID  
  