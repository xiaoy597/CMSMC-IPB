/*
版本信息:1.0
创建者	:XYU
功能描述:期货投资者数_按权益分布分类
加载类型:全量加载
源		 表：
	NSPUBMART.KPI_FINVST_UOPACCT_CDE_VOL_IDX -- 期货投资者统一开户编码数量指标表
目	标	表:	nsPubMart.CMSC_FUTRS_INVST_CNT_EQT	-- 期货投资者数_按权益分布分类
频		 度:M:月
创建日期:	2018-01-24
修改历史:
修改人	修改日期	修改内容
*/

DELETE FROM ${NSPUBMART}.CMSC_FUTRS_INVST_CNT_EQT
WHERE TJRQ = SUBSTR(CAST(CAST(ADD_MONTHS(CAST('${TXDATE}' AS DATE FORMAT 'YYYYMMDD'),-1) AS DATE FORMAT 'YYYYMMDD') AS CHAR(8)),1,6)
;

.IF ERRORCODE <> 0 THEN .QUIT 12;

INSERT INTO ${NSPUBMART}.CMSC_FUTRS_INVST_CNT_EQT
with temp as (
SELECT MAX(CALENDAR_DATE)  AS LAST_MONTH_TRAD_DATE
				FROM ${NSPVIEW}.PTY_TRAD_CLND
				WHERE EXTRACT(MONTH FROM CALENDAR_DATE) = EXTRACT(MONTH FROM ADD_MONTHS(CAST('${TXDATE}' AS DATE FORMAT 'YYYYMMDD'),-1))
				AND EXTRACT(YEAR FROM CALENDAR_DATE) = EXTRACT(YEAR FROM ADD_MONTHS(CAST('${TXDATE}' AS DATE FORMAT 'YYYYMMDD'),-1))
				AND IF_TRADDAY = 1
)
SEL
SUBSTR(CAST(CAST(ADD_MONTHS(CAST('${TXDATE}' AS DATE FORMAT 'YYYYMMDD'),-1) AS DATE FORMAT 'YYYYMMDD') AS CHAR(8)),1,6) AS TJRQ, 
	CASE WHEN t.FUND_EQUT_TOT_AMT < 100000 THEN '1000'
		WHEN t.FUND_EQUT_TOT_AMT >= 100000 AND FUND_EQUT_TOT_AMT < 500000  THEN '2000'
		WHEN t.FUND_EQUT_TOT_AMT >= 500000 AND FUND_EQUT_TOT_AMT < 3000000  THEN '3000'
		WHEN t.FUND_EQUT_TOT_AMT >= 3000000 AND FUND_EQUT_TOT_AMT < 10000000  THEN '4000'
		WHEN t.FUND_EQUT_TOT_AMT >= 10000000 THEN '5000'
		ELSE '1000'
	END AS TZZFL
	,COUNT(DISTINCT t.FUTRS_UNFY_OPN_ACCT_CUST_CDE) AS QHTZZSL
	,COUNT(DISTINCT t.FUTRS_UNFY_OPN_ACCT_CUST_CDE)/CAST(QHTZZZS AS DECIMAL(30,4)) AS QHTZZSLZB	
FROM 
	(SEL a.FUTRS_UNFY_OPN_ACCT_CUST_CDE
			,SUM(b.FUND_EQUT_TOT_AMT) AS FUND_EQUT_TOT_AMT
	FROM nspview.ACT_FUTRS_INVST_CLSF_HIS	a
	LEFT JOIN nsoview.CFMMC_FC_CUST_BSC_FD b
		ON b.DATA_SUBMTD_COMP_ID = a.FC_MEMB_NBR
		AND b.CUST_INSD_FUND_ACCT = a.INSD_BANKRL_ACCT
		AND b.DATA_DATE = (
		SELECT LAST_MONTH_TRAD_DATE FROM temp
		)
	WHERE a.s_date <= (
		SELECT LAST_MONTH_TRAD_DATE FROM temp
		)
		AND a.e_date > (
		SELECT LAST_MONTH_TRAD_DATE FROM temp
		)
		AND a.ACCT_STS = 'A'
	GROUP BY 1) t,
	(SEL COUNT(DISTINCT FUTRS_UNFY_OPN_ACCT_CUST_CDE) QHTZZZS
	FROM nspview.ACT_FUTRS_INVST_CLSF_HIS	a
	LEFT JOIN nsoview.CFMMC_FC_CUST_BSC_FD b
		ON b.DATA_SUBMTD_COMP_ID = a.FC_MEMB_NBR
		AND b.CUST_INSD_FUND_ACCT = a.INSD_BANKRL_ACCT
		AND b.DATA_DATE = (
		SELECT LAST_MONTH_TRAD_DATE FROM temp
		)
		WHERE a.s_date <= (
		SELECT LAST_MONTH_TRAD_DATE FROM temp
		)
		AND a.e_date > (
		SELECT LAST_MONTH_TRAD_DATE FROM temp
		)
		AND a.ACCT_STS = 'A') t1
WHERE TZZFL IS NOT NULL
GROUP BY 1,2;




.IF ERRORCODE <> 0 THEN .QUIT 12;

.QUIT



