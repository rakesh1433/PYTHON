import os

DRIVER='ODBC Driver 17 for SQL Server'
SERVER='beastie.tsl.telus.com'
DATABASE='PAPM'
UID='BeastReports'
PWD=f'{os.environ["BEAST_REPORTS_KEY"]}'

queryEmailreportmonthly = """
SELECT [PROV]
,[ACNA]
,[RCID]
,[ACCOUNT]
,[RAC]
,[BILL_REV]
,[CCYYMM]
,[BILL_SYST]
,[SAP_PROD_CD]
,[SEG_A]
,[TS_LAST_RUN]
FROM [PAPM].[mkt].[VW_NCAMSUMMARY] where ACCOUNT like '%BELL%'
AND CCYYMM = FORMAT(DATEADD(MONTH, -1, GETDATE()), 'yyyyMM')
"""
