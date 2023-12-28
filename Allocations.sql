SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
DECLARE @Startdateid INT = 20230401
DECLARE @EndDateID INT = CONVERT(VARCHAR(10), DATEADD(DAY, -1, GETDATE()), 112) 
DECLARE @60DayBakeDateID INT = CONVERT(VARCHAR(10), DATEADD(DAY, -60, GETDATE()), 112) 
--------------------------------------------------------------------------------------------------------------------------
--Milestone Base
DROP TABLE IF EXISTS #Milestones
SELECT 
 LID.LoanNumber
,lid.LoanIdentifierDimSK
,LMF.EventDateTime
,LMD.GroupName
,cb.commonid
,LMF.EventDateDimId
,lmd.groupid
,lmf.LoanGroupRank

INTO #Milestones
FROM EDW.Loan.LoanMajorMilestoneFact LMF WITH (NOLOCK) 
INNER JOIN EDW.Dimensions.LoanIdentifierDim LID WITH (NOLOCK) ON  LID.LoanIdentifierDimSK = LMF.LoanIdentifierDimSK AND LID.IsCurrentRecordInd = 1    
INNER JOIN EDW.Dimensions.LoanMilestoneDim LMD WITH (NOLOCK) ON  LMD.LoanMilestoneSK = LMF.LoanMilestoneSK
INNER JOIN qlods.dbo.Capstone_BankerDim cb ON cb.CapstoneBankerID = lmf.CapstoneBankerID
--INNER JOIN edw.Dimensions.TeamMemberIdentityDim tid ON tid.EmployeeID = lmf.BankerID AND tid.IsCurrentRecordInd = 1

WHERE 1=1
AND LID.LoanNumber IS NOT NULL
AND LMF.LoanGroupRank = 1 --first occurrence of milestone per l#   
AND LMD.GroupID IN ('7')--allocations
AND LMF.EventDateDimId BETWEEN @Startdateid AND @Enddateid
CREATE CLUSTERED INDEX ix_Milestones ON #Milestones (LoanNumber)
--------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #base
SELECT
 m.*
,edd.MonthSK
,FirstAllocationFlg = 1
,CASE WHEN credit.EventDateTime > m.EventDateTime THEN 1 ELSE 0 END AS 'FirstAllocateToCreditFlg'
,CASE WHEN lk.ClosingDt > m.EventDateTime THEN 1 ELSE 0 END AS 'FirstAllocateToCloseFlg'
,DATEDIFF(second,m.eventdatetime,lk.closingdt)/ 86400.0 as 'FirstAllocateToCloseDays'
,CASE WHEN m.EventDateDimId < @60DayBakeDateID THEN 1 ELSE 0 END AS '60DayBakedAllocationFlg'
,CASE WHEN wcd.WebCode IN ('GENSOCPRFR','RBSGENSOCPRFP','GENSOCPRFP','REFERSOCPRFR','RBSREFERSOCPRFP','REFERSOCPRFP'
,'FLOREFERSOCPRFP','FLOREFERSOCPRFR','BNKREFERSOCPRFR','BNKREFERSOCPRFP') THEN 1 ELSE 0 END AS 'SocialProofReferralFlg'
,CASE WHEN wcd.WebCode IN ('GENSOCPRFR','RBSGENSOCPRFP','GENSOCPRFP','REFERSOCPRFR','RBSREFERSOCPRFP','REFERSOCPRFP'
,'FLOREFERSOCPRFP','FLOREFERSOCPRFR','BNKREFERSOCPRFR','BNKREFERSOCPRFP') THEN 'Social Proof Referral' ELSE 'Non-Social Proof Referral' END AS 'AllocationType'
,CASE WHEN wcd.WebCode IN ('RBSGENSOCPRFP','RBSREFERSOCPRFP') THEN 1 ELSE 0 END AS 'SocialProofReferralREAFlg'

INTO #base
FROM #Milestones m
INNER JOIN EDW.Dimensions.LoanIdentifierDim LID WITH (NOLOCK) ON  LID.LoanIdentifierDimSK = m.LoanIdentifierDimSK AND LID.IsCurrentRecordInd = 1
--LEFT JOIN EDW.Marketing.vwRocket_Traditional_Winner rtw ON rtw.LoanIdentifierDimSK = LID.LoanIdentifierDimSK AND LID.IsCurrentRecordInd  = 1
INNER JOIN qlods.dbo.lola lola ON lola.JacketNumber = lid.LoanNumber AND lid.IsCurrentRecordInd = 1
LEFT JOIN qlods.dbo.WebCodeDim wcd ON wcd.WebCodeid = lola.webreferrerid AND wcd.leadtypeflg = 1
LEFT JOIN EDW.LoanOrigination.vwLoanBusinessChannelClassification BCC ON BCC.LoanIdentifierDimSK = LID.LoanIdentifierDimSK AND LID.IsCurrentRecordInd = 1
LEFT JOIN qlods.dbo.lkwd lk ON lk.loannumber = lid.loannumber AND LID.IsCurrentRecordInd = 1 AND LID.IsCurrentRecordInd = 1
INNER JOIN edw.Dimensions.DateDim edd ON edd.DateSK = m.EventDateDimId
LEFT JOIN 
	(SELECT lmf.EventDateTime, lmf.LoanIdentifierDimSK, RECORD = Row_Number() OVER(PARTITION BY LMF.LoanIdentifierDimSK ORDER BY LMF.EventDateTime ASC)
	FROM EDW.Loan.LoanMajorMilestoneFact LMF WITH (NOLOCK)
	INNER JOIN EDW.Dimensions.LoanMilestoneDim LMD WITH (NOLOCK) ON  LMD.LoanMilestoneSK = LMF.LoanMilestoneSK
	WHERE 1=1
	AND LMD.GroupID IN ('112','1') --soft & hard
	AND LMF.LoanGroupRank = 1 --first occurrence of milestone per l#;need partition for true first
	) as Credit
		ON credit.LoanIdentifierDimSK = lid.LoanIdentifierDimSK AND credit.RECORD = 1 

WHERE 1=1
AND (lk.IsQLMSFlg = 0 OR lk.IsQLMSFlg IS NULL)
AND bcc.BusinessChannelName = 'Core Purchase'
AND lola.LoanPurposeID <> '7' --refi
AND wcd.webcode NOT IN (
'RLREMKTP' -- Digital No Movement 
,'RLHAP'-- Digital Pitstops
,'WHALFCHAT'
,'WHALFRKTCHAT' -- Ironbar
-- LIV/OH Chat:
,'whalfrktchat', 'whalfchat', 'ohchat', 'chatql', 'chatblf', 'chat', 'rocktestrefiah', 'rocktestrefiac', 'rocktestrefi', 'rocktestpurchah', 'rocktestpurchac', 'rocktestpurch', 'rockcarirefi', 'rockcarirah', 'rockcaripurch', 'rockcaripah', 'rhchatah', 'rhchat', 'chatblpah', 'chatblp', 'chatblfah', 'carlifpurrh', 'carilivrefipo', 'carilivrefieqpurah', 'carilivrefiah', 'carilivrefi', 'carilivpurchpoah', 'carilivpurchpo', 'carilivpurchah', 'carilivpurch', 'carilivpuporhah', 'carilivpuporh', 'carilivpoah', 'cariliveqrefiah', 'cariliveqrefi', 'cariliveqpurhah', 'cariliveqpurh', 'cariliveqpurch', 'cariliveqpurah', 'carilivpurrh', 'carilivpurrhah', 'carilivpurrhahr', 'carilivpurrhr', 'carilivrefipoah', 'carilivriskrefi', 'carilivrskrefah', 'chatsurv', 'chatos', 'ohchatcari', 'ohrmchat', 'ohrmchatcari', 'qlchatoh', 'qlchatohnb', 'rhcari', 'rhcariah', 'ohchatman'
-- Spanish:
,'ACCARISPAN','ACCARISPANP','ACNAHREP','ACSPANISH','ACSPANP','ACSPANPURCH','ACSPANR','ARSPWEB','BGSPANISHESC','CARISPANSV','CARISPANSVNC'                             
,'CRMCLSPN','CRWCARISPANP','CRWCARISPANR','DCRFSP','FBQLESP','FBQLHARPSP','FBQLSSP','FBQLVASP','FHARESERVSPAN','FHARESERVSPANP','FRQLSPA','HGTVSP21','HGTVSP22','IHRARFSSDT'                                
,'IHRARFSSDT750','IHRASSBK','IHRASSBK750','LBQLESPANOL','LBQLSP','LBQLSPERN','LPNAHREP','LTQLSPAN','OJOLABSSPAN','OJOLABSSPANTF','ORMSPANISHTFR','PROGPIESPV1','PROGPIESPV2'                                
,'RMSPANISHLC','SBSPANDNC22','SBSPANP21','SBSPANP22','SBSPANPDNC22','SBSPANR21','SBSPANR22','SBSPANRDNC22','SPANISHMKTCAC','SPANISHMKTCWEB','SPANLBR','SPANWEBLEAD','SPNWDPURCH'                                
,'SPNWDREFI','WHAMESPV1','WHAMESPV2','ACSPANMKTMAILR','ACSPANMKTMAILP','WEBSPANMKTMAILR','WEBSPANMKTMAILP'
 ) 
--------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #final
SELECT
 b.MonthSK
,b.[60DayBakedAllocationFlg]
,b.SocialProofReferralREAFlg
,b.[AllocationType]
,SUM(b.FirstAllocationFlg) as 'FirstAllocationFlg'
,SUM(b.FirstAllocateToCloseFlg) as 'FirstAllocateToCloseFlg'
,SUM(FirstAllocateToCreditFlg) as 'FirstAllocateToCreditFlg'

INTO #final
FROM #base b

WHERE 1=1
GROUP BY b.MonthSK, b.[AllocationType], b.[60DayBakedAllocationFlg], b.SocialProofReferralREAFlg
--------------------------------------------------------------------------------------------------------------------------
SELECT
 f.*
,CAST(f.firstallocatetocloseflg AS FLOAT) / CAST(f.firstallocationflg AS FLOAT) as 'FirstAllocateToClose'
,GETDATE() as 'RefreshDt'

FROM #final f