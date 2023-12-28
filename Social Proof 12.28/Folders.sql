SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
DECLARE @Startdateid INT = 20230401
DECLARE @EndDateID INT = CONVERT(VARCHAR(10), DATEADD(DAY, -35, GETDATE()), 112)  
--------------------------------------------------------------------------------------------------------------------------
--Milestones & Banker at Milestone Base--First Occurrence Only
DROP TABLE IF EXISTS #Milestones
SELECT 
 LID.LoanNumber
,lid.LoanIdentifierDimSK
,LMF.EventDateTime
,lmf.EventDateDimId
,LMD.GroupName
,tid.commonid 

INTO #Milestones
FROM EDW.Loan.LoanMajorMilestoneFact LMF WITH (NOLOCK) 
INNER JOIN EDW.Dimensions.LoanIdentifierDim LID WITH (NOLOCK) ON  LID.LoanIdentifierDimSK = LMF.LoanIdentifierDimSK AND LID.IsCurrentRecordInd = 1    
INNER JOIN EDW.Dimensions.LoanMilestoneDim LMD WITH (NOLOCK) ON  LMD.LoanMilestoneSK = LMF.LoanMilestoneSK
--INNER JOIN qlods.dbo.Capstone_BankerDim cb ON cb.CapstoneBankerID = lmf.CapstoneBankerID
INNER JOIN edw.Dimensions.TeamMemberIdentityDim tid ON tid.EmployeeID = lmf.BankerID AND tid.IsCurrentRecordInd = 1

WHERE 1=1
AND LID.LoanNumber IS NOT NULL
AND LMF.LoanGroupRank = 1 --first occurrence of milestone per l#   
AND LMD.GroupID IN ('3')--folder
AND LMF.EventDateDimId BETWEEN @Startdateid AND @Enddateid
CREATE CLUSTERED INDEX ix_Milestones ON #Milestones (LoanNumber)
--------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #base
SELECT
 m.*
,edd.MonthSK
,FolderFlg = 1
,CASE WHEN lk.ClosingDt > m.EventDateTime THEN 1 ELSE 0 END AS 'FolderToCloseFlg'
,CASE WHEN lk.ClosingDt > m.EventDateTime THEN DATEDIFF(SECOND,m.EventDateTime,lk.closingdt)/ 86400.0 ELSE NULL END AS 'FolderToCloseDays'
,CASE WHEN wcd.WebCode IN ('GENSOCPRFR','RBSGENSOCPRFP','GENSOCPRFP','REFERSOCPRFR','RBSREFERSOCPRFP','REFERSOCPRFP'
,'FLOREFERSOCPRFP','FLOREFERSOCPRFR','BNKREFERSOCPRFR','BNKREFERSOCPRFP') THEN 'Social Proof Referral' ELSE 'Non-Social Proof Referral' END AS 'AllocationType'
,COALESCE(lk.withdrawndt,lk.DeniedDt) as 'KillDate'
--,CASE WHEN COALESCE(lk.withdrawndt,lk.DeniedDt) IS NOT NULL THEN KillReason.ReasonText ELSE NULL END AS 'KillReason'
--,CASE WHEN COALESCE(lk.withdrawndt,lk.DeniedDt) IS NOT NULL THEN KillReason.ReasonDetailText ELSE NULL END AS 'KillSubReason'
,CASE WHEN COALESCE(lk.withdrawndt,lk.DeniedDt) IS NOT NULL THEN reason.reason ELSE NULL END AS 'KillReason'
,CASE WHEN COALESCE(lk.withdrawndt,lk.DeniedDt) IS NOT NULL THEN reason.subreason ELSE NULL END AS 'KillSubReason'
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
	(SELECT
	LMRF.LoanIdentifierDimSK
	,LMD.EventName
	,LMRD.LoanMilestoneReasonDescription 'Reason'
	,LMSRD.LoanMilestoneSubReasonDescription 'SubReason'
	,RECORD = ROW_NUMBER() OVER(PARTITION BY LMRF.LoanIdentifierDimSK ORDER BY LMRF.EventDateId DESC, LMRF.EventTimeId DESC)
	FROM [EDW].[LoanOrigination].[LoanMilestoneReasonFact] LMRF 
	INNER JOIN  EDW.Dimensions.LoanMilestoneDim LMD on LMD.LoanMilestoneSK = LMRF.LoanMilestoneSK
	INNER JOIN [EDW].[Dimensions].[LoanMilestoneSubReasonGroupBridge] LMSRGB on LMSRGB.LoanMilestoneSubReasonGroupBridgeSK = LMRF.LoanMilestoneSubReasonGroupBridgeSK
	INNER JOIN [EDW].[Dimensions].[LoanMilestoneReasonDim] LMRD on LMRD.LoanMilestoneReasonSK = LMSRGB.LoanMilestoneReasonSK
	INNER JOIN [EDW].[Dimensions].[LoanMilestoneSubReasonDim] LMSRD on LMSRD.LoanMilestoneSubReasonSK = LMSRGB.LoanMilestoneSubReasonSK
	WHERE 1=1
	AND lmd.groupid IN ('13','110') --fallout, withdrawn
	) as Reason
		ON reason.LoanIdentifierDimSK = lid.LoanIdentifierDimSK AND reason.record = 1

/*OUTER APPLY (
SELECT TOP 1 
 R.ReasonText
,rd.ReasonDetailText
FROM QLODS..LKWDTransFact LTF 
LEFT JOIN QLODS.dbo.LKWDStatusReasonGroupBridge b (NOLOCK) ON b.ReasonGroupID = ltf.ReasonGroupID
LEFT JOIN QLODS.dbo.LKWDStatusReasonDim r (NOLOCK) ON b.ReasonID = r.ReasonID
LEFT JOIN QLODS.dbo.LKWDStatusReasonDetailDim rd (NOLOCK) ON b.ReasonDetailID = rd.ReasonDetailID
WHERE 1=1
AND LTF.LoanNumber = Lid.LoanNumber
AND LTF.EventTypeID = 2 --status change
AND LTF.StatusID IN('84','87')-- 110 - Withdrawn, 100 - Denied
AND LTF.DeleteFlg = 0
AND LTF.RollBackFlg = 0
ORDER BY LTF.TransDateTime DESC 
)KillReason */

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
SELECT
 b.AllocationType
,b.MonthSK
,b.KillReason
,b.KillSubReason
,SUM(b.FolderFlg) as 'Folders'
,SUM(b.FolderToCloseFlg) as 'ConvertedFolders'
,AVG(b.FolderToCloseDays) as 'FolderToCloseDays'
FROM #base b

WHERE 1=1
GROUP BY b.AllocationType, b.MonthSK, b.killreason, b.KillSubReason