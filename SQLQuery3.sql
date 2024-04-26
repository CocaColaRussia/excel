CREATE OR REPLACE FUNCTION dma_fin.load_dm_free_short(
 p_idDateStart int4
,p_idDateEnd   int4 
)
RETURNS bool
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE 
--Параметры функции оставлю для простого дебага
     --p_idDateStart integer := '20240401'; 
     --p_idDateEnd   integer  = '20240401';
--Переменные внутри функции
     --v_dt_to_actual date;
     --v_period_id    integer;
     --v_id_from      integer; 
     --v_id_to_actual integer;
     --v_id_to        integer; 
     --v_id_prev      integer;
     --v_period       text;

begin  --Начало. Основная процедура
DECLARE @DateEnd DATE = DATEADD(d, -2, CURRENT_DATE()/*GETDATE()*/)

IF @idDateEnd		IS NULL SET @idDateEnd	 = DWH_BCS.[dbo].[fGET_ID_DATE_FROM_DATETIME](DATEADD(d,-2,GETDATE()))
IF @idDateStart IS NULL SET @idDateStart = 20230201

	
DECLARE @ProcessID        INT,
				@ProcessName      VARCHAR(100),
				@StepName         VARCHAR(100),
				@StartTime        DATETIME,
				@EndTime          DATETIME,
				@description      VARCHAR(8000),
				@ProcedureName    VARCHAR(100)		= OBJECT_NAME(@@PROCID)

SET @description = '@idDateStart = ' + CAST(@idDateStart AS VARCHAR(8)) + '; ' + '@idDateEnd = ' + CAST(@idDateEnd AS VARCHAR(8)) + ';'

-- инициализация процесса логирования
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID OUTPUT, @ProcessName   = 'LOAD_FCT_ALL_DATA', @ProcedureName = @ProcedureName,  @description   = @description

-- собираем группу типов
IF OBJECT_ID(N'tempdb..#DIC_TYPE2GROUPTYPE_1') IS NOT NULL DROP TABLE #DIC_TYPE2GROUPTYPE_1
SELECT * 
INTO #DIC_TYPE2GROUPTYPE_1
FROM DWH_BCS.DDS.DIC_TYPE2GROUPTYPE
WHERE 1=1
AND ID_GROUPTYPE = 1

-- собираем минимальные даты по генсогам
IF OBJECT_ID(N'TEMPDB..#AGREEMENT_FST') IS NOT NULL DROP TABLE #AGREEMENT_FST
SELECT 
 ID_CLIENT_U 
,MIN(AGG.[START_DATE]) AS MIN_START_DATE
INTO #AGREEMENT_FST
FROM DWH_BCS.DDS.DIC_AGREEMENT								AGG
INNER JOIN	DWH_BCS.DMA.DM_CLIENT2CLIENT_U		C2C  ON C2C.ID_CLIENT = AGG.ID_CLIENT
WHERE 1=1
AND AGG.DELETED_FLAG = 'N' 
AND AGG.ID_SOURCE NOT IN (31,32)
GROUP BY ID_CLIENT_U

CREATE UNIQUE CLUSTERED INDEX IDX_UNIQ ON #AGREEMENT_FST (ID_CLIENT_U) --3 сек

-- выбираем сеть ХМИ
IF OBJECT_ID(N'tempdb..#SEGMENT') IS NOT NULL DROP TABLE  #SEGMENT
SELECT * 
INTO #SEGMENT 
FROM DWH_BCS.DDS.DIC_SEGMENT_LV 
where 1=1
and LV1 not in ('FINS', 'Ген. дир.', 'Холдинг Корпоративный инвестиционный Банк', 'БКС Банк', 'ИББ')
	
-- берем полку 'Брокер'
IF OBJECT_ID(N'tempdb..#SHELFLV') IS NOT NULL DROP TABLE  #SHELFLV
SELECT *
INTO #SHELFLV
FROM DWH_BCS.DDS.DIC_SHELF_LV 
WHERE 1=1
AND LV1 = 'Брокер'
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INSERT #SHELFLV',@rowcount = @@ROWCOUNT,@description = ''

--собираем сделки по SUBACCOUNT_CCODE2 in ('6','240'), начиная с 1.02.2023
IF OBJECT_ID(N'tempdb..#DEALREPO_2') IS NOT NULL DROP TABLE #DEALREPO_2
SELECT * 
INTO #DEALREPO_2
FROM BCS_REPORT.rep.DEALREPO_SHORTS dr			/*[S-DWH-SRV-02].DWH_BCS.olap.DIM_DEALREPO_2 dr --создала таблицу, которая обновляется ежедневно по т-7.. т-2*/
WHERE 1=1
AND dr.SUBACCOUNT_CCODE2 in ('6','240') 
AND DR.REG_ACT_DATE1 >= '2023-02-01'
AND DR.REG_ACT_DATE1 <= @DateEnd
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #DEALREPO_2',@rowcount = @@ROWCOUNT,@description = ''


declare @REG_ACT_DATE1_START date = [BCS_REPORT].[bm].[int_to_date] (@idDateStart)

-- собираем сделки переноса маржинальных позиций (включая свопы) за период с 01.02.23 по 17.09.23 
IF OBJECT_ID(N'tempdb..#DEALREPO_FLAG') IS NOT NULL DROP TABLE  #DEALREPO_FLAG
SELECT * 
INTO #DEALREPO_FLAG
FROM #DEALREPO_2 dr 
WHERE 1=1
AND dr.SUBACCOUNT_CCODE2 in ('6','240') 
AND DR.REG_ACT_DATE1 < '2023-09-18'
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #DEALREPO_FLAG',@rowcount = @@ROWCOUNT,@description = ''

CREATE CLUSTERED INDEX IDX_UNIQ ON #DEALREPO_FLAG (ID_DEAL)  -- 10 сек
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'CLUSTERED INDEX IDX_UNIQ ON #DEALREPO_FLAG',@rowcount = @@ROWCOUNT,@description = ''

-- собираем сделки РЕПО с фильтрами по шортам
IF OBJECT_ID(N'tempdb..#DEALREPO') IS NOT NULL DROP TABLE #DEALREPO
SELECT *  -- для фильтра	суммы бесплатных шортов -- 19 секунд потребовалось всего
INTO #DEALREPO
FROM #DEALREPO_2  dr
WHERE 1=1
AND dr.[RATE_FPNL] = 0 
AND dr.DEALSUBTYPE = 'Обратное РЕПО'
AND dr.COUNTERPART = 'Финансовый Консалтинг'
AND DR.REG_ACT_DATE1 >= @REG_ACT_DATE1_START
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID, @StepName= 'INTO #DEALREPO', @rowcount = @@ROWCOUNT, @description = ''

	
CREATE CLUSTERED INDEX IDX_UNIQ ON #DEALREPO (ID_DEAL)  --3 сек
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'CLUSTERED INDEX IDX_UNIQ ON #DEALREPO',@rowcount = @@ROWCOUNT,@description = ''

--сделки РЕПО с фильтрами для фильтра общей суммы репо
IF OBJECT_ID(N'tempdb..#DEALREPOALL') IS NOT NULL DROP TABLE #DEALREPOALL
SELECT *  
INTO #DEALREPOALL
FROM #DEALREPO_2 DR
WHERE 1=1
AND dr.COUNTERPART = 'Финансовый Консалтинг'
AND REG_ACT_DATE1 >= @REG_ACT_DATE1_START
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #DEALREPOALL',@rowcount = @@ROWCOUNT,@description = ''
	

CREATE CLUSTERED INDEX IDX_UNIQ ON #DEALREPOALL (ID_DEAL)
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'CLUSTERED INDEX IDX_UNIQ ON #DEALREPOALL',@rowcount = @@ROWCOUNT,@description = ''

--************************************ Начало. Rest ************************************************	
--соберем остатки по общим фильтрам
IF OBJECT_ID(N'tempdb..#REST') IS NOT NULL DROP TABLE  #REST  --19 минут
SELECT 
 ID_DATE								  = ATR.ID_DATE
,ID_CLIENT_U						  = ATR.ID_CLIENT_U
,ID_AGREEMENT						  = ATR.ID_AGREEMENT
,ID_DEAL								  = ATR.ID_DEAL
,ID_FLOOR								  = atr.ID_FLOOR
,ID_CLIENT_SEGMENT_2014	  = atr.ID_CLIENT_SEGMENT_2014
,ID_MARKETPLACE					  = atr.ID_MARKETPLACE
,ID_PRODUCT 						  = atr.ID_PRODUCT 
,ID_SHELF								  = atr.ID_SHELF
,ID_FININSTR						  = atr.ID_FININSTR
,ID_RESTTYPE						  = atr.ID_RESTTYPE
,ID_DEALTYPE						  = ATR.ID_DEALTYPE
,ID_CURRENCY						  = ATR.ID_CURRENCY
,BALANCE_AMT_RUR				  = BALANCE_AMT_RUR
,ID_ACCOUNT							  = ATR.ID_ACCOUNT
INTO #REST
FROM BCS_SLICE.[olap].fFCT_REST_CUBE_WITH_ATR(@idDateStart,@idDateEnd)	atr		 /*[BCS_SLICE].[olap].[vFCT_REST_CUBE_WITH_ATR]*/
inner join #DIC_TYPE2GROUPTYPE_1																				t										 ON atr.ID_CORR = t.id_type 
INNER JOIN #SEGMENT																											SEG									 ON SEG.ID_SEGMENT = ATR.ID_CLIENT_SEGMENT_2014 
inner join #SHELFLV																											SLV									 ON SLV.ID_SHELF = ATR.ID_SHELF  
WHERE 1=1
AND ATR.ID_DATE >= @idDateStart	
AND ATR.ID_DATE < @idDateEnd --< --можно поменять на <=
AND ATR.ID_RESTTYPE = 30
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #REST',@rowcount = @@ROWCOUNT,@description = ''

	-- для t-2 лучше взять из [olap].[fFCT_REST_CUBE_WITH_ATR]
IF OBJECT_ID('tempdb..#Calendar') IS NOT NULL DROP TABLE #Calendar
;WITH ten AS(
SELECT * FROM (VALUES(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) T(n)
),
millions AS(
SELECT RowNum = ROW_NUMBER() OVER (ORDER BY (SELECT '1') )
FROM ten t1
CROSS JOIN ten t2
CROSS JOIN ten t3
CROSS JOIN ten t4
CROSS JOIN ten t5
),
Calendar as(
SELECT
Dates = DATEADD(d,RowNum-1, cast('19900101' as date) )
FROM millions
),
fields as(
select
 Dates			 =  Dates
,[Dates_int] =  cast(LEFT(Dates,4) + RIGHT(LEFT(Dates,7),2) + RIGHT(Dates,2) as int)
from Calendar
where Dates <= (getdate())
)
select 
 [Dates_int]					
,[Dates_datetime] = cast(Dates as datetime)
,[Dates_date]		  = cast(Dates as date)
into #Calendar 
from fields

--declare @idDateStart int = 20240414
--declare @idDateEnd	 int = 20240414
IF OBJECT_ID('tempdb..#CLIENT_ATR') IS NOT NULL DROP TABLE #CLIENT_ATR
select 
 ID_DATE
,ID_CLIENT_U
,ID_FLOOR
,ID_CLIENT_SEGMENT_2014
into #CLIENT_ATR
from DWH_BCS.[olap].[fFCT_CLIENT_ALL_SEGMENT_ATTR](@idDateStart, @idDateEnd)
where 1=1
AND ID_DATE between @idDateStart and @idDateEnd

IF OBJECT_ID('tempdb..#DM_REST_BLNC_ALL') IS NOT NULL DROP TABLE #DM_REST_BLNC_ALL
SELECT
 ID_DATE									=  P.ID_DATE
,ID_CLIENT_U							=  ISNULL(cl_u.ID_CLIENT_U, C.ID_CLIENT_U)
,ID_AGREEMENT							=  P.ID_AGREEMENT
,ID_DEAL									=  P.ID_DEAL
,ID_FLOOR									=  COALESCE(P.[ID_FLOOR], CA.[ID_FLOOR],-1)
,ID_CLIENT_SEGMENT_2014		=  COALESCE(d2acco.ID_CLIENT_SEGMENT, P.ID_CLIENTSEGMENT, CA.ID_CLIENT_SEGMENT_2014, -1)
,ID_MARKETPLACE						=  P.ID_MARKETPLACE
,ID_PRODUCT								=  P.ID_PRODUCT
,ID_SHELF									=  ISNULL(P.ID_PNL, -1)
,ID_FININSTR							=  P.ID_FININSTR
,ID_RESTTYPE							=  ISNULL(RT.ID_RESTTYPE,-1)
,ID_DEALTYPE							=  -1
,ID_CURRENCY							=  P.ID_CURRENCY
,ID_ACCOUNT								=  P.ID_ACCOUNT
,ID_CORR								  =  P.ID_CORR
,BALANCE_AMT_RUR					=  P.VL
into #DM_REST_BLNC_ALL
FROM DWH_BCS.DWH.DM_REST_BLNC_ALL				  (NOLOCK)  P		  	  
INNER JOIN #Calendar																Calendar  ON  P.ID_DATE = Calendar.[Dates_int]
LEFT JOIN DWH_BCS.olap.DIC_CLIENT				  (NOLOCK)  C					ON  C.ID_CLIENT = P.ID_CLIENT				 
LEFT JOIN DWH_BCS.DMA.DM_CLIENT2CLIENT_U  (NOLOCK)  CL2				ON  CL2.ID_CLIENT = P.ID_CLIENT AND ( Calendar.[Dates_date] /*CAST(P.ID_DATE AS VARCHAR(8))*/ BETWEEN CL2.[START_DATE] AND CL2.END_DATE) AND P.ID_DATE >= 20200401
LEFT JOIN DWH_BCS.dma.dic_Client_u			  (NOLOCK)  cl_u			ON  ISNULL(CL2.ID_CLIENT_U, C.ID_CLIENT_U) = cl_u.id_Client_u
LEFT /*HASH*/ JOIN #CLIENT_ATR											CA				ON  CA.ID_DATE = P.ID_DATE AND CA.ID_CLIENT_U = ISNULL(cl_u.id_Client_u, C.ID_CLIENT_U) 
LEFT JOIN DWH_BCS.DMA.DM_DEAL2ACCO				(NOLOCK)	d2acco		ON  d2acco.ID_DEAL = p.ID_DEAL and d2acco.ID_DATE >= p.ID_DATE
LEFT JOIN DWH_BCS.DWH.DWH_REST					  (NOLOCK)  RT				ON  RT.ID_REST = P.ID_REST AND RT.ID_DATE = P.ID_DATE and RT.ID_DATE between @idDateStart and @idDateEnd
where 1=1
AND P.ID_DATE BETWEEN @idDateStart AND @idDateEnd
and P.ID_DATE >= 20200101 


INSERT INTO #REST (ID_DATE,ID_CLIENT_U,ID_AGREEMENT,ID_DEAL,ID_FLOOR,ID_CLIENT_SEGMENT_2014,ID_MARKETPLACE, ID_PRODUCT, ID_SHELF, ID_FININSTR, ID_RESTTYPE,ID_DEALTYPE,ID_CURRENCY,BALANCE_AMT_RUR,ID_ACCOUNT)
SELECT 
 ID_DATE									= ATR.ID_DATE
,ID_CLIENT_U							= ATR.ID_CLIENT_U
,ID_AGREEMENT							= ATR.ID_AGREEMENT
,ID_DEAL									= ATR.ID_DEAL
,ID_FLOOR									= ATR.ID_FLOOR
,ID_CLIENT_SEGMENT_2014		= ATR.ID_CLIENT_SEGMENT_2014
,ID_MARKETPLACE						= ATR.ID_MARKETPLACE
,ID_PRODUCT								= ATR.ID_PRODUCT
,ID_SHELF									= ATR.ID_SHELF
,ID_FININSTR							= ATR.ID_FININSTR
,ID_RESTTYPE							= ATR.ID_RESTTYPE
,ID_DEALTYPE							= ATR.ID_DEALTYPE
,ID_CURRENCY							= ATR.ID_CURRENCY
,BALANCE_AMT_RUR					= ATR.BALANCE_AMT_RUR
,ID_ACCOUNT								= ATR.ID_ACCOUNT
FROM #DM_REST_BLNC_ALL						ATR		/*[DWH_BCS].[olap].[fFCT_REST_CUBE_WITH_ATR](@idDateEnd,@idDateEnd)*/
INNER JOIN #DIC_TYPE2GROUPTYPE_1	t			ON ATR.ID_CORR = t.id_type 
INNER JOIN #SEGMENT								SEG		ON SEG.ID_SEGMENT = ATR.ID_CLIENT_SEGMENT_2014 
INNER JOIN #SHELFLV								SLV		ON SLV.ID_SHELF = ATR.ID_SHELF  
WHERE 1=1
AND ATR.ID_DATE = @idDateEnd
AND ATR.ID_RESTTYPE = 30
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #REST t-2',@rowcount = @@ROWCOUNT,@description = ''


----CREATE UNIQUE CLUSTERED INDEX IDX_UNIQ ON #REST (ID_CLIENT_U,ID_DATE, ID_DEAL)

-- выберем остатки по шортам
IF OBJECT_ID(N'tempdb..#REST_F') IS NOT NULL DROP TABLE  #REST_F
select * 
INTO #REST_F
FROM #REST
WHERE 1=1								--ID_RESTTYPE = 30 
AND ID_MARKETPLACE = 32 --ММВБ 
AND ID_CURRENCY = 19		--рубль 
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #REST_F',@rowcount = @@ROWCOUNT,@description = ''

--считаем общую сумму репо 
--считаем остатки по сделкам репо (ОСТАТКИ!!!)
IF OBJECT_ID(N'TEMPDB..#REPOTOTAL1') IS NOT NULL DROP TABLE  #REPOTOTAL1
SELECT
 ID_CLIENT_U						 = ATR.ID_CLIENT_U
,id_date								 = atr.id_date
,ID_DEAL								 = ATR.ID_DEAL
,ID_AGREEMENT						 = ATR.ID_AGREEMENT
,ID_FLOOR								 = ATR.ID_FLOOR
,ID_CLIENT_SEGMENT_2014	 = ATR.ID_CLIENT_SEGMENT_2014
,ID_MARKETPLACE					 = ATR.ID_MARKETPLACE
,ID_PRODUCT							 = ATR.ID_PRODUCT
,ID_SHELF								 = ATR.ID_SHELF
,ID_RESTTYPE						 = ATR.ID_RESTTYPE
,ID_DEALTYPE						 = ATR.ID_DEALTYPE
,ID_CURRENCY						 = ATR.ID_CURRENCY
,ID_FININSTR						 = ATR.ID_FININSTR
,ID_ACCOUNT							 = ATR.ID_ACCOUNT
,REPO_RST								 = BALANCE_AMT_RUR
INTO #REPOTOTAL1
FROM #REST									ATR
INNER JOIN #DEALREPOALL			DR		ON DR.ID_DEAL = ATR.ID_DEAL AND DR.ID_ACCOUNT = ATR.ID_ACCOUNT
--WHERE ID_RESTTYPE = 30 
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #REPOTOTAL1',@rowcount = @@ROWCOUNT,@description = ''
--************************************ Конец. Rest ************************************************	

-- общая сумма репо по сделкам
IF OBJECT_ID(N'TEMPDB..#REPOTOTAL2') IS NOT NULL DROP TABLE  #REPOTOTAL2
SELECT DISTINCT 
 ID_account								= dr.ID_account
,REG_ACT_DATE1						= DR.REG_ACT_DATE1
,ID_DEAL									= DR.ID_DEAL
,REPO											= DR.AMT1 
,ID_Date									= 0
,ID_ACCOUNT_R							= R.ID_ACCOUNT 
,ID_DEAL_R								= R.ID_DEAL 
,ID_AGREEMENT							= R.ID_AGREEMENT
,ID_CLIENT_SEGMENT_2014		= R.ID_CLIENT_SEGMENT_2014
,ID_CLIENT_U							= R.ID_CLIENT_U
,ID_CURRENCY							= R.ID_CURRENCY
,ID_DEALTYPE							= R.ID_DEALTYPE
,ID_FININSTR							= R.ID_FININSTR
,ID_MARKETPLACE						= R.ID_MARKETPLACE
,ID_RESTTYPE							= R.ID_RESTTYPE
,ID_FLOOR									= R.ID_FLOOR
,ID_SHELF									= R.ID_SHELF
,ID_PRODUCT								= R.ID_PRODUCT
INTO #REPOTOTAL2
FROM  #DEALREPOALL	DR 
LEFT JOIN #REST			r		ON DR.id_deal = r.id_deal and dr.id_account = r.ID_ACCOUNT 
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #REPOTOTAL2',@rowcount = @@ROWCOUNT,@description = ''
		

UPDATE #REPOTOTAL2 
SET ID_Date = DWH_BCS.[dbo].[fGET_ID_DATE_FROM_DATETIME](REG_ACT_DATE1 ) -- 11 СЕК НА ГОД
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'UPDATE #REPOTOTAL2',@rowcount = @@ROWCOUNT,@description = ''


--считаем бесплатные шорты
IF OBJECT_ID(N'tempdb..#SHORT') IS NOT NULL DROP TABLE #SHORT
SELECT /*RN = ROW_NUMBER() OVER (PARTITION BY ATR.ID_CLIENT_U, ATR.ID_DEAL ORDER BY ATR.ID_DATE),*/
 ID_DATE									= atr.ID_DATE
,ID_CLIENT_U							= atr.ID_CLIENT_U
,ID_AGREEMENT							= atr.ID_AGREEMENT
,ID_DEAL									= atr.ID_DEAL
,ID_FLOOR									= atr.ID_FLOOR
,ID_CLIENT_SEGMENT_2014		= atr.ID_CLIENT_SEGMENT_2014
,ID_MARKETPLACE						= atr.ID_MARKETPLACE
,ID_PRODUCT								= atr.ID_PRODUCT
,ID_SHELF									= atr.ID_SHELF
,ID_FININSTR							= atr.ID_FININSTR
,ID_RESTTYPE							= atr.ID_RESTTYPE
,ID_DEALTYPE							= atr.ID_DEALTYPE
,ID_CURRENCY							= atr.ID_CURRENCY
,BALANCE_AMT_RUR					= atr.BALANCE_AMT_RUR
,ID_ACCOUNT								= atr.ID_ACCOUNT                    
INTO #SHORT
FROM #REST_F					atr
INNER JOIN #dealrepo  dr	on  dr.id_deal = atr.ID_DEAL AND DR.ID_ACCOUNT = ATR.ID_ACCOUNT
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #SHORT',@rowcount = @@ROWCOUNT,@description = ''
		

--CREATE UNIQUE CLUSTERED INDEX IDX_UNIQ ON #SHORT (ID_CLIENT_U,ID_DATE, ID_DEAL)
	
-- СОБРАЛИ СДЕЛКИ, КОТОРЫХ НЕТ В ОСТАТКАХ, В Т.Ч. ЗАКРЫТЫЕ ОДНИМ ДНЕМ
IF OBJECT_ID(N'TEMPDB..#REPOTOTAL3') IS NOT NULL DROP TABLE  #REPOTOTAL3
SELECT 
 ID_account		= ID_account
,ID_DEAL			= ID_DEAL
,REPO					= REPO
,ID_Date			= ID_Date
INTO #REPOTOTAL3
FROM #REPOTOTAL2 R2 
WHERE 1=1
AND R2.ID_DEAL_R IS NULL 
AND EXISTS (SELECT TOP 1 1 FROM #SHORT S WHERE S.ID_ACCOUNT = R2.ID_ACCOUNT)
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #REPOTOTAL3',@rowcount = @@ROWCOUNT,@description = ''
	

CREATE CLUSTERED INDEX IDX_CLR ON #REPOTOTAL3 (ID_Date)
CREATE NONCLUSTERED INDEX IDX_CLR1 ON #REPOTOTAL3 (ID_Date)
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'CREATE INDEX #REPOTOTAL3',@rowcount = @@ROWCOUNT,@description = ''


IF OBJECT_ID(N'TEMPDB..#NORESTDEALS') IS NOT NULL DROP TABLE #NORESTDEALS
SELECT 
TT.ID_CURRENCY,
TT.ID_FININSTR,
TT.ID_MARKETPLACE,
TT.ID_PRODUCT,
R3.*
INTO #NORESTDEALS
FROM DWH_BCS.DWH.DM_TRAN_TURN		TT  WITH (NOLOCK)
INNER JOIN #REPOTOTAL3					R3  							 ON R3.ID_Date = TT.ID_DATE AND R3.ID_DEAL = TT.ID_DEAL AND R3.ID_ACCOUNT = TT.ID_ACCOUNT_CRED AND TT.ID_FININSTR <>-1 
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #NORESTDEALS',@rowcount = @@ROWCOUNT,@description = ''


IF OBJECT_ID(N'TEMPDB..#NORESTDEALS_CL') IS NOT NULL DROP TABLE #NORESTDEALS_CL
SELECT DISTINCT
AG.ID_AGREEMENT,
AG.ID_CLIENT,
C2C.ID_CLIENT_U, 
ATR.ID_FLOOR, 
ATR.ID_CLIENT_SEGMENT_2014,
NRD.*
INTO #NORESTDEALS_CL
FROM #NORESTDEALS																				 NRD
INNER JOIN DWH_BCS.DDS.DIC_ACCOUNT											 ACC   ON ACC.ID_ACCOUNT = NRD.ID_ACCOUNT
INNER JOIN DWH_BCS.DDS.DIC_AGREEMENT										 AG	   ON AG.ID_AGREEMENT = ACC.ID_AGREEMENT
INNER JOIN DWH_BCS.DMA.DM_CLIENT2CLIENT_U								 C2C   ON AG.ID_CLIENT = C2C.ID_CLIENT AND END_DATE >GETDATE()
INNER JOIN DWH_BCS.DMA.VDIC_CLIENT_ALL_SEGMENT_ATR_ALL 	 ATR   ON ATR.ID_CLIENT_U = C2C.ID_CLIENT_U AND ATR.ID_DATE = NRD.ID_Date 
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID,@StepName= 'INTO #NORESTDEALS_CL',@rowcount = @@ROWCOUNT,@description = ''
	

-- выбираем клиентов по фильтрам "бесплатные шорты"
IF OBJECT_ID(N'TEMPDB..#CLIENTS') IS NOT NULL DROP TABLE  #CLIENTS
SELECT DISTINCT 
ID_CLIENT_U 
INTO #CLIENTS 
FROM #SHORT

-- считаем сумму активов на 18 сентября для отобранных клиентов
IF OBJECT_ID(N'tempdb..#ACTIVE') IS NOT NULL DROP TABLE  #ACTIVE
SELECT 
 ID_CLIENT_U		 = ATR.ID_CLIENT_U
,Active_20230918 = CAST(SUM( ISNULL(ATR.BALANCE_AMT_RUR,0)) AS DECIMAL(16,2)) 
INTO #ACTIVE
FROM BCS_SLICE.[olap].fFCT_REST_CUBE_WITH_ATR(@idDateStart,@idDateEnd) ATR   /*[BCS_SLICE].[olap].[vFCT_REST_CUBE_WITH_ATR]*/
inner join #DIC_TYPE2GROUPTYPE_1																			 t   ON atr.id_corr = t.id_type 
inner JOIN #CLIENTS																										 c   ON c.ID_CLIENT_U = atr.ID_CLIENT_U 
WHERE 1=1
AND ATR.ID_DATE = 20230918 
group by ATR.ID_CLIENT_U

CREATE UNIQUE CLUSTERED INDEX IDX_UNIQ ON #ACTIVE (ID_CLIENT_U)

-- собираем клиентов для проставления флага наличия сделки переноса маржинальных позиций (включая свопы) за период с 01.02.23 по 17.09.23 
IF OBJECT_ID(N'tempdb..#FLAG') IS NOT NULL DROP TABLE  #FLAG
SELECT DISTINCT ID_CLIENT_U
INTO #FLAG
FROM #REST S
INNER JOIN #DEALREPO_FLAG F ON F.ID_DEAL = S.ID_DEAL and f.id_account = s.id_account

-- собираем результат в два этапа - 1 + остатки репо, 2 + дневной оборот репо + активы
    
IF OBJECT_ID(N'tempdb..#FINALREPOREST') IS NOT NULL
DROP TABLE  #FINALREPOREST
SELECT 
 ID_DATE								 = ISNULL(S.ID_DATE, RT1.ID_DATE)																
,ID_CLIENT_U						 = ISNULL (S.ID_CLIENT_U,RT1.ID_CLIENT_U)												
,ID_AGREEMENT						 = ISNULL (S.ID_AGREEMENT,RT1.ID_AGREEMENT)											
,ID_DEAL								 = ISNULL (S.ID_DEAL,RT1.ID_DEAL)																
,ID_FLOOR								 = ISNULL (S.ID_FLOOR,RT1.ID_FLOOR)															
,ID_CLIENT_SEGMENT_2014	 = ISNULL (S.ID_CLIENT_SEGMENT_2014,RT1.ID_CLIENT_SEGMENT_2014)	
,ID_MARKETPLACE					 = ISNULL (S.ID_MARKETPLACE,RT1.ID_MARKETPLACE)									
,ID_PRODUCT							 = ISNULL (S.ID_PRODUCT,RT1.ID_PRODUCT)													
,ID_SHELF								 = ISNULL (S.ID_SHELF,RT1.ID_SHELF)															
,ID_FININSTR						 = ISNULL (S.ID_FININSTR,RT1.ID_FININSTR)												
,ID_RESTTYPE						 = ISNULL (S.ID_RESTTYPE, RT1.ID_RESTTYPE)												
,ID_DEALTYPE						 = ISNULL (S.ID_DEALTYPE, RT1.ID_DEALTYPE)												
,ID_CURRENCY						 = ISNULL (S.ID_CURRENCY, RT1.ID_CURRENCY)												
,ID_ACCOUNT							 = ISNULL (S.ID_ACCOUNT, RT1.ID_ACCOUNT)													
,BALANCE_AMT_RUR				 = S.BALANCE_AMT_RUR																						
,REPO_REST							 = ISNULL(CAST(RT1.REPO_RST as decimal(16,2)),0)									
INTO #FINALREPOREST
FROM  #SHORT S
FULL OUTER JOIN #REPOTOTAL1 RT1 ON RT1.ID_DATE = S.ID_DATE AND RT1.ID_CLIENT_U = S.ID_CLIENT_U and rt1.ID_DEAL = s.ID_DEAL
INNER JOIN #CLIENTS c on c.ID_CLIENT_U = rt1.ID_CLIENT_U
/*	------------------------------------------TEST
WHERE  (S.ID_DATE = 20231124 OR RT1.ID_DATE= 20231124)
AND (S.id_account = 24553444 OR RT1.ID_CLIENT_U = -147812100 )
------------------------------------------TEST
*/	
	
	
IF OBJECT_ID(N'tempdb..#REPO_VL1') IS NOT NULL	DROP TABLE  #REPO_VL1
SELECT 
 ID_DATE 								 = S.ID_DATE
,ID_CLIENT_U						 = S.ID_CLIENT_U
,ID_AGREEMENT						 = S.ID_AGREEMENT
,ID_DEAL								 = S.ID_DEAL
,ID_FLOOR								 = S.ID_FLOOR
,ID_CLIENT_SEGMENT_2014	 = S.ID_CLIENT_SEGMENT_2014
,ID_MARKETPLACE					 = S.ID_MARKETPLACE
,ID_PRODUCT							 = S.ID_PRODUCT
,ID_SHELF								 = S.ID_SHELF			
,ID_FININSTR						 = S.ID_FININSTR	
,ID_RESTTYPE						 = S.ID_RESTTYPE		
,ID_DEALTYPE						 = S.ID_DEALTYPE		
,ID_CURRENCY						 = S.ID_CURRENCY
,ID_ACCOUNT							 = S.ID_ACCOUNT
,BALANCE_AMT_RUR				 = S.BALANCE_AMT_RUR
,REPO_REST							 = REPO_REST
,repo_vl								 = isnull(cast(r.REPO as decimal(16,2)),0) 
INTO #REPO_VL1
FROM #FINALREPOREST S
LEFT join #REPOTOTAL2 r on R.id_date = S.ID_DATE AND R.ID_ACCOUNT = S.ID_ACCOUNT and r.id_deal = S.id_deal
--------------------------------------------TEST
--WHERE  (S.ID_DATE = 20231124 OR R.ID_DATE= 20231124)
--AND (S.id_account = 24553444 OR R.id_account = 24553444)
--------------------------------------------TEST
	
--select * from #REPO_VL1 where id_date = 20231124 and id_account = 26975312
	
	
--собираем маппинг для аналитики (полки)
IF OBJECT_ID(N'tempdb..#SHELVES') IS NOT NULL	DROP TABLE  #SHELVES
SELECT * 
INTO #SHELVES
FROM DWH_BCS.dma.DM_FININSTR_PRODUCT2SHELF p2s
WHERE EXISTS (SELECT TOP 1 1 FROM (SELECT DISTINCT ID_FININSTR, ID_PRODUCT FROM #NORESTDEALS_CL) P WHERE p.ID_FININSTR = p2s.ID_FININSTR AND P.ID_PRODUCT = P2S.ID_PRODUCT)

IF OBJECT_ID(N'tempdb..#SHELVES2') IS NOT NULL DROP TABLE  #SHELVES2
SELECT * 
INTO #SHELVES2 
FROM DWH_BCS.dma.DM_PRODUCT2SHELF P2S
WHERE EXISTS (SELECT TOP 1 1 FROM (SELECT DISTINCT ID_FININSTR, ID_PRODUCT FROM #NORESTDEALS_CL) P WHERE P.ID_PRODUCT = P2S.ID_PRODUCT)


--declare @idDateStart int = 20240301
--declare @ID_PREV_DATE INT


--SET @ID_PREV_DATE = DWH_BCS.[DBO].[FGET_ID_DATE_FROM_DATETIME](DATEADD(D,-1,CAST(@IDDATESTART AS VARCHAR(10))))
--SELECT @ID_PREV_DATE

/*20240412. БузынинДА https://jira.bcs.ru/browse/FINDIR-16715 принято решение список клиентов загрузить явно*/		 
IF OBJECT_ID(N'tempdb..#MARGIN_FLAG') IS NOT NULL DROP TABLE  #MARGIN_FLAG
SELECT DISTINCT
 ID_CLIENT_U
,MARGIN_FLAG
INTO #MARGIN_FLAG
FROM BCS_REPORT.[rep].[DM_FREE_SHORT_MARGIN_CLIENTS]
--WHERE MARGIN_FLAG = 1

--ID_DATE,ID_CLIENT_U,ID_AGREEMENT,ID_DEAL,ID_FLOOR,ID_CLIENT_SEGMENT_2014,ID_MARKETPLACE,ID_PRODUCT,ID_SHELF,ID_FININSTR,ID_RESTTYPE,ID_DEALTYPE,
	--	ID_CURRENCY,ID_ACCOUNT,FIRST_AGREEMENT_DATE,BALANCE_AMT_RUR, --  REPO_AMOUNT (REPO_REST),,REPO_AMOUNT_2 (REPO_VL) - В КОНЦЕ САМОМ ACTIVES_20230918,MARGIN_FLAG	
	
IF OBJECT_ID(N'tempdb..#FINALRESULT') IS NOT NULL DROP TABLE  #FINALRESULT
SELECT 
 ID_DATE														= ISNULL(S.ID_DATE,RT.ID_DATE)																
,ID_CLIENT_U												= ISNULL(S.ID_CLIENT_U,RT.ID_CLIENT_U)												
,ID_AGREEMENT												= ISNULL(S.ID_AGREEMENT,RT.ID_AGREEMENT)											
,ID_DEAL														= ISNULL(S.ID_DEAL,RT.ID_DEAL)																
,ID_FLOOR														= ISNULL(S.ID_FLOOR,RT.ID_FLOOR)															
,ID_CLIENT_SEGMENT_2014							= ISNULL(S.ID_CLIENT_SEGMENT_2014, RT.ID_CLIENT_SEGMENT_2014) 
,ID_MARKETPLACE											= ISNULL(S.ID_MARKETPLACE,RT.ID_MARKETPLACE)									
,ID_PRODUCT													= ISNULL(S.ID_PRODUCT,RT.ID_PRODUCT)													
,ID_SHELF														= COALESCE(S.ID_SHELF, SL.ID_SHELF, SL2.ID_SHELF)							
,ID_FININSTR												= ISNULL(S.ID_FININSTR,RT.ID_FININSTR)												
,ID_RESTTYPE												= ISNULL(S.ID_RESTTYPE,-1)																		
,ID_DEALTYPE												= ISNULL(S.ID_DEALTYPE,-1)																		
,ID_CURRENCY												= ISNULL(S.ID_CURRENCY,RT.ID_CURRENCY)												
,ID_ACCOUNT													= ISNULL(S.ID_ACCOUNT, RT.ID_ACCOUNT)													
,FIRST_AGREEMENT_DATE								= ISNULL(AGF.MIN_START_DATE, AGF1.MIN_START_DATE)							
,BALANCE_AMT_RUR										= ISNULL(S.BALANCE_AMT_RUR,0)																	
,REPO_AMOUNT												= ISNULL(S.REPO_REST,0)																				
,REPO_AMOUNT_V2											= isnull(S.repo_vl,CAST(RT.REPO AS decimal(16,2))) 														
,ACTIVES_20230918										= COALESCE(A.Active_20230918, A1.Active_20230918, 0)						
,MARGIN_FLAG												= ISNULL(MF.MARGIN_FLAG,0)
/*CASE WHEN ISNULL(MF.MARGIN_FLAG,0) = 1 THEN 1 WHEN F.ID_CLIENT_U IS NULL AND F1.ID_CLIENT_U IS NULL THEN 0 ELSE 1 END AS MARGIN_FLAG*/
/*20240412. БузынинДА https://jira.bcs.ru/browse/FINDIR-16715 принято решение список клиентов загрузить явно*/		 
INTO #FINALRESULT
FROM  #REPO_VL1									S
FULL OUTER JOIN #NORESTDEALS_CL RT	  ON  RT.id_date = S.ID_DATE AND RT.ID_ACCOUNT = S.ID_ACCOUNT and rt.id_deal = s.id_deal
LEFT JOIN #MARGIN_FLAG					MF	  ON  MF.ID_CLIENT_U = ISNULL(S.ID_CLIENT_U,RT.ID_CLIENT_U)
LEFT JOIN #ACTIVE								A		  ON  A.ID_CLIENT_U = S.ID_CLIENT_U
LEFT JOIN #ACTIVE								A1	  ON  A1.ID_CLIENT_U = RT.ID_CLIENT_U
LEFT JOIN #FLAG									F		  ON  F.ID_CLIENT_U = S.ID_CLIENT_U
LEFT JOIN #FLAG									F1	  ON  F1.ID_CLIENT_U = RT.ID_CLIENT_U
LEFT JOIN #SHELVES							SL	  ON  SL.ID_PRODUCT = RT.ID_PRODUCT AND SL.ID_FININSTR = RT.ID_FININSTR
LEFT JOIN #SHELVES2							SL2	  ON  SL2.ID_PRODUCT = RT.ID_PRODUCT
LEFT JOIN #AGREEMENT_FST				AGF	  ON  AGF.ID_CLIENT_U = S.ID_CLIENT_U
LEFT JOIN #AGREEMENT_FST				AGF1  ON  AGF1.ID_CLIENT_U = RT.ID_CLIENT_U
--	------------------------------------------TEST
--	WHERE  (S.ID_DATE = 20231124 OR RT.ID_DATE= 20231124)
--	AND (S.id_account = 24553444 OR RT.id_account = 24553444)
--	------------------------------------------TEST
--select * from #FINALRESULT where id_date = 20231124 and id_account = 24553444
--select * from #FINALRESULT where id_date = 20231124 and id_account = 24553444-- 26975312--


DELETE 
FROM BCS_REPORT.[rep].[DM_FREE_SHORT] 
WHERE 1=1
AND id_date >= @idDateStart 
AND id_date <= @idDateEnd
	
INSERT INTO BCS_REPORT.[rep].[DM_FREE_SHORT]
(
 ID_DATE
,ID_CLIENT_U
,ID_AGREEMENT
,ID_DEAL
,ID_FLOOR
,ID_CLIENT_SEGMENT_2014
,ID_MARKETPLACE
,ID_PRODUCT
,ID_SHELF
,ID_FININSTR
,ID_RESTTYPE
,ID_DEALTYPE
,ID_CURRENCY
,ID_ACCOUNT
,FIRST_AGREEMENT_DATE
,BALANCE_AMT_RUR
,REPO_AMOUNT
,ACTIVES_20230918
,MARGIN_FLAG
,REPO_AMOUNT_2
)
SELECT 
 ID_DATE
,ID_CLIENT_U
,ID_AGREEMENT
,ID_DEAL
,ID_FLOOR
,ID_CLIENT_SEGMENT_2014
,ID_MARKETPLACE
,ID_PRODUCT
,ID_SHELF
,ID_FININSTR
,ID_RESTTYPE
,ID_DEALTYPE
,ID_CURRENCY
,ID_ACCOUNT
,FIRST_AGREEMENT_DATE
,BALANCE_AMT_RUR
,REPO_AMOUNT
,ACTIVES_20230918
,MARGIN_FLAG
,REPO_AMOUNT_V2
FROM #FINALRESULT
EXEC [BCS_REPORT].[log].[Calculation_Process_Log] @ProcessID = @ProcessID, @StepName  = 'end'

return true;

end;  --Конец.  Основная процедура



$$
EXECUTE ON ANY;