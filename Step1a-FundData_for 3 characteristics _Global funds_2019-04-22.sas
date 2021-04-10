proc datasets library= work kill nolist; quit;

/*libname FCApr14 'D:\Mutual Funds - Holding Classification\SASData\FCApr14'; run;  ***The folder was created on Apr 14, 2013;*/
/*libname FCApr14 'D:\Data\Projects\Mutual Funds - Holding Classification\SASData\FCApr14'; run; ***On new comp;*/
libname FCSep29 'E:\Drive\Local Disk F\Mutual fund competition'; run;
libname mfglobal 'G:\MF competition\Global'; run;
libname MF 'G:\MF competition'; run;
libname MATCH 'D:\BAB Adarsh'; run;

LIBNAME PN 'D:\BAB Adarsh'; run; *Prof Nitin's 2TB HDD;
LIBNAME PN1 'D:\BAB Adarsh\Xpressfeed tables'; run;
LIBNAME PNF 'D:\BAB Adarsh\Xpressfeed tables\Full Holdings sample'; run;
LIBNAME PND 'D:\BAB Adarsh\Xpressfeed tables\Domestic Holdings sample'; run;

%let wrds=wrds.wharton.upenn.edu 4016; 
options comamid=TCP remote=WRDS;        
signon username=_prompt_;               
                                         
libname rwork slibref = work server = wrds; run;

rsubmit;
options nocenter nodate nonumber ls=max ps=max msglevel=i; 
libname mf '/wrds/crsp/sasdata/q_mutualfunds'; run; *refers to crsp;
libname crspa '/wrds/crsp/sasdata/a_stock'; run; *refers to crsp;
libname ff '/wrds/ff/sasdata'; run;
libname s12 '/wrds/tfn/sasdata/s12'; run; *refers to tfn;
libname mfl '/wrds/mfl/sasdata'; run;
libname a_ccm '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname compa '/wrds/comp/sasdata/naa'; run; *refers to compa;
libname compg '/wrds/comp/sasdata/d_global'; run; *refers to compg;
libname pn '/wrds/comp/sasdata/naa/pension'; run; *compustat pension;
libname ash '/home/isb/adarshkp/Mutual fund comp'; run;
libname temp '/scratch/isb/Adarsh'; run;


*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan1980'd;
%let crspenddate = '31MAY2019'd; 

*Specify Thomson-Reuters begin and end dates;
%let tfnbegdate = '01Jan1980'd;
%let tfnenddate = '31MAY2019'd; 
endrsubmit;



/******************************/
/*** CRSP MUTUAL FUNDS DATA ***/
/******************************/

**Matched Global and Candaian securities from BAB project 'H:\Global holdings data cleaned\Refined';
*pnd.table1_matched is year-wise matched global securites from Morningstar domestic holdings dataset with unique global securities from Xpressfeed;
*pnd.table2_matched is year-wise matched canadian securites from Morningstar domestic holdings dataset with unique canadian securities from Xpressfeed NA;
*pn.global_funds4 is domestic holdings dataset;

*include only matched securities in the holdings dataset;
*(1) Global funds;
proc sql;
	create table pn.global_funds5 as
	select distinct a.*, b.*
	from pn.global_funds4 as a, pnd.table1_matched as b
	where a.isin=b.isin and year(a.date1)=b.year and missing(a.isin)=0
	order by a.fundid, a.date1, a.isin;
quit;
*11,884,827 obs from 77,301,200 obs;

proc sort data=pn.global_funds5 nodupkey out=pn.global_funds6; by fundid date1 isin domicile; run;

*Re-scale the weights;
proc sql;
  create table pn.global_funds6 as
  select distinct *, rescale_weighting2/sum(rescale_weighting2) as rescale_weighting3
  from pn.global_funds6
  group by FundId, Date1;
quit; 
*11,136,022 obs;

******************************************************************************************;
*create matched global and canadian securities files to extract fundamentals data from XPF;

/*proc sql;*/
/*	create table test as*/
/*	select distinct fundid, date1, sum(rescale_weighting3) as rescale_weighting3*/
/*	from pn.global_funds6*/
/*	group by fundid, date1;*/
/*quit;*/


*(2) Canada funds;
proc sql;
	create table pn.global_funds5a as
	select distinct a.*, b.*
	from pn.global_funds4 as a, pnd.table2_matched as b
	where a.cusip=b.cusip and year(a.date1)=b.year and missing(a.cusip)=0
	order by a.fundid, a.date1, a.cusip;
quit;
*3,061,418 obs from 77,301,200 obs;

proc sort data=pn.global_funds5a nodupkey out=pn.global_funds6a; by fundid date1 cusip domicile; run;
*2,408,558 obs;

*Re-scale the weights;
proc sql;
  create table pn.global_funds6a as
  select distinct *, rescale_weighting2/sum(rescale_weighting2) as rescale_weighting3
  from pn.global_funds6a
  group by FundId, Date1;
quit; 
*2,408,558 obs;

data pn.global_funds7;
	set pn.global_funds6 pn.global_funds6a;
	keep fundid domicile date1 isin cusip country NumberOfShare marketvalue excntry year gvkey mktcap forex_rate rescale_weighting3;
run;
*13,544,580 obs;

*************************************************************************
			Removing fund-months with less than 25 securities;
*************************************************************************;
proc sql;
	create table test as
	select distinct date1, fundid, count(distinct gvkey) as ncount
	from pn.global_funds7
	group by date1, fundid;
quit;

data test1;
	set test;
	if ncount>=25;
run;
*219,141 out of 338,148 obs;

proc sort data=pn.global_funds7; by fundid date1 rescale_weighting3; run;
*13,544,580 obs;

proc sql;
	create table pn.global_funds8 as
	select distinct a.*
	from pn.global_funds7 as a, test1 as b
	where a.fundid=b.fundid and a.date1=b.date1
	order by a.fundid, a.date1, a.rescale_weighting3;
quit;
*11,884,985 obs;


rsubmit;
/* Step 1.1. Calculate raw and industry-adjusted BM Ratio */
/* Step 1.1.1. Obtain Book-Equity from Compustat */
%let crspbegdate = '01Jan1979'd; *redfine crspbegdate to obtain BE for 1980;
data comp_extract;
   format gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc TXDITC BE0 BE1 EPSPX EBITDA SALE DVC CSHOI;
   set compg.g_funda
   (where=(fyr>0 and at>0 and consol='C' and
           indfmt='INDL' and datafmt='HIST_STD' and popsrc='I'));

   **BE as in Daniel and Titman (JF, 2006); 
   *Shareholder equity(she);
   if missing(SEQ)=0 then she=SEQ; 
   *SEQ=Stockholders Equity (Total);
   else if missing(CEQ)=0 and missing(PSTK)=0 then she=CEQ+PSTK; 
        *CEQ=Common/Ordinary Equity (Total), PSTK=Preferred/Preference Stock (Capital) (Total);
        else if missing(AT)=0 and missing(LT)=0 then she=AT-LT;
		     *AT=Assets (Total), LT=Liabilities (Total), MIB=Minority Interest (Balance Sheet); 
             else she=.;

   if missing(PSTKR)=0 then BE0=she-PSTKR; *PSTKRV=Preferred Stock (Redemption Value);
        else if missing(PSTK)=0 then BE0=she-PSTK; *PSTK=Preferred/Preference Stock (Capital) (Total);
             else BE0=.;
  
   **BE as in Kayhan and Titman (2003);
   *Book_Equity = Total Assets - [Total Liabilities + Preferred Stock] + Peferred Taxes;  
   if AT>0 and LT>0 then BE1=AT-(LT+PSTK)+TXDITC;
   else BE1=.;


   *Converts fiscal year into calendar year data;
   if (1<=fyr<=5) then date_fyend=intnx('month',mdy(fyr,1,fyear+1),0,'end');
   else if (6<=fyr<=12) then date_fyend=intnx('month',mdy(fyr,1,fyear),0,'end');
   calyear=year(date_fyend);
   format date_fyend date9.;

   *Accounting data since calendar year 't-1';
   if year(&crspbegdate)-1<=year(date_fyend)<=year(&crspenddate)+1;
   keep gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc TXDITC BE0 BE1 EPSPX EBITDA SALE DVC CSHOI;

   *Note that datadate is same as date_fyend;
run;
*547,788 obs;

/* Step 1.2 Calculate BE as in Daniel and Titman (JF, 2006) */
data comp_extract_be;
	set comp_extract;
	if missing(TXDITC)=0 then BE=BE0+TXDITC; 
    else BE=BE0;
run;
*For 2 fiscal year ends within the same cal year (for whatever reason), chose latest fiscal year end;
proc sql;
  create table comp_extract_be as
  select distinct *
  from comp_extract_be
  group by gvkey, calyear
  having fyr=max(fyr);
quit;
*546,014 obs;
Proc sort data=comp_extract_be nodupkey out=comp_extract_be1; by gvkey; run;

/* Step 1.3 Obtain Book to Market (BM) ratios by dividing with ME at December of calendar year */
proc sql;
   create table BM0  as 
   select distinct a.gvkey, a.calyear, b.datadate format=date9., a.BE*1000000 as BE, 
          a.BE*1000000/(abs(b.prccd)*b.cshoc) as BM
   from comp_extract_be as a, compg.g_secd  (where=(month(datadate)=12)) as b
   where a.gvkey=b.gvkey  and a.calyear=year(b.datadate) and 
         (abs(b.prccd)*b.cshoc)>0
   group by a.gvkey,a.calyear
   having b.datadate=max(b.datadate);
quit;
*476,546 obs;
 
proc sort data=BM0 nodupkey; by gvkey calyear; run;
*Keep only those cases with valid stock market size in June;
*Crsp stock size at June;
/*data temp.comp_extract_be; set comp_extract_be; run;*/
proc sql;
   create table size_june as 
   select distinct gvkey, year(datadate) as calyear, datadate format=date9., (prccd*cshoc) as size
   from compg.g_secd
   where year(&crspbegdate)<=year(datadate)<=year(&crspenddate) and month(datadate)=6
   group by gvkey, year(datadate)
   having datadate=max(datadate);
quit;
*716,900 obs;

proc sql;
   create table BM as 
   select distinct a.gvkey, a.calyear, a.datadate as decdate, a.bm label='BM (decdate)', 
                   b.datadate format=date9.
   from BM0 as a, size_june as b
   where a.gvkey=b.gvkey and intck('month',a.datadate,b.datadate)=6 and b.size>0;
quit;
*412,888 obs;
 
proc sort data=BM nodupkey; by gvkey calyear; run;
*Add date1 and date2 to BM;
data BM;
  set BM;
  date1=intnx("month",datadate,0,"E");
  date2=intnx("month",datadate,12,"E");
  format date1 date9. date2 date9.;
  drop datadate;
run;
data BM;
  format gvkey calyear decdate bm date1 date2;
  set BM;
  label date1='Date1 (BM valid from this date)';
  label date2='Date2 (BM valid to this date)';
run;

/* Step 1.4 Take log of BM */
data BM;
  set BM;
  if BM<0 then BM=0;
  LogBM=log(1+BM);
run;

/* Step 1.5 Winsorize LogBM at 1/99 */
data BM;
  set BM;
  decenddate=intnx("month",decdate,0,"E");
  format decenddate date9.;
run;
proc sort data=BM; by decenddate; run;
proc univariate data=BM noprint;
  where missing(LogBM)=0;
  by decenddate;
  var LogBM;
  output out=LogBMPctl pctlpts=0 1 2 98 99 100 pctlpre=LogBM;
quit;
/*
proc download data=BMPctl out=BMPctl; run;
PROC EXPORT DATA= BMPctl
            OUTFILE= "D:\Mutual Funds - Holding Classification\Exported Data\BMPctl.xls" 
            DBMS=EXCEL5 REPLACE;
RUN;
*/
proc sql;
  create table BM as
  select distinct a.*, b.LogBM1, b.LogBM99
  from BM as a, LogBMPctl as b
  where a.decenddate=b.decenddate;
quit;
data BM;
  set BM;
  if missing(LogBM)=0 and LogBM<LogBM1 then LogBM=LogBM1;
  if missing(LogBM)=0 and LogBM>LogBM99 then LogBM=LogBM99;
  drop LogBM1 LogBM99;
run;
data temp.BM; set BM; run;
endrsubmit;

/*data temp.bm; set bm; run;*/
*412,888 obs;


rsubmit;
/**********************************************************************/
/***Step 1.6 Calculate stock level returns, size, raw bm, ind-adj bm, momentum chrs  ***/
/**********************************************************************/
*create monthly returns file;
data g_secd; set compg.g_secd; run;
proc sort data=g_secd; by gvkey datadate; run;
data g_secd;
	set g_secd;
	by gvkey datadate;

	*A)Local currency returns;
	if first.gvkey=1 then ret=.;
	else ret=((((abs(prccd)/ajexdi)*trfd)/((abs(lag(prccd))/ lag(ajexdi))* lag(trfd)))-1);
run;

proc sql;
	create table g_secd as
	select distinct gvkey, datadate, ret
	from g_secd
	group by gvkey, month(datadate), year(datadate)
	having datadate=max(datadate);
quit; 

/* Step 1.7. Extract stocks at the end of each month and obtain log(size) */
proc sql;
  create table CRSP_StkQtrs_Size as
  select distinct intnx("month",datadate,0,"E") as month format=date9. label='Month End Date', gvkey, 
                  log(prccd*cshoc/1000000) as LogSize label='Log of Size ($Million)'
  from compg.g_secd
  group by gvkey, year(datadate), month(datadate)
  having datadate=max(datadate);
quit;
*8,824,998 obs;

/* Step 1.8 calculate momentum */
data CRSP_StkQtrs_Mom;
  set CRSP_StkQtrs_Size;
  date_12=intnx("month",month,-12,"E");
  format date_12 date9.;
run;  

proc sql;
  create table CRSP_StkQtrs_Mom as
  select distinct a.*, b.datadate format date9., b.ret
  from CRSP_StkQtrs_Mom as a, g_secd as b
  where a.gvkey=b.gvkey and a.date_12<b.datadate<a.month and missing(b.ret)=0;
  *Past 11 months return exluding the quarter ending month;
  *112,554,458 obs;

  create table CRSP_StkQtrs_Mom as
  select distinct month, gvkey, exp(sum(log(1+ret))) - 1 as Mom label='Prior 11 Month CumRet'
  from CRSP_StkQtrs_Mom
  group by month, gvkey
  having count(datadate)>=10; *at least 10 months of ret should be there;
  *7,446,692 obs;
quit;

*Winsorize Mom at 1/99 percentile;
proc univariate data=CRSP_StkQtrs_Mom noprint;
  by month;
  var Mom;
  output out=MomPctl pctlpts=1 99 pctlpre=Mom;
quit;
*138 obs;
proc sql;
  create table CRSP_StkQtrs_Mom as
  select distinct a.*, b.Mom1, b.Mom99
  from CRSP_StkQtrs_Mom as a, MomPctl as b
  where a.month=b.month;
quit;
*7,446,692 obs;

data CRSP_StkQtrs_Mom;
  set CRSP_StkQtrs_Mom;
  if missing(Mom)=0 and Mom<Mom1 then Mom=Mom1;
  if missing(Mom)=0 and Mom>Mom99 then Mom=Mom99;
  drop Mom1 Mom99;
run;

data temp.CRSP_StkQtrs_Mom; set CRSP_StkQtrs_Mom; run;
data temp.CRSP_StkQtrs_Size; set CRSP_StkQtrs_Size; run;


/* Step 1.9 calculate BM */
proc sql;
  create table CRSP_StkQtrs_BM as
  select distinct a.month, a.gvkey, b.BM, b.LogBM
  from CRSP_StkQtrs_Size as a left join BM as b
  on a.gvkey=b.gvkey and b.date1<a.month<=b.date2;
quit;
*8,198,941 obs;


/* Step 1.10. Consolidate all characteristics in a single dataset */
proc sql;
  create table CRSP_StkQtrs_Chrs as
  select distinct a.*, b.bm as BM, b.LogBM
  from CRSP_StkQtrs_Size as a left join CRSP_StkQtrs_BM as b
  on a.month=b.month and a.gvkey=b.gvkey;
  *8826048 obs;

  create table CRSP_StkQtrs_Chrs as
  select distinct a.*, b.Mom
  from CRSP_StkQtrs_Chrs as a left join CRSP_StkQtrs_Mom as b
  on a.month=b.month and a.gvkey=b.gvkey;
  *8826048 obs;
quit;
proc sort data=CRSP_StkQtrs_Chrs nodupkey; by month gvkey; run;
*8198941 obs;
data ash.CRSP_StkQtrs_Chrs_global; set CRSP_StkQtrs_Chrs; run;
endrsubmit;
/*proc sql;*/
/*  drop table BM0, Comp_Extract, Comp_Extract_BE, CRSP_StkQtrs_BM, CRSP_StkQtrs_DivYield, CRSP_StkQtrs_Mom, CRSP_StkQtrs_Size, */
/*             DivYield, DYPctl, MedLogBM, LogBMPctl, MomPctl, Size_June;*/
/*quit; */


/************************************************************************************************************************/
/************************************************************************************************************************/
/*************************************** METHOD 4: Z-SCORES WITH ORTHOGONALIZATION **************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/


/****************************************************************/
/********************* Stock Level 3D Vector ********************/
/****************************************************************/

rsubmit;
*** Step 2.1. Obtain stock characteristics, i.e. use CRSP_StkQtrs_Chrs;
*** Step 2.2. Keep firm with non-missing LogSize and LogBM;
data CRSP_StkQtrs_Chrs_3D_ZS;
  set ash.CRSP_StkQtrs_Chrs_global;
  if missing(LogSize)=0 and missing(LogBM)=0 and missing(Mom)=0;
  keep month gvkey LogSize LogBM Mom;
run;
*4,778,994 obs;

*** Step 2.3. Standardize LogSize, LogBM and Mom;
proc sort data=CRSP_StkQtrs_Chrs_3D_ZS nodupkey; by month gvkey; run;
proc univariate data=CRSP_StkQtrs_Chrs_3D_ZS noprint;
  by month;
  var LogSize LogBM Mom;
  output out=Temp Mean=Mean_LogSize Mean_LogBM Mean_Mom Std=Std_LogSize Std_LogBM Std_Mom;
quit;
*Calculate standardized variables for ALL stocks;
proc sql;
  create table CRSP_StkQtrs_Chrs_3D_ZS as
  select distinct a.month, a.gvkey, 
                  (a.LogSize-b.Mean_LogSize)/b.Std_LogSize as LogSize, 
                  (a.LogBM-b.Mean_LogBM)/b.Std_LogBM as LogBM,
                  (a.Mom-b.Mean_Mom)/b.Std_Mom as Mom  
  from CRSP_StkQtrs_Chrs_3D_ZS as a, Temp as b
  where a.month=b.month;
quit;



*** Step 2.4. Regress and take the residuals;
proc sort data=CRSP_StkQtrs_Chrs_3D_ZS; by month; run;
proc reg data=CRSP_StkQtrs_Chrs_3D_ZS noprint outest=Parms1_NYSE_3D; 
  by month; 
  model LogBM = LogSize; 
  output out=CRSP_StkQtrs_Chrs_3D_ZS R=rLogBM;
quit;
proc reg data=CRSP_StkQtrs_Chrs_3D_ZS noprint outest=Parms2_NYSE_3D; 
  by month; 
  model Mom = LogSize LogBM;
  output out=CRSP_StkQtrs_Chrs_3D_ZS R=rMom;
quit;



*** Step 2.5. Predicted and residual, rLogBM, rMom for AMEX and NASD firms;
proc sql;
  create table CRSP_StkQtrs_Chrs_3D_ZS as
  select distinct a.*, a.LogBM - (b.Intercept + (b.LogSize*a.LogSize)) as rLogBM, a.Mom - (c.Intercept + (c.LogSize*a.LogSize) + (c.LogBM*a.LogBM)) as rMom 
  from CRSP_StkQtrs_Chrs_3D_ZS as a, Parms1_NYSE_3D as b, Parms2_NYSE_3D as c
  where a.month=b.month=c.month;
quit; 
proc sort data=CRSP_StkQtrs_Chrs_3D_ZS nodupkey; by month gvkey; run;
data ash.CRSP_StkQtrs_Chrs_3D_ZS_Global; set CRSP_StkQtrs_Chrs_3D_ZS; run;
*4,778,994 obs;
endrsubmit;



/***************************************************************/
/********************* Fund Level Vectors ********************/
/***************************************************************/

/*********************** 3D Vector *******************/
*** Rescale weights of quartely portfolios for stocks with non-missing LogSize, rLogBM, rMom;
proc sql;
  create table Fund_Chrs_3D_ZS as
  select distinct a.fundid, a.domicile, a.date1, a.gvkey, a.rescale_weighting3, b.LogSize, b.rLogBM, b.rMom
  from pn.global_funds7 as a, pn.CRSP_StkQtrs_Chrs_3D_ZS_Global as b
  where a.gvkey=b.gvkey and a.date1=b.month and 
        missing(a.rescale_weighting3)=0 and missing(b.LogSize)=0 and missing(b.rLogBM)=0 and missing(b.rMom)=0;
  *8,387,289 obs;

  *Rescale weights;
  create table Fund_Chrs_3D_ZS as
  select distinct fundid, domicile, date1, gvkey, rescale_weighting3/sum(rescale_weighting3) as wt_rdate_rescaled, LogSize, rLogBM, rMom
  from Fund_Chrs_3D_ZS
  group by fundid, date1;
quit;


*** Obtain Fund-level 3D Vector = [LogSize, rLogBM, rMom];
proc sql;
  create table Fund_Chrs_3D_ZS as
  select distinct fundid, domicile, date1, count(distinct gvkey) as nonmiss_nstk, 
                  sum(wt_rdate_rescaled*LogSize) as LogSize label='Fund Level LogSize',
                  sum(wt_rdate_rescaled*rLogBM) as rLogBM label='Fund Level rLogBM',
				  sum(wt_rdate_rescaled*rMom) as rMom label='Fund Level rMom'
  from Fund_Chrs_3D_ZS
  group by fundid, date1;
  *268,027 obs;
quit;



***Copy to pn;
data pn.Tfnfund_Chrs_Qtrly_ZS_3D_Global; set Fund_Chrs_3D_ZS; run;
*268,027 obs;









*************************************************************************************************************************
													Sanity check
*************************************************************************************************************************;
proc sql;
	create table test as
	select distinct domicile, date1, count(distinct fundid) as ncount
	from pn.global_funds7
	group by date1, domicile;
quit; 

proc sql;
	create table test1 as
	select distinct domicile, date1, count(distinct fundid) as ncount
	from pn.global_funds8
	group by date1, domicile;
quit; 

proc sort data=test; by domicile date1; run;
proc sort data=test1; by domicile date1; run;
