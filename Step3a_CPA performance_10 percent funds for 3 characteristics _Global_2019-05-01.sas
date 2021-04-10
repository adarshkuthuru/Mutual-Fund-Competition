proc datasets library= work kill nolist; run; quit;

libname FCSep29 'E:\Drive\Local Disk F\Mutual fund competition'; run;
libname mfglobal 'E:\Drive\Local Disk F\Mutual fund competition\Global'; run;
libname MF 'G:\MF competition'; run;
libname MFglobal 'G:\MF competition\Global'; run;

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
libname pn '/wrds/comp/sasdata/naa/pension'; run; *compustat pension;
libname ash '/home/isb/adarshkp/Mutual fund comp'; run;

*Specify CRSP begin and end dates;
%let crspbegdate = '01Jan1980'd;
%let crspenddate = '31JAN2019'd; 

*Specify Thomson-Reuters begin and end dates;
%let tfnbegdate = '01Jan1980'd;
%let tfnenddate = '31JAN2019'd; 
endrsubmit;


**************************************************************************************;
		*Map Global fund characteristics (including returns) to Fundid and domicile;
**************************************************************************************;

***Import xlsx;
PROC IMPORT OUT= pn.fund_mapping
            DATAFILE= "G:\MF competition\Global\MstarID Mapping.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

proc sql;
	create table pn.fund_mapping as
	select distinct fundid, domicile
	from pn.fund_mapping
	group by fundid;
quit;
proc sort data=pn.fund_mapping nodupkey; by fundid; run;
*0 duplicates;

data pn.fund_mapping;
	set pn.fund_mapping;
	if missing(fundid)=1 then delete;
	domicile=upcase(domicile);
run;

**Import csv;
PROC IMPORT OUT= pn.Global_fund_chrs_monthly
            DATAFILE= "G:\MF competition\Global\mstar_chars_global.csv" 
            DBMS=CSV REPLACE;
RUN;

proc sql;
	create table pn.Global_fund_chrs_monthly as
	select distinct fundid, date, mean(mret_afxp) as mret_afxp, mean(mret_bexp) as mret_bexp
	from pn.Global_fund_chrs_monthly
	group by fundid, date;
quit;
*4,912,006 obs;
proc sort data=pn.Global_fund_chrs_monthly nodupkey; by fundid date; run;

proc sql;
	create table pn.Global_fund_chrs_monthly as
	select distinct a.*, b.domicile
	from pn.Global_fund_chrs_monthly as a left join pn.fund_mapping as b
	on a.fundid=b.fundid
	group by a.fundid, a.date;
quit;

data pn.Global_fund_chrs_monthly;
	set pn.Global_fund_chrs_monthly;
	if missing(mret_afxp)=1 then delete;
run;
*3,470,579 obs;

**copy files to work directory;
data Global_fund_chrs_monthly; set pn.Global_fund_chrs_monthly; run;
data ff; set mfglobal.ff; run;
/*data mf.tfnfund_chrs_qtrly_zs_3d3; */
/*	set mf.tfnfund_chrs_qtrly_zs_3d3; 		*/
/*	nqdate=intnx('qtr',qdate,1,'E');*/
/*	next_nqdate=intnx('qtr',qdate,2,'E');*/
/*	date_3m=intnx('month',date,3,'E');*/
/*	format nqdate date9. next_nqdate date9. date_3m date9.;	*/
/*run;*/

*Generate month end date for the time-period;
%let start_date=31JAN1980;
%let end_date=31MAR2019;

data month;
date="&start_date"d;
do while (date<="&end_date"d);
    output;
    date=intnx('month', date, 1, 'E');
end;
format date date9.;
run;

**********************************************************************************************************************
												2018-11-28
						Identifying 25 Rival funds for each of the fund-quarter combination
**********************************************************************************************************************;
*List of unique domiciles;
proc sql;
	create table pn.country as
	select distinct domicile
	from pn.tfnfund_chrs_qtrly_zs_3d_global
	group by domicile;
quit;

data pn.country;
	set pn.country;
	n=_N_;
run;

proc sql;
	create table pn.tfnfund_chrs_qtrly_zs_3d_global as
	select distinct a.*, b.n
 	from pn.tfnfund_chrs_qtrly_zs_3d_global as a left join pn.country as b
	on a.domicile=b.domicile;
quit;

proc sql noprint;
        select count(*) into :num from pn.country;
quit;

options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit;
%do i=2 %to &num;

data tfnfund_chrs_qtrly_zs_3d;
	set pn.tfnfund_chrs_qtrly_zs_3d_global;
	if n=&i;
run;

proc sort data=tfnfund_chrs_qtrly_zs_3d; by date1 fundid; run;


*get rivals coordinates for each fund by quarter;
proc sql;
	create table tfnfund_chrs_qtrly_zs_3d2 as
	select distinct a.date1, a.fundid, b.fundid as rivfundid, a.logsize, a.rlogbm, a.rmom,
	b.logsize as logsize1, b.rlogbm as rLogBM1, b.rmom as rMom1
	from tfnfund_chrs_qtrly_zs_3d as a left join tfnfund_chrs_qtrly_zs_3d as b
	on a.date1=b.date1
	order by a.date1, a.fundid, b.fundid;
quit;

*estimate euclidean distance;
data tfnfund_chrs_qtrly_zs_3d2;
	set tfnfund_chrs_qtrly_zs_3d2;
	d=sqrt(sum((sum(logsize,-logsize1)**2),(sum(rlogbm,-rlogbm1)**2),(sum(rMom,-rMom1)**2)));
	ndate1=intnx('month',date1,1,'E');
	next_ndate1=intnx('month',date1,2,'E');
	format ndate1 date9. next_ndate1 date9.;
run;
proc sort data=tfnfund_chrs_qtrly_zs_3d2; by ndate1 fundid d; run;

*count the number of rival funds for each fund by each month;
data tfnfund_chrs_qtrly_zs_3d2;
	set tfnfund_chrs_qtrly_zs_3d2;
	by date1 fundid;
	if first.fundid=1 then count=0;
	count+1;
run;

data tfnfund_chrs_qtrly_zs_3d2;
	set tfnfund_chrs_qtrly_zs_3d2;
	count1=count-1;
run;

**********************************************************************************************************************
									Estimating the peer fraction of funds
**********************************************************************************************************************;

proc sql;
	create table peer_fraction as
	select distinct ndate1, count(distinct fundid) as nfunds, round(count(distinct fundid)/10,1) as tenperc_nfunds
	from tfnfund_chrs_qtrly_zs_3d2
	group by ndate1;
quit;

proc sql;
	create table tfnfund_chrs_qtrly_zs_3d3 as
	select distinct a.*, b.tenperc_nfunds
	from tfnfund_chrs_qtrly_zs_3d2 as a, peer_fraction as b
	where a.ndate1=b.ndate1 and a.count1<=b.tenperc_nfunds and a.count1>0;
quit;

/*proc sort data=tfnfund_chrs_qtrly_zs_3d3; by ndate1 fundid count1; run;*/
/**/
/*proc sql;*/
/*create table tfnfund_chrs_qtrly_zs_3d3 as*/
/*select distinct a.*, b.tenperc_nfunds*/
/*from tfnfund_chrs_qtrly_zs_3d3 as a left join peer_fraction as b*/
/*on a.date1=b.date1*/
/*group by a.date1;*/
/*quit;*/
/**/
/*data tfnfund_chrs_qtrly_zs_3d3;*/
/*  set tfnfund_chrs_qtrly_zs_3d3;*/
/*if count1<=tenperc_nfunds;*/
/*if count1=0 then delete;*/
/*run;*/


proc sort data=tfnfund_chrs_qtrly_zs_3d3 nodupkey; by ndate1 fundid rivfundid; run;
*0 obs deleted;


**********************************************************************************************************************
						Estimation of customized peer alpha (CPA)
**********************************************************************************************************************;


*Expand quarterly dataset to monthly;
proc sql;
	create table CPA as
	select distinct a.ndate1,a.fundid,a.rivfundid,a.next_ndate1,b.date, max(a.d) as maxdis
	from tfnfund_chrs_qtrly_zs_3d3 as a left join month as b
	on a.ndate1<b.date and b.date<=a.next_ndate1
	group by a.ndate1,a.fundid
	order by a.ndate1, a.fundid, b.date, a.d;
quit;


*include rivwficn returns before expenses data from CRSP fund chrs dataset;
proc sql;
	create table CPA1 as
	select distinct a.*, b.mret_bexp as rivwficnret
	from cpa as a left join Global_fund_chrs_monthly as b
	on a.date=b.date and a.rivfundid=b.fundid
	order by b.date, a.fundid, a.rivfundid;
quit;

*Estimate avg ret of rival funds;
proc sql;
	create table cpa1 as
	select distinct ndate1, date, fundid, mean(rivwficnret) as avgretrivwficn, maxdis
	from cpa1
	group by ndate1, date, fundid
	order by ndate1, date, fundid;
quit;

*Include wficn returns;
proc sql;
	create table CPA1 as
	select distinct a.*, b.mret_bexp as wficnret, sum(b.mret_bexp,-a.avgretrivwficn) as CPA10
	from cpa1 as a, Global_fund_chrs_monthly as b
	where a.date=b.date and a.fundid=b.fundid and missing(b.mret_bexp)=0 and missing(a.avgretrivwficn)=0
	order by a.ndate1, a.date, a.fundid;
quit;

data cpa1;
	set cpa1;
	date1=intnx('month',date,1,'E');
	past12monthdate=intnx('month',date,-12,'E');
	format date1 date9. past12monthdate date9.;
run;


*Include wficn returns for next month;
/*proc sql;*/
/*	create table CPA1 as*/
/*	select distinct a.*, b.mret_bexp as wficnret1*/
/*	from cpa1 as a left join crspfund_chrs_monthly as b*/
/*	on a.date1=b.date and a.wficn=b.wficn*/
/*	order by a.nqdate, a.date, a.wficn;*/
/*quit;*/
*479320 obs;

**Past 12-month avg CPA estimation by fund & date;
proc sql;
  create table CPA2 as
  select distinct a.ndate1, a.date, a.fundid, a.past12monthdate, b.CPA10, b.date as date_12m
  from CPA1 as a, CPA1 as b
  where a.fundid=b.fundid and a.past12monthdate<b.Date<=a.Date
  group by a.fundid, a.date
/*  order by a.wficn, a.date, b.date*/
  having count(distinct b.Date)=12; *Require non-missing CPA for all 12 months;
quit;

proc sql;
	create table cpa2 as
	select distinct fundid, date, mean(CPA10) as CPA10
	from CPA2
	group by fundid, date;
quit;

*Include avg CPA in main dataset;
proc sql;
	create table CPA1 as
	select distinct a.*, b.CPA10 as Avg_CPA10
	from cpa1 as a, cpa2 as b
	where a.date=b.date and a.fundid=b.fundid
	order by a.ndate1, a.date, a.fundid;
quit;

*Include fund returns for next month;
proc sql;
	create table CPA1 as
	select distinct a.*, b.mret_bexp as wficnret1
	from cpa1 as a, Global_fund_chrs_monthly as b
	where a.date1=b.date and a.fundid=b.fundid and missing(b.mret_bexp)=0
	order by a.ndate1, a.date1, a.fundid;
quit;


*********************************************************************************************************************
									*Performance of CPA25 factor, Form Portfolios;
********************************************************************************************************************;


	***Decile groups;
proc sort data=CPA1; by date Avg_CPA10; run;
proc rank data=CPA1 groups=10 out=CPA1;
  by date;
  var Avg_CPA10;
  ranks Rank_CPA10;
run;
data CPA1;
  set CPA1;
  Rank_CPA10=Rank_CPA10+1;
run;

data CPA1;
  set CPA1;
  next_ndate1=intnx('month',ndate1,1,'E');
  format next_ndate1 date9.;
run;

*Estimate avg ret of portfolios by date;
proc sql;
	create table cpa2 as
	select distinct ndate1, date, date1, next_ndate1, Rank_CPA10, mean(wficnret1) as CPA10ret
	from cpa1
	group by ndate1, date, Rank_CPA10
	order by ndate1, date, Rank_CPA10;
quit;

*Hedge portfolios;
proc sql;
  create table HedgePort1 as
  select distinct a.ndate1, a.date, a.date1, 11 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=10)) as a, cpa2(where=(Rank_CPA10=1)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort2 as
  select distinct a.ndate1, a.date, a.date1, 12 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=10)) as a, cpa2(where=(Rank_CPA10=2)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort3 as
  select distinct a.ndate1, a.date, a.date1, 13 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=10)) as a, cpa2(where=(Rank_CPA10=3)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort4 as
  select distinct a.ndate1, a.date, a.date1, 14 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=9)) as a, cpa2(where=(Rank_CPA10=1)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort5 as
  select distinct a.ndate1, a.date, a.date1, 15 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=9)) as a, cpa2(where=(Rank_CPA10=2)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort6 as
  select distinct a.ndate1, a.date, a.date1, 16 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=9)) as a, cpa2(where=(Rank_CPA10=3)) as b
  where a.date=b.date;        
quit;

**Append hedge portfolios data to cpa2;
proc append data=HedgePort1 base=cpa2; run;
proc append data=HedgePort2 base=cpa2; run;
proc append data=HedgePort3 base=cpa2; run;
proc append data=HedgePort4 base=cpa2; run;
proc append data=HedgePort5 base=cpa2; run;
proc append data=HedgePort6 base=cpa2; run;
*7072 obs;

	/*** Full Sample Results ***/
	***Mean, STD, T-stat;
proc sort data=cpa2; by Rank_CPA10; run;
proc means data=cpa2 noprint;
  by Rank_CPA10;
  var CPA10ret;
  output out=MeanRet mean=CPA10ret;
quit;
proc means data=cpa2 noprint;
  by Rank_CPA10;
  var CPA10ret;
  output out=TstatRet t=CPA10ret;
quit;

data MeanRet;
  set MeanRet;
  drop _Type_ _Freq_ var stat;
  CPA10ret=CPA10ret*12*100;
  rename CPA10ret=CPAret;
run;
data TstatRet;
  set TstatRet;
  drop _Type_ _Freq_ var stat;
  rename CPA10ret=CPAret_Tstat;
run;

	***Alpha;
	**Import csv;
/*PROC IMPORT OUT= mfglobal.FF*/
/*            DATAFILE= "G:\MF competition\Global\Global_5_Factors.csv" */
/*            DBMS=CSV REPLACE;*/
/*RUN;*/

proc sql;
	create table cpa2 as
	select distinct a.*, b.*
	from cpa2 as a, ff as b
	where month(a.date1)=b.month and year(a.date1)=b.year;
quit;

/*data cpa2;*/
/*	set cpa2;*/
/*	drop mktrf rf exret;*/
/*run;*/

data cpa2;
	set cpa2;
	if Rank_CPA10<=10 then Exret=CPA10ret-rf;
	else Exret=CPA10ret;
run;

proc sort data=cpa2; by Rank_CPA10 date; run;

proc reg data=cpa2 noprint tableout outest=Alpha;
  by Rank_CPA10;
  model Exret = MKTRF;
  model Exret = MKTRF SMB HML; *3-factor;
  model Exret = MKTRF SMB HML RMW CMA; *5-factor;
  model Exret = MKTRF SMB HML RMW CMA MOM; *5-factor and MOM;
quit;

data Alpha;
  set Alpha;
  where _Type_ in ('PARMS','PVALUE');
  keep _model_ Rank_CPA10 _Type_ Intercept;
  if _Type_='PARMS' then Intercept=Intercept*12*100;
  rename Intercept=CPA10ret;
  rename _Type_ =Stat;
run;
data Alpha;
  set Alpha;
  if Stat='PARMS' then Var=3;
  if Stat='PVALUE' then Var=4;
  drop _label_ _name_;
run;

*converting long to wide form;
proc sort data=Alpha; by Rank_CPA10 _model_; run;
proc transpose data=Alpha out=Alpha;
  by Rank_CPA10; *only one year per column: will be converted to row;
  var CPA10ret;
  id _model_ Var Stat;
run;
data Alpha;
  set Alpha;
  drop _label_ _name_;
run;

***Collect all results;
proc sql;
	create table teststats as
	select distinct a.*,b.*
	from meanret as a left join tstatret as b
	on a.rank_CPA10=b.rank_CPA10;
quit;

proc sql;
	create table teststats as
	select distinct a.*,b.*
	from teststats as a left join alpha as b
	on a.rank_CPA10=b.rank_CPA10;
quit;

data teststats;
	set teststats;
	n=&i;
run;

proc append data=teststats base=mfglobal.teststats; run;
%end;
%mend doit;
%doit
