proc datasets library= work kill nolist; run; quit;

libname FCSep29 'E:\Drive\Local Disk F\Mutual fund competition'; run;
libname mfglobal 'E:\Drive\Local Disk F\Mutual fund competition\Global'; run;
libname MF 'G:\MF competition'; run;

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

**********************************************************************************************************************
												2018-11-28
						Identifying 25 Rival funds for each of the fund-quarter combination
**********************************************************************************************************************;

proc sort data=mf.tfnfund_chrs_qtrly_zs_3d; by qdate wficn; run;
*173578 obs;

/*data mf.sample;*/
/*	set mf.tfnfund_chrs_qtrly_zs_3d(firstobs=1 obs=10);*/
/*run;*/
/**/
/*proc sql;*/
/*	create table mf.sample1 as*/
/*	select distinct a.qdate, a.wficn, b.wficn as rivwficn, a.logsize, a.rlogbm, a.rmom,*/
/*	b.logsize as logsize1, b.rlogbm as rLogBM1, b.rmom as rMom1*/
/*	from mf.sample as a left join mf.sample as b*/
/*	on a.qdate=b.qdate*/
/*	order by a.qdate, a.wficn, b.wficn;*/
/*quit;*/


/*data mf.sample1;*/
/*	set mf.sample1;*/
/*	d=sqrt(sum((sum(logsize,-logsize1)**2),(sum(rlogbm,-rlogbm1)**2),(sum(rMom,-rMom1)**2)));*/
/*run;*/
/*proc sort data=mf.sample1; by qdate wficn d; run;*/
/*data mf.sample1;*/
/*	set mf.sample1;*/
/*	by wficn qdate;*/
/*	if first.wficn=1 then count=0;*/
/*	count+1;*/
/*run;*/

*get rivals' coordinates for each fund by quarter;
proc sql;
	create table mf.tfnfund_chrs_qtrly_zs_3d2 as
	select distinct a.qdate, a.wficn, b.wficn as rivwficn, a.logsize, a.rlogbm, a.rmom,
	b.logsize as logsize1, b.rlogbm as rLogBM1, b.rmom as rMom1
	from mf.tfnfund_chrs_qtrly_zs_3d as a left join mf.tfnfund_chrs_qtrly_zs_3d as b
	on a.qdate=b.qdate
	order by a.qdate, a.wficn, b.wficn;
quit;
*256924854 obs;

*estimate euclidean distance;
data mf.tfnfund_chrs_qtrly_zs_3d2;
	set mf.tfnfund_chrs_qtrly_zs_3d2;
	d=sqrt(sum((sum(logsize,-logsize1)**2),(sum(rlogbm,-rlogbm1)**2),(sum(rMom,-rMom1)**2)));
	nqdate=intnx('qtr',qdate,1,'E');
	next_nqdate=intnx('qtr',qdate,2,'E');
	format nqdate date9. next_nqdate date9.;
run;
proc sort data=mf.tfnfund_chrs_qtrly_zs_3d2; by nqdate wficn d; run;

*count the number of rival funds for each fund by each quarter;
data mf.tfnfund_chrs_qtrly_zs_3d2;
	set mf.tfnfund_chrs_qtrly_zs_3d2;
	by qdate wficn;
	if first.wficn=1 then count=0;
	count+1;
run;
*256,924,854 obs;

data mf.tfnfund_chrs_qtrly_zs_3d2;
	set mf.tfnfund_chrs_qtrly_zs_3d2;
	count1=count-1;
run;
*256,924,854 obs;

**********************************************************************************************************************
									Estimating the peer fraction of funds
**********************************************************************************************************************;

proc sql;
	create table mf.peer_fraction as
	select distinct nqdate, count(distinct wficn) as nfunds, round(count(distinct wficn)/10,1) as tenperc_nfunds
	from mf.tfnfund_chrs_qtrly_zs_3d2
	group by nqdate;
quit;

proc sql;
	create table mf.tfnfund_chrs_qtrly_zs_3d3 as
	select distinct a.*, b.tenperc_nfunds
	from mf.tfnfund_chrs_qtrly_zs_3d2 as a, mf.peer_fraction as b
	where a.nqdate=b.nqdate and a.count1<=b.tenperc_nfunds and a.count1>0;
quit;
*25700106 obs;
proc sort data=mf.tfnfund_chrs_qtrly_zs_3d3; by nqdate wficn count1; run;

**Splitting datasets into subsets before mapping;
%macro split1(num);
data _null_;
 if 0 then set mf.tfnfund_chrs_qtrly_zs_3d2 nobs=count;
 call symput('numobs',put(count,8.));
run;
%let m=%sysevalf(&numobs/&num,ceil);
data %do J=1 %to &m ; filename_&J %end; ;
set mf.tfnfund_chrs_qtrly_zs_3d2;
 %do I=1 %to &m;
 if %eval(&num*(&i-1)) <_n_ <=
 %eval(&num*&I) then output filename_&I;
%end;
run;
%mend split1;

%split1(10000000)
*10000000 is the desired number of observations in the individual datasets;

/* Counting the number of datasets in the folder */

*Run a PROC CONTENTS to create a SAS data set with the names of the SAS data sets in the SAS data  library;
proc contents data=_all_ out=cont(keep=memname) noprint; run;


*Eliminate any duplicate names of the SAS data set names stored in the SAS data set;
proc sort data=cont nodupkey; by memname; run;
*26 obs;

*Run a DATA _NULL_ step to create 2 macro variables: one with the names of each SAS data set and 
the other with the final count of the number of SAS data sets;
data _null_;
  set cont end=last;
  by memname;
  i+1;
  call symputx('name'||trim(left(put(i,8.))),memname);
  if last then call symputx('count',i);
run;


**Macro to filter the top 10% funds;
%macro mpp;
  %do i=1 %to &count;
	  proc sql;
		create table filename_&i as
		select distinct a.*, b.tenperc_nfunds
		from filename_&i as a left join mf.peer_fraction as b
		on a.qdate=b.qdate
		group by a.qdate;
	  quit;

	  data filename_&i;
	  	set filename_&i;
		if count1<=tenperc_nfunds;
		if count1=0 then delete;
      run;
/*    proc append base=TSBeta data=NK.&&name&i force; run;*/
  %end;
%mend mpp;

*Call macro;
%mpp;

/* Create a consolidated dataset */
*Macro containing the PROC APPEND that executes for each SAS data set you want to concatenate together to create 1 SAS data set;
%macro combdsets;
  %do i=1 %to &count;
    proc append base=mf.tfnfund_chrs_qtrly_zs_3d3 data=filename_&i force; run;
  %end;
%mend combdsets;

*Call macro;
%combdsets;
*25,699,872 obs;

proc sort data=mf.tfnfund_chrs_qtrly_zs_3d3 nodupkey; by nqdate wficn rivwficn; run;
*0 obs deleted;
**********************************************************************************************************************
						Estimation of customized peer alpha (CPA)
**********************************************************************************************************************;

**copy files to work directory;
data crspfund_chrs_monthly; set mf.crspfund_chrs_monthly; run;
data ff; set mf.ff; run;
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

*Expand quarterly dataset to monthly;
proc sql;
	create table CPA as
	select distinct a.nqdate,a.wficn,a.rivwficn,a.next_nqdate,b.date, max(a.d) as maxdis
	from mf.tfnfund_chrs_qtrly_zs_3d3 as a left join month as b
	on a.nqdate<b.date and b.date<=a.next_nqdate
	group by a.nqdate,a.wficn
	order by a.nqdate, a.wficn, b.date, a.d;
quit;
*77100318 obs;


*include rivwficn returns before expenses data from CRSP fund chrs dataset;
proc sql;
	create table CPA1 as
	select distinct a.*, b.mret_bexp as rivwficnret
	from cpa as a left join crspfund_chrs_monthly as b
	on a.date=b.date and a.rivwficn=b.wficn
	order by b.date, a.wficn,a.rivwficn;
quit;
*77100318 obs;

*Estimate avg ret of rival funds;
proc sql;
	create table cpa1 as
	select distinct nqdate, date, wficn, mean(rivwficnret) as avgretrivwficn, maxdis
	from cpa1
	group by nqdate, date, wficn
	order by nqdate, date, wficn;
quit;
*520734 obs;

*Include wficn returns;
proc sql;
	create table CPA1 as
	select distinct a.*, b.mret_bexp as wficnret, sum(b.mret_bexp,-a.avgretrivwficn) as CPA10
	from cpa1 as a, crspfund_chrs_monthly as b
	where a.date=b.date and a.wficn=b.wficn and missing(b.mret_bexp)=0 and missing(a.avgretrivwficn)=0
	order by a.nqdate, a.date, a.wficn;
quit;
*479320 obs;

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
  select distinct a.nqdate, a.date, a.wficn, a.past12monthdate, b.CPA10, b.date as date_12m
  from CPA1 as a, CPA1 as b
  where a.wficn=b.wficn and a.past12monthdate<b.Date<=a.Date
  group by a.wficn, a.date
/*  order by a.wficn, a.date, b.date*/
  having count(distinct b.Date)=12; *Require non-missing CPA for all 12 months;
quit;
*4987848 obs;

proc sql;
	create table cpa2 as
	select distinct wficn, date, mean(CPA10) as CPA10
	from CPA2
	group by wficn, date;
quit;
*415654 obs;

*Include avg CPA in main dataset;
proc sql;
	create table CPA1 as
	select distinct a.*, b.CPA10 as Avg_CPA10
	from cpa1 as a, cpa2 as b
	where a.date=b.date and a.wficn=b.wficn
	order by a.nqdate, a.date, a.wficn;
quit;
*415654 obs;

*Include wficn returns for next month;
proc sql;
	create table CPA1 as
	select distinct a.*, b.mret_bexp as wficnret1
	from cpa1 as a, crspfund_chrs_monthly as b
	where a.date1=b.date and a.wficn=b.wficn and missing(b.mret_bexp)=0
	order by a.nqdate, a.date1, a.wficn;
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
  next_nqdate=intnx('qtr',nqdate,1,'E');
  format next_nqdate date9.;
run;

*Estimate avg ret of portfolios by date;
proc sql;
	create table cpa2 as
	select distinct nqdate, date, date1, next_nqdate, Rank_CPA10, mean(wficnret1) as CPA10ret
	from cpa1
	group by nqdate, date, Rank_CPA10
	order by nqdate, date, Rank_CPA10;
quit;

*Hedge portfolios;
proc sql;
  create table HedgePort1 as
  select distinct a.nqdate, a.date, a.date1, 11 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=10)) as a, cpa2(where=(Rank_CPA10=1)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort2 as
  select distinct a.nqdate, a.date, a.date1, 12 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=10)) as a, cpa2(where=(Rank_CPA10=2)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort3 as
  select distinct a.nqdate, a.date, a.date1, 13 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=10)) as a, cpa2(where=(Rank_CPA10=3)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort4 as
  select distinct a.nqdate, a.date, a.date1, 14 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=9)) as a, cpa2(where=(Rank_CPA10=1)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort5 as
  select distinct a.nqdate, a.date, a.date1, 15 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
  from cpa2(where=(Rank_CPA10=9)) as a, cpa2(where=(Rank_CPA10=2)) as b
  where a.date=b.date;        
quit;

proc sql;
  create table HedgePort6 as
  select distinct a.nqdate, a.date, a.date1, 16 as Rank_CPA10, a.CPA10ret-b.CPA10ret as CPA10ret
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
	/*PROC IMPORT OUT= mf.FF*/
	/*            DATAFILE= "D:\MF competition\FF5.csv" */
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


data mf.teststats_3d2; set teststats; run;
