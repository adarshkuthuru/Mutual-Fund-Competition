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

*estimate euclidean distance;
data mf.tfnfund_chrs_qtrly_zs_3d2;
	set mf.tfnfund_chrs_qtrly_zs_3d2;
	d=sqrt(sum((sum(logsize,-logsize1)**2),(sum(rlogbm,-rlogbm1)**2),(sum(rMom,-rMom1)**2)));
run;
proc sort data=mf.tfnfund_chrs_qtrly_zs_3d2; by qdate wficn d; run;

*count the number of rival funds for each fund by each quarter;
data mf.tfnfund_chrs_qtrly_zs_3d2;
	set mf.tfnfund_chrs_qtrly_zs_3d2;
	by qdate wficn;
	if first.wficn=1 then count=0;
	count+1;
run;
*256924854 obs;


**********************************************************************************************************************
									Estimating the peer fraction of funds
**********************************************************************************************************************;
proc sql;
	create table mf.peer_fraction as
	select distinct qdate, count(distinct wficn) as nfunds, 1 as id
	from mf.tfnfund_chrs_qtrly_zs_3d
	group by qdate;
quit;

proc sql;
	create table test as
	select distinct npeers, 1 as id
	from mf.teststats;
quit;

proc sql;
	create table mf.peer_fraction as
	select distinct a.*,b.npeers,(b.npeers/a.nfunds) as fraction_peers
	from mf.peer_fraction as a left join test as b
	on a.id=b.id;
quit;

*Remove data with peer_fraction>1;
data mf.peer_fraction;
	set mf.peer_fraction;
	if fraction_peers>1 then dummy=1;
	else dummy=0;
run;

data mf.peer_fraction;
	set mf.peer_fraction;
	by qdate dummy;
	if first.dummy=1 then dummy=0;
run;

data mf.peer_fraction;
	set mf.peer_fraction;
	if dummy=0;
	drop id;
run;

data mf.peer_fraction;
	set mf.peer_fraction;
	by qdate;
	if last.qdate then npeers=nfunds;
run;

data mf.peer_fraction;
	set mf.peer_fraction;
	fraction_peers=npeers/nfunds;
	drop dummy;
run;

proc sql;
	create table mf.peer_fraction2 as
	select distinct qdate, nfunds
	from mf.peer_fraction
	group by qdate;
quit;

**********************************************************************************************************************
						Estimation of customized peer alpha (CPA)
**********************************************************************************************************************;

**copy files to work directory;
data crspfund_chrs_monthly; set mf.crspfund_chrs_monthly; run;
data ff; set mf.ff; run;
data mf.tfnfund_chrs_qtrly_zs_3d2; 
	set mf.tfnfund_chrs_qtrly_zs_3d2; 		
	nqdate=intnx('qtr',qdate,1,'E');		
	format nqdate date9.;
run;


*Loop for different number of peers;
%let _sdtm=%sysfunc(datetime());

/*proc sql noprint;*/
/*        select count(*) into :num from mf.peer_fraction2;*/
/*quit;*/


options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit;
%do i=18 %to 20;

/*	proc sql noprint;*/
/*	    select nfunds into :nfunds from mf.peer_fraction2 where nrow=&i;*/
/*	quit;*/

	data peer_fraction; 
		set mf.peer_fraction2; 
		peer_max=int(0.05*&i*nfunds);
	run;

	proc sql;
		create table tfnfund_chrs_qtrly_zs_3d3 as
		select distinct a.*,b.peer_max
		from mf.tfnfund_chrs_qtrly_zs_3d2 as a, peer_fraction as b
		where a.qdate=b.qdate and a.count<=b.peer_max;
	quit;
	*12767288 obs;

		*create next quarter date;
	data tfnfund_chrs_qtrly_zs_3d3;
		set tfnfund_chrs_qtrly_zs_3d3;
/*		if count<=peer_max;*/
		if count=1 then delete; *same fund;
		nqdate=intnx('qtr',qdate,1,'E');
		format nqdate date9.;
	run;

	*Generate month end date for the time-period;
	%let start_date=01JAN1980;
	%let end_date=31DEC2018;

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
		select distinct a.qdate,a.wficn,a.rivwficn,a.nqdate,b.date, max(a.d) as maxdis
		from tfnfund_chrs_qtrly_zs_3d3 as a left join month as b
		on a.qdate<=b.date and b.date<a.nqdate
		group by a.qdate,a.wficn
		order by b.date, a.wficn,a.rivwficn;
	quit;
	*37781130 obs;


	*include rivwficn returns before expenses data from CRSP fund chrs dataset;
	proc sql;
		create table CPA as
		select distinct a.*, b.mret_bexp as rivwficnret
		from cpa as a left join crspfund_chrs_monthly as b
		on a.date=b.date and a.rivwficn=b.wficn
		order by b.date, a.wficn,a.rivwficn;
	quit;

	*Estimate avg ret of rival funds;
	proc sql;
		create table cpa1 as
		select distinct qdate, date, wficn, mean(rivwficnret) as avgretrivwficn, maxdis
		from cpa
		group by qdate, date, wficn
		order by qdate, date, wficn;
	quit;
	*520734 obs;

	*Include wficn returns;
	proc sql;
		create table CPA1 as
		select distinct a.*, b.mret_bexp as wficnret, sum(b.mret_bexp,-a.avgretrivwficn) as CPA25
		from cpa1 as a, mf.crspfund_chrs_monthly as b
		where a.date=b.date and a.wficn=b.wficn and missing(b.mret_bexp)=0 and missing(a.avgretrivwficn)=0
		order by a.qdate, a.date, a.wficn;
	quit;
	*479320 obs;

	data cpa1;
		set cpa1;
		date1=intnx('month',date,1,'E');
		past12monthdate=intnx('month',date,-12,'E');
		format date1 date9. past12monthdate date9.;
	run;

	*Include wficn returns for next month;
	proc sql;
		create table CPA1 as
		select distinct a.*, b.mret_bexp as wficnret1
		from cpa1 as a left join crspfund_chrs_monthly as b
		on a.date1=b.date and a.wficn=b.wficn
		order by a.qdate, a.date, a.wficn;
	quit;
	*479320 obs;

	**Past 12-month avg CPA estimation by fund & date;
	proc sql;
	  create table CPA2 as
	  select distinct a.*, b.date as date_12m
	  from CPA1 as a, CPA1 as b
	  where a.wficn=b.wficn and a.past12monthdate<b.Date<=a.Date
	  group by a.wficn, a.date
	  having count(distinct b.Date)=12; *Require non-missing CPA for all 12 months;;
	quit;
	*4987848 obs;

	proc sql;
		create table cpa2 as
		select distinct wficn, date, mean(CPA25) as CPA25
		from CPA2
		group by wficn, date;
	quit;
	*415654 obs;

	*Include avg CPA in main dataset;
	proc sql;
		create table CPA1 as
		select distinct a.*, b.CPA25 as Avg_CPA25
		from cpa1 as a, cpa2 as b
		where a.date=b.date and a.wficn=b.wficn
		order by a.qdate, a.date, a.wficn;
	quit;
	*415654 obs;


*********************************************************************************************************************
									*Performance of CPA25 factor, Form Portfolios;
********************************************************************************************************************;


	***Decile groups;
	proc sort data=CPA1; by date Avg_CPA25; run;
	proc rank data=CPA1 groups=10 out=CPA1;
	  by date;
	  var Avg_CPA25;
	  ranks Rank_CPA25;
	run;
	data CPA1;
	  set CPA1;
	  Rank_CPA25=Rank_CPA25+1;
	run;

	*Estimate avg ret of portfolios by date;
	proc sql;
		create table cpa2 as
		select distinct qdate, date, Rank_CPA25, mean(wficnret1) as CPA25ret
		from cpa1
		group by qdate, date, Rank_CPA25
		order by qdate, date, Rank_CPA25;
	quit;

	*Hedge portfolios;
	proc sql;
	  create table HedgePort1 as
	  select distinct a.qdate, a.Date, 11 as Rank_CPA25, a.CPA25ret-b.CPA25ret as CPA25ret
	  from cpa2(where=(Rank_CPA25=10)) as a, cpa2(where=(Rank_CPA25=1)) as b
	  where a.Date=b.Date;        
	quit;

	proc sql;
	  create table HedgePort2 as
	  select distinct a.qdate, a.Date, 12 as Rank_CPA25, a.CPA25ret-b.CPA25ret as CPA25ret
	  from cpa2(where=(Rank_CPA25=10)) as a, cpa2(where=(Rank_CPA25=2)) as b
	  where a.Date=b.Date;        
	quit;

	proc sql;
	  create table HedgePort3 as
	  select distinct a.qdate, a.Date, 13 as Rank_CPA25, a.CPA25ret-b.CPA25ret as CPA25ret
	  from cpa2(where=(Rank_CPA25=10)) as a, cpa2(where=(Rank_CPA25=3)) as b
	  where a.Date=b.Date;        
	quit;

	proc sql;
	  create table HedgePort4 as
	  select distinct a.qdate, a.Date, 14 as Rank_CPA25, a.CPA25ret-b.CPA25ret as CPA25ret
	  from cpa2(where=(Rank_CPA25=9)) as a, cpa2(where=(Rank_CPA25=1)) as b
	  where a.Date=b.Date;        
	quit;

	proc sql;
	  create table HedgePort5 as
	  select distinct a.qdate, a.Date, 15 as Rank_CPA25, a.CPA25ret-b.CPA25ret as CPA25ret
	  from cpa2(where=(Rank_CPA25=9)) as a, cpa2(where=(Rank_CPA25=2)) as b
	  where a.Date=b.Date;        
	quit;

	proc sql;
	  create table HedgePort6 as
	  select distinct a.qdate, a.Date, 16 as Rank_CPA25, a.CPA25ret-b.CPA25ret as CPA25ret
	  from cpa2(where=(Rank_CPA25=9)) as a, cpa2(where=(Rank_CPA25=3)) as b
	  where a.Date=b.Date;        
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
	proc sort data=cpa2; by Rank_CPA25; run;
	proc means data=cpa2 noprint;
	  by Rank_CPA25;
	  var CPA25ret;
	  output out=MeanRet mean=CPA25ret;
	quit;
	proc means data=cpa2 noprint;
	  by Rank_CPA25;
	  var CPA25ret;
	  output out=TstatRet t=CPA25ret;
	quit;

	data MeanRet;
	  set MeanRet;
	  drop _Type_ _Freq_ var stat;
	  CPA25ret=CPA25ret*100;
	  rename CPA25ret=CPAret;
	run;
	data TstatRet;
	  set TstatRet;
	  drop _Type_ _Freq_ var stat;
	  rename CPA25ret=CPAret_Tstat;
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
		where month(a.date)=b.month and year(a.date)=b.year;
	quit;

	/*data cpa2;*/
	/*	set cpa2;*/
	/*	drop mktrf rf exret;*/
	/*run;*/

	data cpa2;
		set cpa2;
		if Rank_CPA25<=10 then Exret=CPA25ret-rf;
		else Exret=CPA25ret;
	run;

	proc sort data=cpa2; by Rank_CPA25 date; run;

	proc reg data=cpa2 noprint tableout outest=Alpha;
	  by Rank_CPA25;
	  model Exret = MKTRF;
	  model Exret = MKTRF SMB HML; *3-factor;
	  model Exret = MKTRF SMB HML RMW CMA; *5-factor;
	quit;

	data Alpha;
	  set Alpha;
	  where _Type_ in ('PARMS','T');
	  keep _model_ Rank_CPA25 _Type_ Intercept;
	  if _Type_='PARMS' then Intercept=Intercept*100;
	  rename Intercept=CPA25ret;
	  rename _Type_ =Stat;
	run;
	data Alpha;
	  set Alpha;
	  if Stat='PARMS' then Var=3;
	  if Stat='T' then Var=4;
	  drop _label_ _name_;
	run;

	*converting long to wide form;
	proc sort data=Alpha; by Rank_CPA25 _model_; run;
	proc transpose data=Alpha out=Alpha;
	  by Rank_CPA25; *only one year per column: will be converted to row;
	  var CPA25ret;
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
		on a.rank_cpa25=b.rank_cpa25;
	quit;

	proc sql;
		create table teststats as
		select distinct a.*,b.*
		from teststats as a left join alpha as b
		on a.rank_cpa25=b.rank_cpa25;
	quit;

	data teststats;
		set teststats;
		peer_perc=5*&i;
	run;

	proc append data=teststats base=mf.teststats1; run;

%end;
%mend doit;
%doit

%let _edtm=%sysfunc(datetime());
%let _runtm=%sysfunc(putn(&_edtm - &_sdtm, 12.4));
%put It took &_runtm second to run the program;
