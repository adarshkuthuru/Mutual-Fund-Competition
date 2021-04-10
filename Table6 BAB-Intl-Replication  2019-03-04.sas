*Clean working directory;
proc datasets lib=work nolist kill; quit;

libname BB1 'F:\Global holdings data cleaned\Xpressfeed merging\Large Market cap stocks\Final datasets'; run;
libname BBUS 'F:\Global holdings data cleaned\Xpressfeed merging\Large Market cap stocks\BAB US'; run;
libname BBGL 'F:\Global holdings data cleaned\Xpressfeed merging\Large Market cap stocks\BAB GLOBAL'; run;
libname XP1 'F:\Global holdings data cleaned\Xpressfeed merging\Large Market cap stocks'; run;

LIBNAME PN 'H:\BAB Adarsh'; run; *Prof Nitin's 2TB HDD;
LIBNAME PN1 'H:\BAB Adarsh\Xpressfeed tables'; run;
LIBNAME PNF 'H:\BAB Adarsh\Xpressfeed tables\Full Holdings sample'; run;
LIBNAME PND 'H:\BAB Adarsh\Xpressfeed tables\Domestic Holdings sample'; run;

LIBNAME P1 'F:\Global holdings data cleaned';run;
LIBNAME P2 'F:\Global holdings data cleaned\MF cleaned files'; run;
LIBNAME p3 'G:\'; run;

%let wrds=wrds-cloud.wharton.upenn.edu 4016; 
options comamid=TCP remote=WRDS;        
signon username=_prompt_;               
                                         
libname rwork slibref = work server = wrds; run;

rsubmit;
options nocenter nodate nonumber ls=max ps=max msglevel=i; 
libname mf '/wrds/crsp/sasdata/q_mutualfunds'; run; *refers to crsp;
libname crspa '/wrds/crsp/sasdata/a_stock'; run; *refers to crsp;
libname ff '/wrds/ff/sasdata'; run;
libname fx '/wrds/frb/sasdata'; run; *Refers to the Federal Reserve Bank;
libname s12 '/wrds/tfn/sasdata/s12'; run; *refers to tfn;
libname mfl '/wrds/mfl/sasdata'; run;
libname a_ccm '/wrds/crsp/sasdata/a_ccm'; run; *refers to crsp;
libname naa '/wrds/comp/sasdata/naa'; run; *refers to compa;
libname FF ' /wrds/ff/sasdata'; run;
libname xpf '/wrds/comp/sasdata/d_global'; run; *refers to Xpressfeed;
libname xpfn '/wrds/comp/sasdata/d_na'; run; *refers to Xpressfeed North America;
libname xpf1 '/wrds/comp/sasdata/d_global/security';run; *refers to Xpressfeed security level data;
libname xpf1n '/wrds/comp/sasdata/d_na/security';run; *refers to Xpressfeed security level data;
libname xpf2  '/wrds/comp/sasdata/d_global/company';run; *refers to Xpressfeed company level data;
libname xpf2n  '/wrds/comp/sasdata/d_na/company';run; *refers to Xpressfeed company level data;
libname Ash '/home/isb/adarshkp/BAB_Global'; run; *temporary directory on WRDS;
libname Ash1 '/scratch/isb/Adarsh/Global'; run; *temporary directory on WRDS;
libname Ash2 '/home/isb/adarshkp/Brown_bag'; run; *temporary directory on WRDS;
endrsubmit;



rsubmit;

/********************************************************************/
/*** STEP-1: ESTIMATING EX-ANTE BETAS USING CRSP STOCK-LEVEL DATA ***/
/********************************************************************/


/* Step 1.1. Select common stocks (tpci = 0) from CRSP */
proc sql;
  create table Stocks as
  select distinct gvkey, upcase(loc) as loc
  from xpf.g_secd
  where tpci='0';
quit;
*56,972 gvkeys;


/* Step 1.2. Subsetting countries of interest */
*County codes are listed from Xpressfeed documentation?;
data CountryList;
	input code $3. country $13.;
	datalines;
	AUS AUSTRALIA
	AUT AUSTRIA
	BEL BELGIUM
	CAN CANADA
	DNK DENMARK
	FIN FINLAND
	FRA FRANCE
	DEU GERMANY
	HKG HONGKONG
	ITA ITALY
	JPN JAPAN
	NLD NETHERLANDS
	NOR NORWAY
	NZL NEWZEALAND
	SWE SWEDEN
	SGP SINGAPORE
	ESP SPAIN
	CHE SWITZERLAND
	GBR UNITEDKINGDOM
	GRC GREECE
	IRL IRELAND
	ISR ISRAEL
	PRT PORTUGAL
	BRA BRAZIL
	CHL CHILE
	MEX MEXICO
	IND INDIA
	TWN TAIWAN
	THA THAILAND
	MYS MALAYSIA
	ZAF SOUTHAFRICA
	;
*31 countries: 22 msci developed and 9 msci emerging countries;


*Ncountries contains total number of countries;
proc sql noprint;
  select distinct count(*) into :Ncountries from CountryList;  
quit;
%put &Ncountries; *31 countries;


*Number codes for countries;
data CountryList;
  set CountryList;
  CountryID = _N_; 
run;
data Ash.CountryList; set CountryList; run;

/* Step 1.3. Creating unique gvkey dataset */
*Select stocks from the countries in CountryList;
proc sql;
  create table StocksList as
  select distinct a.*, b.*
  from Stocks as a, CountryList as b
  where a.loc = b.code;
quit;
*38,906 gvkeys;


proc sort data=StocksList nodupkey; by gvkey; run;
*38906 gvkeys, 0 obs deleted;


proc sort data=StocksList nodupkey; by gvkey code country; run;
data StocksList;
  format gvkey;
  set StocksList;
  gvkeyid = _N_; 
run;
data Ash.StocksList; set StocksList; run;
*39121 obs;



/* Step 1.5. Create exchange rate tables */
**Foreign exchange data (for all countries except Netherlands, Chile and Israel) after 1994 is obtained from Thomson Reuters
as there were many missing observations on WRDS and the data before 1994 is obtained from WRDS as the forex data on Reuters starts in 1994;
**Foreign exchange data for Netherlands is obtained only from WRDS since it is not available on Reuters;
**Foreign exchange data for Chile and Israel is obtained only from Thomson Reuters since the data for these countries is not available on WRDS.
Hence, no data for these countries before 1994;

**Step 1.5.1. Foreign exchange data (for all countries except Netherlands, Chile and Israel) before 1994;
data exchange;
set fx.fx_daily;
 rename exalus	=	AUSTRALIA
; rename exauus	=	AUSTRIA
; rename exbeus	=	BELGIUM
; rename exbzus	=	BRAZIL
; rename excaus	=	CANADA
; rename exdnus	=	DENMARK
; rename exeuus	=	EURO
; rename exfnus	=	FINLAND
; rename exfrus	=	FRANCE
; rename exgeus	=	GERMANY
; rename exgrus	=	GREECE
; rename exhkus	=	HONGKONG
; rename exinus	=	INDIA
; rename exirus	=	IRELAND
; rename exitus	=	ITALY
; rename exjpus	=	JAPAN
; rename exmaus	=	MALAYSIA
; rename exmxus	=	MEXICO
; rename exnous	=	NORWAY
; rename exnzus	=	NEWZEALAND
; rename expous	=	PORTUGAL
; rename exsdus	=	SWEDEN
; rename exsfus	=	SOUTHAFRICA
; rename exsius	=	SINGAPORE
; rename exspus	=	SPAIN
; rename exszus	=	SWITZERLAND
; rename extaus	=	TAIWAN
; rename exthus	=	THAILAND
; rename exukus	=	UNITEDKINGDOM
; rename exvzus	=	VENEZUELA;				
if year(date)<=1993;
run;

data exchange;
	set exchange;
	keep DATE AUSTRALIA AUSTRIA BELGIUM CANADA DENMARK FINLAND FRANCE
  GERMANY HONGKONG ITALY JAPAN NORWAY NEWZEALAND SWEDEN SINGAPORE
  SPAIN SWITZERLAND UNITEDKINGDOM PORTUGAL IRELAND GREECE BRAZIL MEXICO INDIA TAIWAN THAILAND MALAYSIA SOUTHAFRICA;
run;

/*converting wide to long form */
proc transpose data=exchange out=exchange;
  by date notsorted;
  var AUSTRALIA AUSTRIA BELGIUM CANADA DENMARK FINLAND FRANCE
  GERMANY HONGKONG ITALY JAPAN NORWAY NEWZEALAND SWEDEN SINGAPORE
  SPAIN SWITZERLAND UNITEDKINGDOM PORTUGAL IRELAND GREECE BRAZIL MEXICO INDIA TAIWAN THAILAND MALAYSIA SOUTHAFRICA
;
run;

data exchange;
	set exchange;
	country=upcase(_NAME_);
	rename col1=rate;
	drop _label_ _NAME_;
run;

**Step 1.5.2. Foreign exchange data for Netherlands (Taken from WRDS Since it's not available on Reuters/Bloomberg);
data exchange1;
	set fx.fx_daily;
	keep date exneus exeuus;
	rename exneus=NETHERLANDS;
	rename exeuus=EURO;
run;

data exchange1;
	set exchange1;
	if year(date)>1998 then  NETHERLANDS=EURO;
	drop Euro;
run;

/*converting wide to long form */
proc transpose data=exchange1 out=exchange1;
  by date notsorted;
  var NETHERLANDS;
run;

data exchange1;
	set exchange1;
	rename _NAME_=country;
	rename col1=rate;
	drop _label_;
run;

**Step 1.5.3. Merging Netherlands data with the data of other countries; 
proc append data=exchange1 base=exchange; run;
*exchange contains data for 29 countries;

**Step 1.5.4. Merging with foreign exchange data (ash.forex) after 1994, obtained from Thomson Reuters (except for Netherlands);
**Put forex file from dropbox in the scratch folder on WRDS;
proc append data=ash.forex base=exchange; run;
*exchange now contains data for 31 countries;

data exchange;
	set exchange;
	COUNTRY=UPCASE(strip(COUNTRY));
run;

***Include country code to exchange;
proc sql;
	create table ash.exchange as
	select distinct a.*,upcase(b.code) as loc
	from exchange as a, ash.countrylist as b
	where a.country=b.country;
quit;


**Step 1.6. Create global daily dataset only for the countries of interest;
*A) Global securities;
rsubmit;
proc sql;
	create table sample as
	select distinct a.gvkey,a.iid,a.conm,a.isin,a.curcdd,a.exchg,a.loc,a.fic, b.prican,b.priusa,b.prirow
	from xpf.g_secd as a left join xpf2.g_company as b
	on a.gvkey=b.gvkey
	group by a.gvkey,a.iid,b.prican,b.priusa,b.prirow,a.conm,a.isin,a.curcdd,a.exchg,a.loc,a.fic;
quit;

proc sql;
	create table sample as
	select distinct a.*, b.excntry
	from sample as a left join xpf1.g_security as b
	on a.gvkey=b.gvkey and a.exchg=b.exchg
	group by a.gvkey,a.iid,a.conm,a.isin, a.exchg,a.loc,a.fic, a.prican,a.priusa,a.prirow;
quit;
*127559 obs;

proc sql;
	create table test as
	select distinct gvkey, iid, exchg,prican,priusa,prirow,excntry
	from sample
	group by gvkey, iid, exchg,prican,priusa,prirow,excntry;
quit;
*71639 obs;

data test1;
	set test;
	if missing(prirow)=0 and iid=prirow;
run;

proc sort data=test1 nodupkey; by gvkey; run; 

data test2;
	set test;
	if missing(prirow)=1;
run;
proc sort data=test2 nodupkey; by gvkey; run; 

proc append data=test2 base=test1; run;

data ash2.Global;
	set test1;
	if upcase(excntry) in ('AUS','AUT','BEL','CAN','DNK','FIN','FRA','DEU','HKG','ITA',
	'JPN','NLD','NOR','NZL','SWE','SGP','ESP','CHE','GBR','GRC','IRL','ISR',
	'PRT','BRA','CHL','MEX','IND','TWN','THA','MYS','ZAF');
	drop priusa prican prirow;
run;

rsubmit;
proc sql;
	create table g_secd as
	select distinct a.datadate,a.gvkey,a.iid,a.conm,a.isin,a.curcdd,a.exchg,a.prccd,a.cshoc,a.ajexdi,a.TRFD,b.excntry
	from xpf.g_secd as a, ash2.global as b
	where a.gvkey=b.gvkey and a.iid=b.iid and a.tpci='0';
quit;
*105314926 obs;

/* Step 1.4. Last trading day by month and year */
proc sql;
  create table LastTradingDay as
  select distinct year(datadate) as year, month(datadate) as month, upcase(excntry) as excntry, day(datadate) as LastTradingDay
  from g_secd 
  group by year(datadate), month(datadate), upcase(excntry)
  having datadate=max(datadate);
quit;

data LastTradingDay;
	set LastTradingDay;
	if upcase(excntry) in ('AUS','AUT','BEL','CAN','DNK','FIN','FRA','DEU','HKG','ITA',
	'JPN','NLD','NOR','NZL','SWE','SGP','ESP','CHE','GBR','GRC','IRL','ISR',
	'PRT','BRA','CHL','MEX','IND','TWN','THA','MYS','ZAF');
run;
data Ash.LastTradingDay; set LastTradingDay; run;
endrsubmit;




**instead of printing log, it saves log file at mentioned location;
proc printto log="H:/Global holdings data cleaned/Xpressfeed merging/Large Market cap stocks/BAB GLOBAL/BAB GLOBAL 2018-08-17.txt";
run;

/* Step 1.7. Calculate stock-level ex-ante vols and correlations */
**Put returns file from dropbox in the scratch folder on WRDS;

rsubmit;

**put this mapped file manually from final datasets folder in the library;
*Selecting countries with #matched largacap stocks >25;
data Stocks;
	set ash2.global;
	if upcase(excntry) in ('AUS','AUT','BEL','CAN','DNK','FRA','DEU','HKG','ITA',
	'JPN','NLD','NOR','NZL','SWE','SGP','ESP','CHE','GBR','GRC','ISR',
	'CHL','MEX','IND','TWN','THA','MYS','ZAF','FIN','IRL','PRT');
run; 

proc sort data=Stocks nodupkey; by gvkey excntry; run;

data Stocks;
	set Stocks;
	keep gvkey excntry;
run;

data exchange;set ash.exchange;run;
data returns;set ash.returns;run;
data LastTradingDay ;set ash.LastTradingDay;run;

data Stocks;
	set Stocks;
	gvkeyid=_N_;
run;

data returns;
	set returns;
	country=upcase(country);
run;

proc sql;
	create table global as
	select distinct a.*
	from ash2.global as a, ash2.gvkey_new as  b
	where a.gvkey=b.gvkey;
quit;

proc sql;
	create table g_secd as
	select distinct a.datadate,a.gvkey,a.iid,a.conm,a.isin,a.curcdd,a.exchg,a.prccd,a.cshoc,a.ajexdi,a.TRFD,b.excntry
	from xpf.g_secd as a, global as b
	where a.gvkey=b.gvkey and a.iid=b.iid and a.tpci='0';
quit;
*105314926 obs;

/*proc sql noprint;*/
/*  select distinct count(*) into :country from country;  */
/*quit;*/

*totpermno contains total number of permnos;
proc sql noprint;
  select distinct count(*) into :totpermno from Stocks;  
quit;


**Calculating the program run time;
/*%let _sdtm=%sysfunc(datetime());*/

options mprint symbolgen;
%macro TSBeta;
%do i=1 %to &totpermno;

    ***Select one gvkey from perms by i counter;
    proc sql;
	  create table Permno_Temp as
	  select distinct *
	  from Stocks
	  where gvkeyid=&i;
	quit;
    
	proc sql noprint;
      select gvkey into :perm from Permno_Temp;
    quit;

	proc sql noprint;
      select excntry into :excntry from Permno_Temp;
    quit;

	proc sql;
	  create table DailyData_Temp as
	  select distinct datadate format=date9., gvkey, excntry, prccd, TRFD, ajexdi
	  from g_secd
	  where input(gvkey,best12.)=&perm and missing(prccd)=0;
	quit;

    ***Include foreign exchange rates;
	proc sql;
	  create table DailyData_Temp as
	  select distinct a.*, b.rate as forex_rate,b.country
	  from DailyData_Temp as a, exchange as b
	  where a.datadate=b.date and a.excntry=b.loc;
	quit;

	***Calculating USD returns;
	data DailyData_Temp;
		set DailyData_Temp;
		by gvkey;

		*A)Local currency returns;
		if first.gvkey=1 then ret=.;
		else ret=((((abs(prccd)/ajexdi)*trfd)/((abs(lag(prccd))/ lag(ajexdi))* lag(trfd)))-1);


		*B)USD returns;
		if first.gvkey=1 then ret_usd=.;
		else ret_usd=((((abs(prccd)/(forex_rate*ajexdi))*trfd)/((abs(lag(prccd))/(lag(forex_rate)*lag(ajexdi)))* lag(trfd)))-1);
	run;
	
	*Include MSCI market returns;
	proc sql;
  	  create table DailyData_Temp as
  	  select distinct a.*, b.rate as mkt
      from DailyData_Temp as a, returns as b
      where a.datadate=b.date and a.excntry=b.loc;
    quit;
    
    *Estimating three day returns;
    proc sort data=DailyData_Temp; by gvkey datadate; run;
    data DailyData_Temp;
      set DailyData_Temp;
      by gvkey datadate;
      lag1ret = lag(ret_usd);
      lag1vwretd = lag(mkt);
      if first.gvkey=1 then lag1ret=.;
      if first.gvkey=1 then lag1vwretd=.;
    run;

    data DailyData_Temp;
      set DailyData_Temp;
      by gvkey datadate;
      lag2ret = lag(lag1ret);
      lag2vwretd = lag(lag1vwretd);
      if first.permno=1 then lag2ret=.;
      if first.permno=1 then lag2vwretd=.;
    run;
    
    data DailyData_Temp;
      format date gvkey lret lvwretd lret3d lvwretd3d;
      set DailyData_Temp;
      if missing(lag1ret)=1 or missing(lag2ret)=1 then delete;

      lret3d = sum(log(1+ret_usd), log(1+lag1ret), log(1+lag2ret));
      lvwretd3d = sum(log(1+mkt), log(1+lag1vwretd), log(1+lag2vwretd));

      *Log daily returns;
      lret = log(1+ret_usd);
      lvwretd = log(1+mkt);

      *Keep relevant variables;
      keep datadate gvkey lret lvwretd lret3d lvwretd3d excntry country;
    run;


	***Subsetting month end data;
	proc sql;
	  create table MonthEnd_Temp as
	  select distinct datadate,gvkey,country,excntry
	  from DailyData_Temp
	  group by month(datadate),year(datadate)
	  having datadate=max(datadate);
	quit;
	
     
	*Last observsation may not be month end date, for instance, if the data ends on 15Aug2017, then the last row will be 15Aug2017. Remove such observsations;
	proc sql;
	  create table MonthEnd_Temp as
	  select distinct a.*
	  from MonthEnd_Temp as a, LastTradingDay as b
	  where year(a.datadate)=b.year and month(a.datadate)=b.month and day(a.datadate)=b.LastTradingDay and a.excntry=b.excntry;
	quit;
    
	***Create past 1-yr and 5-yr dates;
	data MonthEnd_Temp;
	  set MonthEnd_Temp;
	  vol_date=intnx('day',datadate,-365); *past 1-yr date;
	  corr_date=intnx('day',datadate,-1825); *past 5-yr date;
	  format vol_date date9. corr_date date9.;
	run;
    
	***Subset data for volatility;
	proc sql;
	  create table VolData_Temp as
	  select distinct a.datadate, a.gvkey, a.vol_date, b.datadate as date1, b.lret, b.lvwretd
	  from MonthEnd_Temp as a, DailyData_Temp as b
	  where a.gvkey=b.gvkey and a.vol_date<b.datadate<=a.datadate
	  group by a.datadate, a.gvkey
	  having count(distinct b.datadate)>=120; *Require good return for 120 trading days;
	quit;

	***Subset data for correlation;
	proc sql;
      create table CorrData_Temp as
	  select distinct a.datadate, a.gvkey, a.corr_date, b.datadate as date1, b.lret3d, b.lvwretd3d
	  from MonthEnd_Temp as a, DailyData_Temp as b
	  where a.gvkey=b.gvkey and a.corr_date<b.datadate<=a.datadate
	  group by a.datadate, a.gvkey
	  having count(distinct b.datadate)>=750; *Require good return for 750 trading days;
	quit;
    
    ***Estimate trailing stock and market volatilities;
	proc sql;
      create table Vol_&i as
	  select distinct datadate, gvkey, std(lret) as stockvol, std(lvwretd) as mktvol
      from VolData_Temp
	  group by datadate, gvkey;
	quit;
 
    ***Estimate trailing trailing 3day return correlation;
	proc corr data=CorrData_Temp noprint out=Corr_&i;
	  by datadate gvkey;
	  var lret3d lvwretd3d;
	run;
	proc sql;
	  create table Corr_&i as
	  select distinct datadate, gvkey, lret3d as corr
	  from Corr_&i
	  where _TYPE_='CORR' and _NAME_='lvwretd3d';
	quit;

   
	***Merge data;
	proc sql;
	  create table TSBeta_&i as
	  select distinct a.datadate, a.gvkey, b.stockvol, b.mktvol, c.corr
	  from MonthEnd_Temp as a, Vol_&i as b, Corr_&i as c
	  where a.datadate=b.datadate=c.datadate and a.gvkey=b.gvkey=c.gvkey and missing(b.stockvol)=0 and missing(c.corr)=0;
	quit;
     
	***Copy TSBeta_&i to Ash;
	data ASH1.TSBeta_&i; set TSBeta_&i; run;
    
	***Drop temporary data;
	proc sql;
	  drop table CorrData_Temp, Corr_&i, DailyData_Temp, MonthEnd_Temp, Permno_Temp, TSBeta_&i, VolData_Temp, Vol_&i;
	quit;

%end;
%mend TSBeta;

*Call marco;
%TSBeta;

/*%let _edtm=%sysfunc(datetime());*/
/*%let _runtm=%sysfunc(putn(&_edtm - &_sdtm, 12.4));*/
/*%put It took &_runtm second to run the program;*/


*Restore log output in log window;
proc printto; run;

***House cleaning;
proc sql;
  drop table ASH.CRSPStocks, ASH.LastTradingDay;
quit;

* Direct log of macro to a different directory;
proc printto log="H:/Global holdings data cleaned/Xpressfeed merging/Large Market cap stocks/BAB GLOBAL/BAB Global_append.txt"; run;

rsubmit;
/* Step 1.5. Create a consolidated dataset with ex-ante vols and correlations */

*Run a PROC CONTENTS to create a SAS data set with the names of the SAS data sets in the SAS data  library;
proc contents data=ASH1._all_ out=ASH1.NKcont(keep=memname) noprint; run;


*Eliminate any duplicate names of the SAS data set names stored in the SAS data set;
proc sort data=ASH1.NKcont nodupkey; by memname; run;
*24,698 obs;


*Run a DATA _NULL_ step to create 2 macro variables: one with the names of each SAS data set and 
the other with the final count of the number of SAS data sets;
data _null_;
  set ASH1.NKcont end=last;
  by memname;
  i+1;
  call symputx('name'||trim(left(put(i,8.))),memname);
  if last then call symputx('count',i);
run;


*Macro containing the PROC APPEND that executes for each SAS data set you want to concatenate together to create 1 SAS data set;
%macro combdsets;
  %do i=1 %to &count;
    proc append base=TSBeta data=ASH1.&&name&i force; run;
  %end;
%mend combdsets;

*Call macro;
%combdsets;

data ash1.tsbeta; set tsbeta; run;
endrsubmit;
  
/* Step 1.6. Calculate time-series betas */
data TSBeta;
  set pnl.global_Beta;
  TSbeta = corr * (stockvol / mktvol);
run;

*Sanity check;
proc sql;
  create table DistinctPermno as
  select distinct gvkey
  from TSBeta;
quit;
*23483 obs;


*Copy to NK and download;
data Ash.TSBeta; set TSBeta; run;
proc sort data=TSBeta nodupkey; by gvkey datadate; run;
*61119 duplicates;

*Manually transfer NK.TSBeta to BB0419US;
endrsubmit;
*Restore log output in log window;
proc printto; run;


/*****************************************************/
/* Step 2. Shrink betas towards cross-sectional mean */
/*****************************************************/

*TSBeta and TSBeta1 are the Global stock betas and Canadian betas respectively;
*The above files are in 'BAB Global' folder: Path is given below; 
*F:\Global holdings data cleaned\Xpressfeed merging\Large Market cap stocks\BAB GLOBAL;

data Beta;
  set bbgl.tsBeta;
  date1 = intnx("month",datadate,1,"E");
  beta = (0.6 * TSBeta) + (0.4 * 1);
  format date1 date9.;
/*  if datadate>='28FEB1989'd;*/
  keep datadate gvkey beta date1;
run;
*old: 2905553 obs;
*1272163 obs;

proc sql;
	create table test as
	select distinct excntry, year, count(distinct gvkey) as ncount
	from pnd.table1_matched
	group by excntry, year;
quit;
*412 obs;

data test;
	set test;
	if ncount>=50;
run;
*333 obs;

proc sql;
	create table table1_matched as
	select distinct a.*
	from pnd.table1_matched as a, test as b
	where a.excntry=b.excntry and a.year=b.year;
quit;
*94658 obs filtered from 96740 obs;

*include only large cap stocks for next year which were mapped by year;
proc sql;
	create table Beta as
	select distinct a.*,b.excntry
	from Beta as a, table1_matched as b
	where a.gvkey=b.gvkey and year(a.date1)=b.year
	order by a.gvkey,a.date1;
quit;
*410423 obs; 
*Move beta file to ash2 folder on wrds manually;

/*******************************************************************/
/* Step 2.1. Form Global dataset with Exchange rates and mkt returns */
/*******************************************************************/

*Extract exchange codes;
rsubmit;
/*data Beta;*/
/*	set ash2.Beta; */
/*run;*/

data exchange;set ash.exchange;run;
data returns;set ash.returns;run;
/*data g_secd ;set ash2.g_secd ;run;*/

proc sql;
	create table global as
	select distinct a.*
	from ash2.global as a, ash2.gvkey_bab as  b
	where a.gvkey=b.gvkey;
quit;

proc sql;
	create table g_secd as
	select distinct a.datadate,a.gvkey,a.iid,a.conm,a.isin,a.curcdd,a.exchg,a.prccd,a.cshoc,a.ajexdi,a.TRFD,b.excntry
	from xpf.g_secd as a, global as b
	where a.gvkey=b.gvkey and a.iid=b.iid and a.tpci='0';
quit;
*77865493 obs;


data returns;
	set returns;
	country=upcase(country);
run;

/*data exchange;*/
/*	set exchange;*/
/*	loc1=compress(loc, ,'S'); *Removes fake spaces from variable;*/
/*run;*/

***Include foreign exchange rates;
proc sql;
  create table DailyData_Temp as
  select distinct a.*, b.rate as forex_rate,b.country
  from g_secd as a, exchange as b
  where a.datadate=b.date and a.excntry=b.loc;
quit;
*77962683 obs;


proc sort data=DailyData_Temp; by gvkey datadate; run;


*Include MSCI market returns;
proc sql;
    create table g_secd as
    select distinct a.*, b.rate as mkt
    from DailyData_Temp as a, returns as b
    where a.datadate=b.date and a.excntry=b.loc
	order by a.excntry,a.gvkey,a.datadate;
quit;
*77865493 obs;

/*proc download data=g_secd out=tmp1.BB0419US.g_secd; run;*/
/*endrsubmit;*/
proc sort data=g_secd; by gvkey datadate; run;
data g_secd;
	set g_secd;
	by gvkey datadate;

	*A)Local currency returns;
	if first.gvkey=1 then ret=.;
	else ret=((((abs(prccd)/ajexdi)*trfd)/((abs(lag(prccd))/ lag(ajexdi))* lag(trfd)))-1);


	*B)USD returns;
	if first.gvkey=1 then ret_usd=.;
	else ret_usd=((((abs(prccd)/(forex_rate*ajexdi))*trfd)/((abs(lag(prccd))/(lag(forex_rate)*lag(ajexdi)))* lag(trfd)))-1);
run;
*77865493 obs;

**Splitting datasets into subsets;
/*%macro split1(num);*/
/*data _null_;*/
/* if 0 then set g_secd nobs=count;*/
/* call symput('numobs',put(count,8.));*/
/*run;*/
/*%let m=%sysevalf(&numobs/&num,ceil);*/
/*data %do J=1 %to &m ; ash2.filename_&J %end; ;*/
/*set g_secd;*/
/* %do I=1 %to &m;*/
/* if %eval(&num*(&i-1)) <_n_ <=*/
/* %eval(&num*&I) then output ash2.filename_&I;*/
/*%end;*/
/**/
/*%mend split1;*/
/**/
/*%split1(10000000)*/
*1000000 is the desired number of observations in the individual datasets;


/*******************************************/
/* Step 3. BAB Portfolio Performance Stats */
/*******************************************/

/* Step 3.0. Form BAB Portfolios */

/*rsubmit;*/
/*data g_secd ;set ash2.g_secd1 ;run;*/

/*proc upload data=Beta out=Beta; run;*/

/* Step 3.1 Form ranked portfolios */

/*proc sort data=Beta; by loc datadate beta; run;*/
/*proc rank data=Beta groups=10 out=Beta2;*/
/*  by loc datadate;*/
/*  var beta;*/
/*  ranks Rank_beta;*/
/*run;*/
/*data Beta2;*/
/*  set Beta2;*/
/*  Rank_beta = Rank_beta + 1;*/
/*run;*/
/*proc sort data=beta2;by datadate loc Rank_beta beta;run;*/
/**/
/**/
/*/* Step 3.1 Portfolio returns for the next month */*/
/*data Beta3;*/
/*  set Beta2;*/
/*  date1 = intnx("month",datadate,1,"E");*/
/*  format date1 date9.;*/
/*run;*/
/**/
/*proc sql;*/
/*  create table Beta3 as*/
/*  select distinct a.*, b.Ret_usd as HoldRet label='Holding Period',b.loc*/
/*  from Beta3 as a, g_secd as b*/
/*  where a.gvkey=b.gvkey and intnx("month",a.date1,0,"E")=intnx("month",b.datadate,0,"E") and missing(b.Ret_usd)=0;*/
/*quit;*/
/*proc sql;*/
/*  create table Beta_RET as*/
/*  select distinct datadate, loc, rank_beta, date1, mean(beta) as PortBeta_ExAnte, mean(HoldRet) as PortRet  */
/*  from Beta3*/
/*  group by datadate, rank_beta, date1,loc*/
/*  order by datadate, rank_beta, date1,loc; */
/* quit;*/
/**/
/*proc sql;*/
/*	create table test as*/
/*	select distinct datadate, loc, count(distinct rank_beta) as count*/
/*	from beta_ret*/
/*	group by datadate, loc;*/
/*quit;*/


rsubmit;
/* Step 3.2 BAB factor returns for the next month */
rsubmit;
data beta; set ash2.beta; run;
*Cross-sectional percentile ranks;
proc sort data=Beta; by excntry datadate; run;
proc rank data=Beta fraction out=Beta4;
  by excntry datadate;
  var beta;
  ranks PctlRank_Beta;
run;
proc sql;
  create table Beta4 as
  select distinct *, PctlRank_Beta-mean(PctlRank_Beta) as wt
  from Beta4
  group by datadate,excntry
  order by datadate,excntry,wt;
quit;

*Long low beta stocks, short high beta stocks;
data Beta4;
  set Beta4;
  if wt < 0 then long = 1;
  else long = 0;
run;

*Rescale the weights in long and short legs;
*Larger weight to stocks with lower betas in the long leg, larger weights to stocks with higher betas in the short leg;
proc sql;
  create table Beta4 as
  select distinct *, wt/sum(wt) as wt1
  from Beta4
  group by datadate, excntry, long
  order by datadate, excntry, wt;
quit;
proc sort data=Beta4; by datadate descending excntry long beta; run;

*Next month returns;
data Beta4;
  set Beta4;
  date1 = intnx("month",datadate,1,"E");
  format date1 date9.;
run;
*1614300 obs;

proc sql;
  create table Beta4 as
  select distinct a.*, b.Ret_usd as HoldRet label='Holding Period'
  from Beta4 as a left join g_secd as b
  on a.gvkey=b.gvkey and intnx("month",a.date1,0,"E")=intnx("month",b.datadate,0,"E") and missing(b.Ret_usd)=0 and a.excntry=b.excntry;
quit;

proc sql;
  create table Beta5 as
  select distinct gvkey, excntry, datadate, long, date1, mean(beta) as beta, sum(HoldRet) as HoldRet, mean(PctlRank_Beta) as PctlRank_Beta,
  mean(wt) as wt, mean(wt1) as wt1
  from Beta4
  group by gvkey, datadate, long, excntry
  order by gvkey, datadate, long, excntry;
quit;

***BAB portfolio return;
proc sql;
  create table BAB_RET as
  select distinct excntry, datadate, long, date1, sum(wt1*beta) as PortBeta_ExAnte, sum(wt1*HoldRet) as PortRet  
  from Beta5
  group by datadate, long, excntry
  order by datadate, long, excntry; 
 quit;

 *Bab returns for AUS;
/*data BABFactor_AUS;*/
/*	set BAB_ret;*/
/*	if loc='AUS';*/
/*run;*/

*Include risk-free rate;
/*proc sql;*/
/*   create table BAB_RET as*/
/*   select distinct a.*, b.rf*/
/*   from BAB_RET as a, ff.factors_monthly as b*/
/*   where intnx("month",a.date1,0,"E")=intnx("month",b.date,0,"E");*/
/*quit;*/

*BAB calculation;
data BAB_RET;
  set BAB_RET;
  ExPortret = PortRet; *Changed formula;
  DollarLongShort = 1 / PortBeta_ExAnte;
  LeveredPortRet = (1 / PortBeta_ExAnte) * (PortRet); *Changed formula;
  LeveredPort_ExAnteBeta = PortBeta_ExAnte /  PortBeta_ExAnte; *Both long and short portfolios have ex-ante beta of 1 by constuction;
run;

data BAB_RET;
  set BAB_RET;
  if missing(LeveredPort_ExAnteBeta)=1 then delete;
run;

proc sql;
  create table BABFactor as
  select distinct a.excntry,a.datadate, 11 as rank_beta, a.date1, a.LeveredPort_ExAnteBeta - b.LeveredPort_ExAnteBeta as PortBeta_ExAnte,
                  a.LeveredPortRet - b.LeveredPortRet as PortRet label='BABFactor Port Ret'
  from (select distinct * from BAB_RET where long=1) as a, (select distinct * from BAB_RET where long=0) as b
  where a.datadate=b.datadate and a.excntry=b.excntry;
 quit;

*Combine Beta_Ret and BABFactor;
/*data BAB_RET_US;*/
/*  set BAB_RET BABFactor; *Changed this: taking only BAB factor;*/
/*run;*/
/*proc sort data=BAB_RET_US; by loc datadate rank_beta; run;  */


*Download required data locally;
proc download data=BAB_RET out=BAB_RET; run;
*6824 obs;
proc download data=BABFactor out=BABFactor ; run;
*3412 obs;
/*proc download data=Beta5 out=Beta5; run;*/
data ash2.BETA5; set BETA5; run;
*410423 obs;
endrsubmit;

data pnd.BABFactor; set BABFactor; run;
data pnd.BAB_RET; set BAB_RET; run;

*Monthly market returns;
data returns_monthly;
	set bb1.returns;
	month=month(date);
	year=year(date);
run;

proc sql;
	create table returns_monthly as
	select distinct country, loc, sum(rate) as ret, month, year
	from returns_monthly
	group by month, year, loc;
quit;
proc sort data=BABFactor; by excntry datadate; run; 

*Merging market returns with BAB returns;
proc sql;
	create table BABFactor as
	select distinct a.*,b.ret as mktret
	from BABFactor as a left join returns_monthly as b
	on a.excntry=b.loc and month(a.datadate)=b.month and year(a.datadate)=b.year
	group by a.excntry,b.month,b.year
	order by a.excntry,b.month,b.year;
quit; 

*Include risk-free rate: Use USA rf rate for all countries;
PROC IMPORT OUT=Rf
            DATAFILE= "F:\Global holdings data cleaned\Xpressfeed merging\Large Market cap stocks\Brown bag\monthly_rf.csv" 
            DBMS=CSV REPLACE;
RUN;

proc sql;
   create table BABFactor as
   select distinct a.*, b.rf/100 as rf
   from BABFactor as a, rf as b
   where month(a.date1)=b.month and year(a.date1)=b.year;
quit;

data BABFactor;
  set BABFactor;
  if mktret<0 then PortRet1=rf;
  else PortRet1=PortRet;
run; 


/*********************************************/
/*** Step 4. Performance of BAB Portfolios ***/
/*********************************************/


/* Step 4.1 Dollars long and short */

/*proc sql;*/
/*  create table DollarLongShort as*/
/*  select distinct long, excntry, mean(DollarLongShort) as DollarLongShort*/
/*  from BAB_RET*/
/*  group by long, excntry;*/
/*quit;*/


/* Step 4.2 Performance stats of beta and BAB portfolios */

*A) Unconditional BAB stats;
proc sort data=BABFactor out=BABfactor_PcRet; by excntry date1; run;
data BABfactor_PcRet;
  set BABfactor_PcRet;
  PortRet=PortRet*100;
/*  if excntry='AUS' and datadate in ('29SEP1995'd,'31OCT1995'd,'31MAY2002'd) then delete;*/
/*  if excntry='DEU' and datadate in ('30DEC2005'd,'30SEP2016'd) then delete;*/
/*  ExPortRet=ExPortRet*100;*/
/*  if PortRet > 0 then PortRet1 = 0; else PortRet1 = PortRet;*/ *Conditional BAB;
run;


**AVERAGE PERFORMANCE;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet;
  output out=AvgPerf N=Nobs mean=Mean_PortRet;
quit;


**t-stats, AVERAGE PERFORMANCE;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet;
  output out=TstatAvgPerf N=Nobs t=tstat_PortRet;
quit;


**VOLATILITY;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet;
  output out=Volatility std=Vol_PortRet; *Vol_PortRet1 measures downside volatility;
quit;


*SKEWNESS, 1 PERCENTILE AND MINIMUM;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet;
  output out=RiskMeasures skew=Skew_PortRet P1=Pctl1_PortRet min=Min_PortRet;
quit;


***COMBINE SUMMARY STATS;
proc sql;
  create table SumStats_Perf_Risk as
  select distinct a.excntry, a._Freq_ as N label='# of Holding Period months', 
         a.Mean_PortRet label='Holding Period Avg EW return', d.tstat_PortRet label='Holding Period t-Stat of Avg EW return',
         b.Vol_PortRet label='Volatility (Holding Period EW return)', 
		 c.Skew_PortRet label='Skenewss (Holding Period EW return)', c.Pctl1_PortRet label='1 percentile (Holding Period EW return)', 
		 c.Min_PortRet label='Minimum (Holding Period EW return)'
  from AvgPerf as a, Volatility as b, RiskMeasures as c, TstatAvgPerf as d
  where a.excntry=b.excntry=c.excntry=d.excntry;
quit;


***SHARPE AND SORTINO RATIOS;
data SumStats_Perf_Risk;
  set SumStats_Perf_Risk;
  Sharpe = (Mean_PortRet/Vol_PortRet)*sqrt(12);
/*  Sortino = (Mean_PortRet/Vol_PortRet1)*sqrt(12);*/
  format Mean_PortRet 6.3 Vol_PortRet 6.3 
         Skew_PortRet 6.3 Pctl1_PortRet 6.3 Min_PortRet 6.3 Sharpe 6.3 ;
run;
proc sql;
  drop table AvgPerf, Volatility, RiskMeasures, TstatAvgPerf, BAB_RET_US_PcRet;
quit;

proc export data=SumStats_Perf_Risk
    outfile='C:\Users\30970\Desktop\test1.csv' dbms=csv replace;
run;

*B) Conditional BAB stats;

proc sort data=BABFactor out=BABfactor_PcRet; by excntry date1; run;
data BABfactor_PcRet;
  set BABfactor_PcRet;
  if mktret>=0 then portret1=portret1*100;
/*  if excntry='AUS' and datadate in ('29SEP1995'd,'31OCT1995'd,'31MAY2002'd) then delete;*/
/*  if excntry='DEU' and datadate in ('30DEC2005'd,'30SEP2016'd) then delete;*/
/*  ExPortRet=ExPortRet*100;*/
/*  if PortRet > 0 then PortRet1 = 0; else PortRet1 = PortRet;*/ *Conditional BAB;
run;


**AVERAGE PERFORMANCE;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet1;
  output out=AvgPerf N=Nobs mean=Mean_PortRet;
quit;


**t-stats, AVERAGE PERFORMANCE;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet1;
  output out=TstatAvgPerf N=Nobs t=tstat_PortRet;
quit;


**VOLATILITY;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet1;
  output out=Volatility std=Vol_PortRet; *Vol_PortRet1 measures downside volatility;
quit;


*SKEWNESS, 1 PERCENTILE AND MINIMUM;
proc means data=BABfactor_PcRet noprint;
  by excntry;
  var PortRet1;
  output out=RiskMeasures skew=Skew_PortRet P1=Pctl1_PortRet min=Min_PortRet;
quit;


***COMBINE SUMMARY STATS;
proc sql;
  create table SumStats_Perf_Risk1 as
  select distinct a.excntry, a._Freq_ as N label='# of Holding Period months', 
         a.Mean_PortRet label='Holding Period Avg EW return', d.tstat_PortRet label='Holding Period t-Stat of Avg EW return',
         b.Vol_PortRet label='Volatility (Holding Period EW return)', 
		 c.Skew_PortRet label='Skenewss (Holding Period EW return)', c.Pctl1_PortRet label='1 percentile (Holding Period EW return)', 
		 c.Min_PortRet label='Minimum (Holding Period EW return)'
  from AvgPerf as a, Volatility as b, RiskMeasures as c, TstatAvgPerf as d
  where a.excntry=b.excntry=c.excntry=d.excntry;
quit;


***SHARPE AND SORTINO RATIOS;
data SumStats_Perf_Risk1;
  set SumStats_Perf_Risk1;
  Sharpe = (Mean_PortRet/Vol_PortRet)*sqrt(12);
/*  Sortino = (Mean_PortRet/Vol_PortRet1)*sqrt(12);*/
  format Mean_PortRet 6.3 Vol_PortRet 6.3 
         Skew_PortRet 6.3 Pctl1_PortRet 6.3 Min_PortRet 6.3 Sharpe 6.3 ;
run;
proc sql;
  drop table AvgPerf, Volatility, RiskMeasures, TstatAvgPerf, BAB_RET_US_PcRet;
quit;

proc export data=SumStats_Perf_Risk1
    outfile='C:\Users\30970\Desktop\test2.csv' dbms=csv replace;
run;


*************************************************************************************
									Table-3
************************************************************************************;
proc sort data=BABfactor; by excntry Datadate; run;

data BABfactor1;
  set BABfactor;
  if missing(PortRet)=1 then delete;
  if mktret<0 then D1=1;
  else D1=0;
run;

data BABfactor1;
  set BABfactor1;
  by excntry;
  if first.excntry=1 then D2=.;
  else D2=lag(D1);
run;

*Delete observations if the data is not available for 12 months in the year;
proc sql;
	create table test as
	select distinct excntry, year(datadate) as year, count(distinct datadate) as ncount
	from babfactor
	group by excntry, year(datadate);
quit;

data test;
	set test;
	if ncount<12 then delete;
run;

proc sql;
	create table babfactor1 as
	select distinct a.*
	from babfactor1 as a, test as b
	where a.excntry=b.excntry and year(a.datadate)=b.year;
quit;

proc sort data=babfactor1; by excntry D2; run;
proc means data=babfactor1 noprint;
  by excntry D2;
  var PortRet;
  output out=BABStats mean=mean t=tstat probt=pvalue;
run;


ods trace on;
ods output statistics = statistics TTests=ttests; 
proc ttest data=babfactor1;
  by excntry;
  class D2;
  var portret;
run;
ods trace off;

data statistics;
  set statistics;
  if class='Diff (1-2)';
  D2 = .;
  keep excntry D2 mean;
run;

proc sql;
  create table statistics as
  select distinct a.*, b.tvalue as tstat, b.probt as pvalue
  from statistics as a, ttests as b
  where a.excntry=b.excntry and b.method='Pooled';
quit;

data BABStats1;
  set BABStats statistics;
  if D2=. then D2=2;
  drop _Type_;
  mean=mean*100;
  format mean 6.3 tstat 6.3 pvalue 6.3;
run;
proc sort data=BABStats1; by excntry D2; run;

data babstats4;
	set babstats1;
	if missing(_FREQ_)=1;
run;

data babstats2;
	set babstats1;
	if D2=0;
run;

data babstats3;
	set babstats1;
	if D2=1;
run;




