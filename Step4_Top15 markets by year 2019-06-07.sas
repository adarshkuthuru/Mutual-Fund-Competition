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


**********************************************************************************
						*Top 15 markets by year list;
*********************************************************************************;

proc sql;
	create table fund_values as
	select distinct fundid, domicile, date1, sum(rescale_weighting4*MarketValue_usd) as Fund_MarketValue, count(distinct gvkey) as nsecurities
	from pn.global_funds6
	group by fundid, date1;
quit;
*35076 obs;
proc sort data=fund_values; by domicile date1 fundid; run;

*monthly stats by domicile;
proc sql;
	create table global2.fund_stats_by_domicile_by_month as
	select distinct domicile, date1, count(distinct fundid) as avg_nfunds, mean(nsecurities) as avg_nsecurities
	from fund_values
	group by domicile, date1;
quit;


proc sql;
	create table fund_values2 as
	select distinct fundid, domicile, year(date1) as year, mean(Fund_MarketValue) as Fund_MarketValue_USD, mean(nsecurities) as avg_nfundsecurities_annual
	from fund_values
	group by fundid, year(date1);
quit;

proc sql;
	create table fund_values3 as
	select distinct domicile, year, sum(Fund_MarketValue_USD) as Fund_MarketValue_USD, count(distinct fundid) as ncountryfund_annual, 
	mean(avg_nfundsecurities_annual) as avg_ncountrysecurities_annual
	from fund_values2
	group by domicile, year;
quit;

*sort top 15 countries by fund values each year;
proc sort data=fund_values3; by year descending Fund_MarketValue_USD ; run;

data fund_values3;
	set fund_values3;
	by year;
	if first.year=1 then nrow=0;
	nrow+1;
run;
*420 obs;

data fund_values3;
	set fund_values3;
	if nrow<=15;
run;
*246 obs;

proc sql;
	create table fund_values4 as
	select distinct domicile, mean(Fund_MarketValue_USD) as Fund_MarketValue_USD, mean(ncountryfund_annual) as avg_ncountryfund
	from fund_values3
	group by domicile;
quit;
proc sort data=fund_values4; by descending Fund_MarketValue_USD; run;


data global2.fund_stats_by_domicile;
	set fund_values4;
run;


***Import Global AUM xlsx;
/*PROC IMPORT OUT= Pn.Global_AUM*/
/*            DATAFILE= "G:\MF competition\Global\Global funds AUM data.xlsx" */
/*            DBMS=EXCEL REPLACE;*/
/*sheet="Sheet2";*/
/*GETNAMES=YES;*/
/*MIXED=NO;*/
/*SCANTEXT=YES;*/
/*USEDATE=YES;*/
/*SCANTIME=YES;*/
/*RUN;*/
/**/
/**converting Fund_Returns from wide to long form;*/
/**/
/*proc transpose data=Pn.Global_AUM out=Pn.Global_AUM;*/
/*  by Domicile notsorted;*/
/*  var _2007 _2008 _2009 _2010 _2011 _2012 _2013 _2014 _2015 _2016 _2017 _2018;*/
/*run;*/
/**/
/*proc sql;*/
/*	create table Pn.Global_AUM1 as*/
/*	select distinct domicile, mean(COL1) as AUM*/
/*	from Pn.Global_AUM*/
/*	group by domicile;*/
/*quit;*/
/*proc sort data=Pn.Global_AUM1; by descending AUM; run;*/
