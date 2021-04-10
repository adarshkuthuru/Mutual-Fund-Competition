libname FCSep29 'D:\MF competition'; run;

*Step-1: Cleaning returns data;

PROC IMPORT OUT= FCSep29.Fund_Returns
            DATAFILE= "E:\Drive\Local Disk F\Mutual fund competition\US_set1.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Returns";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

*converting Fund_Returns from wide to long form;

proc transpose data=FCSep29.Fund_Returns out=FCSep29.Fund_Returns;
  by Group_Investment SecId FundId notsorted;
  var _all_;
run;

data FCSep29.Fund_Returns;
	set FCSep29.Fund_Returns;
	Date=input(_LABEL_,MMDDYY10.);
	format Date date9. ;
	if _label_ in ('Group/Investment','SecId','FundId','Base Currency','Domicile','Inception Date',
	'F247','F248','F249','F250','F251','F252','F253','F254','F255') then delete;
	drop _name_ _label_;
run;

data FCSep29.Fund_Returns;
	set FCSep29.Fund_Returns;
	if missing(fundid)=1 or missing(secid)=1 then delete;
	col1=col1/100;
	rename col1=ret;
run;

*Step-2: Cleaning expense ratio data;

PROC IMPORT OUT= FCSep29.Expense_ratio
            DATAFILE= "E:\Drive\Local Disk F\Mutual fund competition\US_set1.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Expense_ratio";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

*converting Fund_Returns from wide to long form;

proc transpose data=FCSep29.Expense_ratio out=FCSep29.Expense_ratio;
  by Group_Investment SecId FundId notsorted;
  var _all_;
run;

data FCSep29.Expense_ratio;
	set FCSep29.Expense_ratio;
	rename _LABEL_=Date;
	if _label_ in ('Group/Investment','SecId','FundId','Base Currency','Domicile','Inception Date',
	'F247','F248','F249','F250','F251','F252','F253','F254','F255') then delete;
	drop _name_ ;
run;

data FCSep29.Expense_ratio;
	set FCSep29.Expense_ratio;
	if missing(fundid)=1 or missing(secid)=1 then delete;
	col1=col1/100;
	rename col1=Expense_ratio;
run;

*Step-3: Cleaning turnover ratio data;

PROC IMPORT OUT= FCSep29.Turnover_ratio
            DATAFILE= "E:\Drive\Local Disk F\Mutual fund competition\US_set1.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="turnover_ratio";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

*converting Fund_Returns from wide to long form;

proc transpose data=FCSep29.Turnover_ratio out=FCSep29.Turnover_ratio;
  by Group_Investment SecId FundId notsorted;
  var _all_;
run;

data FCSep29.Turnover_ratio;
	set FCSep29.Turnover_ratio;
	rename _LABEL_=Date;
	if _label_ in ('Group/Investment','SecId','FundId','Base Currency','Domicile','Inception Date',
	'F247','F248','F249','F250','F251','F252','F253','F254','F255') then delete;
	drop _name_;
run;

data FCSep29.Turnover_ratio;
	set FCSep29.Turnover_ratio;
	if missing(fundid)=1 or missing(secid)=1 then delete;
	col1=col1/100;
	rename col1=Turnover_ratio;
run;

*Step-4: Cleaning size data;

PROC IMPORT OUT= FCSep29.Fund_Size
            DATAFILE= "E:\Drive\Local Disk F\Mutual fund competition\US_set1.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Size";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

*converting Fund_Size from wide to long form;

proc transpose data=FCSep29.Fund_Size out=FCSep29.Fund_Size;
  by Group_Investment SecId FundId notsorted;
  var _all_;
run;

data FCSep29.Fund_Size;
	set FCSep29.Fund_Size;
	Date=input(_LABEL_,MMDDYY10.);
	format Date date9. ;
	if _label_ in ('Group/Investment','SecId','FundId','Base Currency','Domicile','Inception Date',
	'F247','F248','F249','F250','F251','F252','F253','F254','F255') then delete;
	drop _name_ _label_;
run;

data FCSep29.Fund_Size;
	set FCSep29.Fund_Size;
	if missing(fundid)=1 or missing(secid)=1 then delete;
	rename col1=Family_assets;
run;


**************************************************************************
					Fund Characteristics data
*************************************************************************;

PROC IMPORT OUT= FCSep29.Mstar_chars
            DATAFILE= "E:\Drive\Local Disk F\Mutual fund competition\US_set1.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Returns";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

*converting Fund_Returns from wide to long form;

proc transpose data=FCSep29.Mstar_chars out=FCSep29.Mstar_chars;
  by Group_Investment SecId FundId Inception_date notsorted;
  var _all_;
run;

data FCSep29.Mstar_chars;
	set FCSep29.Mstar_chars;
	Date=input(_LABEL_,MMDDYY10.);
	format Date date9. ;
	if _label_ in ('Group/Investment','SecId','FundId','Base Currency','Domicile',
	'F247','F248','F249','F250','F251','F252','F253','F254','F255') then delete;
	drop _name_ _label_ col1;
run;

data FCSep29.Mstar_chars;
	set FCSep29.Mstar_chars;
	if missing(fundid)=1 or missing(secid)=1 or missing(date)=1 then delete;
run;

**Including all the variables in FCSep29.Mstar_chars dataset;

proc sql;
	create table FCSep29.Mstar_chars as
	select distinct a.*,b.ret as mret_afxp
	from FCSep29.Mstar_chars as a left join FCSep29.Fund_returns as b
	on a.secid=b.secid and a.fundid=b.fundid and a.date=b.date;
quit; 

proc sql;
	create table FCSep29.Mstar_chars as
	select distinct a.*,b.Expense_ratio as expratio
	from FCSep29.Mstar_chars as a left join FCSep29.Expense_ratio as b
	on a.secid=b.secid and a.fundid=b.fundid and year(a.date)=input(b.date,12.);
quit; 

proc sql;
	create table FCSep29.Mstar_chars as
	select distinct a.*,b.Turnover_ratio as turnratio
	from FCSep29.Mstar_chars as a left join FCSep29.Turnover_ratio as b
	on a.secid=b.secid and a.fundid=b.fundid and year(a.date)=input(b.date,12.);
quit; 

proc sql;
	create table FCSep29.Mstar_chars as
	select distinct a.*,b.Family_assets as mtna
	from FCSep29.Mstar_chars as a left join FCSep29.Fund_size as b
	on a.secid=b.secid and a.fundid=b.fundid and a.date=b.date;
quit; 

*Calculating ret after expenses;

data FCSep29.Mstar_chars;
	set FCSep29.Mstar_chars;
	lag_mtna=lag(mtna); 
	label lag_mtna= "Fund Share Class Lagged TNA";
	if first.secid=1 then lag_mtna=.;
	if missing(expratio)=0 and missing(mret_afxp)=0 then mret_bexp=sum(mret_afxp,expratio/12); else mret_bexp=.;
	label mret_afxp="Monthly Returns, After Expense" mret_bexp="Monthly Returns, Before Expense";
run;
*7823040 obs;

/* Step 1: Aggregate multiple share classes, use lagged TNA as weights */
/* Compute monthly before and after expense returns, TNA, expense ratio, turnover ratio at the portfolio level */
/* for fund portfolios with multiple share classes */

data FCSep29.Mstar_chars; 
  set FCSep29.Mstar_chars;
  if mtna<0 then mtna=.;
  if lag_mtna<0 then lag_mtna=.;
run;

proc sql;
  create table Fund_Ret_Oneclass as
  select distinct *
  from FCSep29.Mstar_chars
  group by fundid, date
  having count(secid)=1;
quit;
*3899040 obs;

proc sql;
  create table Fund_Ret_Multiclass as
  select distinct *
  from FCSep29.Mstar_chars
  group by fundid, date
  having count(secid)>1
  order by fundid, date, secid;
quit;
*3924000 obs;


data Fund_Ret_Multiclass;
  set Fund_Ret_Multiclass;
  mtna1=(mtna*100)/100;
  lag_mtna1=(lag_mtna*100)/100;
  mret_afxp1=(mret_afxp*100)/100;
  mret_bexp1=(mret_bexp*100)/100;
  expratio1=(expratio*100)/100;
  turnratio1=(turnratio*100)/100;
  drop mtna lag_mtna mret_afxp mret_bexp expratio turnratio;
run;

data Fund_Ret_Multiclass;
  set Fund_Ret_Multiclass;
  rename mtna1=mtna; 
  rename lag_mtna1=lag_mtna;
  rename mret_afxp1=mret_afxp;
  rename mret_bexp1=mret_bexp;
  rename expratio1=expratio;
  rename turnratio1=turnratio;
run;

proc sql;
  create table Fund_Ret_Multiclass as
  select fundid, date, max(Group_Investment) as Group_Investment, min(Inception_Date) as Inception_Date format date9., sum(mtna) as mtna label='Total Net Assets as of Month End',
         sum(lag_mtna) as lag_mtna label='Total Net Assets as of Prior Month End',
         sum(mret_afxp*lag_mtna)/sum(lag_mtna) as mret_afxp label="Monthly Returns, After Expense",
         sum(mret_bexp*lag_mtna)/sum(lag_mtna) as mret_bexp label="Monthly Returns, Before Expense",
         sum(expratio*lag_mtna)/sum(lag_mtna) as expratio label='Expense Ratio as of Fiscal Year-End', 
		 sum(turnratio*lag_mtna)/sum(lag_mtna) as turnratio label='Turnover Ratio as of Fiscal Year-End', 
         count(secid) as nclass label='# of Share Classes'
  from Fund_Ret_Multiclass
  group by fundid, date;
quit;
*872640 obs;

data Fund_Ret_Oneclass;
  set Fund_Ret_Oneclass;
  mtna1=(mtna*100)/100;
  lag_mtna1=(lag_mtna*100)/100;
  mret_afxp1=(mret_afxp*100)/100;
  mret_bexp1=(mret_bexp*100)/100;
  expratio1=(expratio*100)/100;
  turnratio1=(turnratio*100)/100;
  drop mtna lag_mtna mret_afxp mret_bexp expratio turnratio;
run;

data Fund_Ret_Oneclass;
  set Fund_Ret_Oneclass;
  drop secid;
  nclass=1;
  rename mtna1=mtna; 
  rename lag_mtna1=lag_mtna;
  rename mret_afxp1=mret_afxp;
  rename mret_bexp1=mret_bexp;
  rename expratio1=expratio;
  rename turnratio1=turnratio;
  label expratio='Expense Ratio as of Fiscal Year-End';
  label turnratio='Turnover Ratio as of Fiscal Year-End';  
run;

data FCSep29.Fund_Ret; 
  set Fund_Ret_Multiclass Fund_Ret_Oneclass;
run;
*4771680 obs;

proc sort data=FCSep29.Fund_Ret nodupkey; by date fundid; run;


/* Step 2. Calculate Family Assets as of Month End */
/* Compute Family Assets Based on Actively Managed Funds in the Sample */
proc sql;
  create table FCSep29.Fund_Family_Size as
  select distinct fundid, date, Group_Investment, sum(mtna) as familyassets label='Family Assets as of Date ($Millions)'
  from FCSep29.Fund_Ret
  where missing(mtna)=0
  group by date, fundid;
quit; 

/* Step 3. Calculate Fund Age as of Month End */
proc sql;
  create table FCSep29.Fund_Age as
  select distinct fundid, date, Group_Investment, (date-Inception_Date)/365 as fundage label='Fund Age in Years as of Month End'
  from FCSep29.Fund_Ret
  group by fundid, date;
quit;
*864782 obs;
 
data FCSep29.Fund_Age;
  set FCSep29.Fund_Age;
  if fundage<0 then fundage=.;
run; 

/* Step 4. Merge Fund_Ret, Fund_Family_Size, and Fund_Age into Single Dataset */
proc sql;
  create table FCSep29.Fund_Chrs_Monthly as
  select distinct a.*, b.familyassets
  from FCSep29.Fund_Ret as a left join FCSep29.Fund_Family_Size as b
  on a.date=b.date and a.fundid=b.fundid;
  *865260 obs;

  create table FCSep29.Fund_Chrs_Monthly as
  select distinct a.*, b.fundage 
  from FCSep29.Fund_Chrs_Monthly as a left join FCSep29.Fund_Age as b
  on a.date=b.date and a.fundid=b.fundid;
quit;
proc sort data=FCSep29.Fund_Chrs_Monthly nodupkey; by date Group_Investment; run;

proc sort data=FCSep29.crspFund_Chrs_Monthly nodupkey; by date fund_name; run;

