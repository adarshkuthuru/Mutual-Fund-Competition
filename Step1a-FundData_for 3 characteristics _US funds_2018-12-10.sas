proc datasets library= work kill nolist; quit;

/*libname FCApr14 'D:\Mutual Funds - Holding Classification\SASData\FCApr14'; run;  ***The folder was created on Apr 14, 2013;*/
/*libname FCApr14 'D:\Data\Projects\Mutual Funds - Holding Classification\SASData\FCApr14'; run; ***On new comp;*/
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



/******************************/
/*** CRSP MUTUAL FUNDS DATA ***/
/******************************/

rsubmit;
/* Step 1.1. Extract CRSP Mutual Funds Performance and Characteristics */
/* Merge fraction of portfolio in equity assets with the objective code data */
proc sql;
  create table Fund_Style as 
  select a.crsp_fundno, a.begdt format=date9., a.enddt format=date9., 
         a.si_obj_cd, a.wbrger_obj_cd, a.policy, a.lipper_class, a.lipper_class_name, 
         a.lipper_obj_cd, b.avrcs
  from mf.fund_style as a left join (select distinct crsp_fundno, 
                                                     sum(per_com)/count(per_com) as avrcs 
                                                     label='%Avg Common Stocks Over Lifetime'
                                     from mf.fund_summary 
                                     group by crsp_fundno) as b
  on a.crsp_fundno=b.crsp_fundno;
quit;

*166686 obs;


/* Step 1.2. Sample Selection, focus on Domestic Equity Mutual Funds */
/* SCREENS */ 
data Equity_Funds; 
  set Fund_Style;
  if missing(Lipper_Class)=0 and lipper_class not in ('EIEI','LCCE','LCGE','LCVE','MCCE','MCGE','MCVE',
     'MLCE','MLGE','MLVE','SCCE','SCGE','SCVE') then delete;
  else if missing(Lipper_Class)=1 
       then do;
              if missing(si_obj_cd)=0 and si_obj_cd not in ('AGG','GMC','GRI','GRO','ING','SCG') 
              then delete;
              else if missing(si_obj_cd)=1 
                   then do;
                          if missing(wbrger_obj_cd)=0 and 
                             wbrger_obj_cd not in ('G','G-I','GCI','LTG','MCG','SCG') 
                          then delete;
  						  else if missing(wbrger_obj_cd)=1 
                               then do;
    								  if missing(policy)=0 and policy ne 'CS' 
                                      then delete;
     								  else if missing(policy)=1 and avrcs<80 
                                           then delete;
                                    end; 
                        end; 
            end;
run;
*59528 obs;

/* Step 1.3. Fund Header History on Name, and Mgmt Changes */
/* SCREEN: Exclude Index or Index Based Funds */ 
proc sql;
  create table Fund_Hdr_Hist as
  select distinct a.crsp_fundno, a.chgdt format=date9., a.chgenddt format=date9., a.fund_name, a.mgmt_name,
                  a.ncusip, a.first_offer_dt format=date9., 
                  a.mgmt_name, a.mgmt_cd, a.adv_name, a.index_fund_flag /*a.nasdaq */
  from mf.Fund_Hdr_Hist as a, (select distinct crsp_fundno from Equity_Funds) as b
  where a.crsp_fundno=b.crsp_fundno;
quit;
*171942 obs;

data Fund_Hdr_Hist;
  set Fund_Hdr_Hist;
  if index_fund_flag in ('B','D','E') then delete;
  if find(fund_name,'INDEX','i')>0 or find(fund_name,'S&P')>0 or find(fund_name,'ENHANCED')>0 or find(fund_name,'INDX','i')>0 or
     find(fund_name,'IDX','i')>0 or find(fund_name,'VANGUARD','i')>0 or find(fund_name,'BALANCED','i')>0 or find(fund_name,'ETF','i')>0 or
     index(fund_name,'400')>0 or index(fund_name,'500')>0 or index(fund_name,'600')>0 or find(fund_name,'1000','i')>0  then delete;
run;
*161910 obs;

/*proc sort data=Fund_Hdr_Hist nodupkey; by crsp_fundno; run;*/

*Update Equity_Funds to exclude index funds;
proc sql;
  create table Equity_Funds as
  select distinct a.*,max(b.fund_name) as fund_name, max(b.mgmt_name) as mgmt_name
  from Equity_Funds as a, Fund_Hdr_Hist as b
  where a.crsp_fundno=b.crsp_fundno;
quit;
*54681 obs;
/*proc download data=Fund_Hdr_Hist out=Fund_Hdr_Hist; run;*/

/* Step 1.4. Get CRSP Mutual Fund, Monthly Net Returns for Funds in Equity_Funds in Step 1.2 */
/* Net Returns include distributions (dividends and capital gains) */
/* and are net of total expenses */
proc sql;
  create table Fund_Ret as
  select distinct a.crsp_fundno, b.fund_name, b.mgmt_name, intnx("month",a.caldt,0,"e") as date format=date9., a.mtna, a.mret as mret_afxp
  from mf.Monthly_Tna_Ret_Nav as a, (select distinct crsp_fundno, fund_name, mgmt_name from Equity_Funds) as b
  where a.crsp_fundno=b.crsp_fundno and a.caldt>=&crspbegdate and missing(a.mret)=0;

  create table Fund_Ret as
  select distinct a.*, b.exp_ratio, b.turn_ratio, b.mgmt_fee
  from Fund_Ret as a left join mf.Fund_Fees as b
  on a.crsp_fundno=b.crsp_fundno and intnx("month",b.begdt,0,"e")<a.date<=intnx("month",b.enddt,0,"e");
quit;
*2393737 obs;


/* Step 1.5. Get Before Expense Total Portfolio Return by Adding Back Expenses */
proc sort data=Fund_Ret nodupkey; by crsp_fundno date; run;
data Fund_Ret;
  format crsp_fundno fund_name mgmt_name date mtna lag_mtna mret_afxp mret_bexp;
  set Fund_Ret;
  by crsp_fundno date; 
  if exp_ratio=-99 then exp_ratio=.;
  if turn_ratio=-99 then turn_ratio=.;
  lag_mtna=lag(mtna); 
  label lag_mtna= "Fund Share Class Lagged TNA";
  if first.crsp_fundno=1 then lag_mtna=.;
  if missing(exp_ratio)=0 and missing(mret_afxp)=0 then mret_bexp=sum(mret_afxp,exp_ratio/12); else mret_bexp=.;
  label mret_afxp="Monthly Returns, After Expense" mret_bexp="Monthly Returns, Before Expense";
run;
*2393737 obs;

/* Step 1.6. Merge Fund_Ret, Equity_Funds, and Fund_Hdr_Hist with Portfolio Identifier from MFLIash. */
proc sql;
  create table Fund_Ret as 
  select b.wficn, a.*
  from Fund_Ret as a, mfl.mflink1 as b
  where a.crsp_fundno=b.crsp_fundno and missing(b.wficn)=0
  order by wficn, date, crsp_fundno;
  *2074536 obs;

  create table Equity_Funds as
  select distinct b.wficn, a.*
  from Equity_Funds as a, mfl.mflink1 as b 
  where a.crsp_fundno=b.crsp_fundno and missing(b.wficn)=0
  order by wficn, crsp_fundno, begdt;
  *45037 obs;

  create table Fund_Hdr_Hist as
  select distinct b.wficn, a.*
  from Fund_Hdr_Hist as a, mfl.mflink1 as b 
  where a.crsp_fundno=b.crsp_fundno and missing(b.wficn)=0
  order by wficn, crsp_fundno, chgdt;
  *137015 obs;

quit;

/* Step 1.7. Aggregate multiple share classes, use lagged TNA as weights */
/* Compute monthly before and after expense returns, TNA, expense ratio, turnover ratio at the portfolio level */
/* for fund portfolios with multiple share classes */
data Fund_Ret; 
  set Fund_Ret;
  if mtna<0 then mtna=.;
  if lag_mtna<0 then lag_mtna=.;
run;
*2074536 obs;

proc sql;
  create table Fund_Ret_Oneclass as
  select distinct *
  from Fund_Ret
  group by wficn, date
  having count(crsp_fundno)=1;
quit;
*430963 obs;

proc sql;
  create table Fund_Ret_Multiclass as
  select distinct *
  from Fund_Ret
  group by wficn, date
  having count(crsp_fundno)>1
  order by wficn, date, crsp_fundno;
quit;
*1643573 obs;

proc sql;
  create table Fund_Ret_Multiclass as
  select wficn, max(fund_name) as fund_name, max(mgmt_name) as mgmt_name, date, sum(mtna) as mtna label='Total Net Assets as of Month End',
         sum(lag_mtna) as lag_mtna label='Total Net Assets as of Prior Month End',
         sum(mret_afxp*lag_mtna)/sum(lag_mtna) as mret_afxp label="Monthly Returns, After Expense",
         sum(mret_bexp*lag_mtna)/sum(lag_mtna) as mret_bexp label="Monthly Returns, Before Expense",
         sum(exp_ratio*lag_mtna)/sum(lag_mtna) as expratio label='Expense Ratio as of Fiscal Year-End', 
		 sum(turn_ratio*lag_mtna)/sum(lag_mtna) as turnratio label='Turnover Ratio as of Fiscal Year-End', 
         count(crsp_fundno) as nclass label='# of Share Classes',
		 sum(mgmt_fee*lag_mtna)/sum(lag_mtna) as mgmtfee label='Mgmt Fee as of Fiscal Year-End'
  from Fund_Ret_Multiclass
  group by wficn, date;
quit;
*433819 obs;


data Fund_Ret_Oneclass;
  set Fund_Ret_Oneclass;
  drop crsp_fundno;
  nclass=1;
  rename exp_ratio=expratio;
  label exp_ratio='Expense Ratio as of Fiscal Year-End';
  rename turn_ratio=turnratio;
  label turn_ratio='Turnover Ratio as of Fiscal Year-End';  
  rename mgmt_fee=mgmtfee;
run;
*430963 obs;

data Fund_Ret; 
  set Fund_Ret_Multiclass Fund_Ret_Oneclass;
run;
*864782 obs;

proc sort data=Fund_Ret nodupkey; by date wficn; run;

/* Step 1.8. Calculate Family Assets as of Month End */
/* Compute Family Assets Based on Actively Managed Funds in the Sample */
proc sql;
  create table Fund_Family as
  select distinct a.wficn, max(b.fund_name) as fund_name, max(b.mgmt_name) as mgmt_name, a.date, b.mgmt_cd
  from Fund_Ret as a, Fund_Hdr_Hist as b
  where a.wficn=b.wficn and missing(b.mgmt_cd)=0 and 
        intnx("month",b.chgdt,0,"e")<a.date<=intnx("month",b.chgenddt,0,"e");
  *465284 obs;

  *All fund classes managed by mgmt_cd as of date;
  create table Fund_Family_Size as
  select distinct a.date, a.mgmt_cd, b.crsp_fundno /*b.fund_name, b.mgmt_name */
  from (select distinct date, mgmt_cd from Fund_Family) as a, mf.Fund_Hdr_Hist as b
  where a.mgmt_cd=b.mgmt_cd and missing(b.mgmt_cd)=0 and 
        intnx("month",b.chgdt,0,"e")<a.date<=intnx("month",b.chgenddt,0,"e");
  *3346893 obs;

  *Size of all fund classes;
  create table Fund_Family_Size as
  select distinct a.*, b.mtna 
  from Fund_Family_Size as a left join mf.monthly_tna as b
  on a.crsp_fundno=b.crsp_fundno and year(a.date)=year(b.caldt) and month(a.date)=month(b.caldt);

  create table Fund_Family_Size as
  select distinct date, mgmt_cd, sum(mtna) as familyassets label='Family Assets as of Date ($Millions)' /*fund_name, mgmt_name, */
  from Fund_Family_Size
  where missing(mtna)=0
  group by date, mgmt_cd;
  *86625 obs;

  create table Fund_Family_Size as 
  select distinct a.*, b.familyassets
  from Fund_Family as a, Fund_Family_Size as b
  where a.date=b.date and a.mgmt_cd=b.mgmt_cd;
  *464988 obs;

  drop table Fund_Family;
quit; 

/* Step 1.9. Calculate Fund Age as of Month End */
proc sql;
  create table Fund_Age as
  select distinct a.wficn, a.date, b.earliest_first_offer_dt, 
                  (a.date-b.earliest_first_offer_dt)/365 as fundage label='Fund Age in Years as of Month End'
  from (select distinct wficn, date from Fund_Ret) as a, 
       (select distinct wficn, min(first_offer_dt) as earliest_first_offer_dt
	    from Fund_Hdr_Hist
		group by wficn) as b
  where a.wficn=b.wficn;
quit;
*864782 obs;
 
data Fund_Age;
  set Fund_Age;
  if fundage<0 then fundage=.;
run; 


/* Step 1.10. Merge Fund_Ret, Fund_Family_Size, and Fund_Age into Single Dataset */
proc sql;
  create table Fund_Chrs_Monthly as
  select distinct a.*, b.mgmt_cd, b.familyassets
  from Fund_Ret as a left join Fund_Family_Size as b
  on a.date=b.date and a.wficn=b.wficn;
  *865260 obs;

  create table Fund_Chrs_Monthly as
  select distinct a.*, b.fundage 
  from Fund_Chrs_Monthly as a left join Fund_Age as b
  on a.date=b.date and a.wficn=b.wficn;
quit;
proc sort data=Fund_Chrs_Monthly nodupkey; by date wficn; run;
data Fund_Chrs_Monthly;
  format wficn fund_name date mret_afxp mret_bexp expratio turnratio fundage mtna lag_mtna nclass mgmt_cd familyassets mgmtfee;
  set Fund_Chrs_Monthly;
run; 
*864782 obs;

data ash.Fund_Chrs_Monthly;
	set Fund_Chrs_Monthly;
run;

proc sql;
  drop table Fund_Style, Fund_Ret_Multiclass, Fund_Ret_Oneclass, Fund_Family_Size, Fund_Age, Fund_Ret;
quit;


/* Step 1.11. Unique Fund Classifications */
proc sql;
  create table Fund_Style as
  select distinct wficn, begdt, enddt, lipper_class, lipper_class_name, lipper_obj_cd, si_obj_cd, wbrger_obj_cd, policy
  from Equity_Funds;
quit;
*22945 obs;

proc sort data=Fund_Style nodupkey out=temp; by wficn begdt; run;
*21722 obs;
proc sql;
  create table temp1 as
  select distinct a.* from Fund_Style as a
  except
  select distinct b.* from temp as b;
  *1223 obs;

  create table Fund_Style_Dup as
  select distinct a.*
  from Fund_Style as a, temp1 as b
  where a.wficn=b.wficn and a.begdt=b.begdt;
  *2322 obs;
quit;

*Resolve duplicates;
proc sql;
  create table Fund_Style_Nodup as
  select distinct a.* from Fund_Style as a
  except
  select distinct b.* from Fund_Style_Dup as b;
quit;
*20623 obs;

data Fund_Style_Dup_Lipper Fund_Style_Dup_Other;
  set Fund_Style_Dup;
  if missing(lipper_class)=0 and lipper_class in ('EIEI','G','LCCE','LCGE','LCVE','MCCE',
     'MCGE','MCVE','MLCE','MLGE','MLVE','SCCE','SCGE','SCVE')
  then output Fund_Style_Dup_Lipper;
  else output Fund_Style_Dup_Other;
run;

proc transpose data=Fund_Style_Dup_Lipper out=Fund_Style_Dup_Lipper1;
  by wficn begdt;
  var lipper_class;
run;
*984 wficn-begdt combos;

data Check;
  set Fund_Style_Dup_Lipper1;
  if missing(col1)=0 and missing(col2)=0 and missing(col3)=0 and col1=col2=col3 then output check;
  if missing(col1)=0 and missing(col2)=0 and missing(col3)=1 and col1=col2 then output check;
run;
*959 wficn-begdt combos, so lipper cases are more or less fine;

proc sql;
  create table Fund_Style_Dup_Lipper_Corrected as
  select distinct wficn, begdt, max(enddt) as enddt format=date9., lipper_class, lipper_class_name, lipper_obj_cd,
                  si_obj_cd, wbrger_obj_cd, policy
  from Fund_Style_Dup_Lipper
  group by wficn, begdt;
quit;
proc sort data=Fund_Style_Dup_Lipper_Corrected nodupkey; by wficn begdt; run;
*984 wficn-begdt combos;

proc sql;
  create table Fund_Style_Dup_Other_Corrected as
  select distinct wficn, begdt, max(enddt) as enddt format=date9., lipper_class, lipper_class_name, lipper_obj_cd,
                  si_obj_cd, wbrger_obj_cd, policy
  from Fund_Style_Dup_Other
  group by wficn, begdt;
quit;
proc sort data=Fund_Style_Dup_Other_Corrected nodupkey; by wficn begdt; run;
*115 wficn-begdt combos;

*Combine non-dup wficn-begdt combos;
data Fund_Style;
  set Fund_Style_Nodup Fund_Style_Dup_Lipper_Corrected Fund_Style_Dup_Other_Corrected;
run;
*21722 obs;

proc sort data=Fund_Style nodupkey; by wficn begdt; run;
proc sql;
  drop table temp, temp1, Fund_Style_Nodup, Fund_Style_Dup, Fund_Style_Dup_Lipper, Fund_Style_Dup_Other,
             Fund_Style_Dup_Lipper1, Fund_Style_Dup_Lipper_Corrected, Fund_Style_Dup_Other_Corrected, Check;
quit;
/*proc download data=Fund_Chrs_Monthly out=Fund_Chrs_Monthly; run;*/
/*proc download data=Fund_Style out=Fund_Style; run;*/


/************************************/
/*** Thomson Reuters Holding Data ***/
/************************************/

/* Step 2.1. Extract Thomson Reuters fund-rdate combinations from S12type1 */
proc sql;
   create table tfnfunds as
   select distinct fundno, intnx("month",rdate,0,"E") as rdate format=date9. label='Report Date', fdate format=date9.,
                   intnx("qtr",rdate,0,"e") as qdate format=date9. label='Qtr End Date (corr. to rdate)'
   from s12.s12type1
   where year(&tfnbegdate)<=year(rdate)<=year(&tfnenddate) and ioc not in (1,5,6,7,8)
   group by fundno, intnx("month",rdate,0,"E")
   having fdate=min(fdate)
   order by fundno, rdate; 
quit;
*692498 obs;

*For each fundno-qdate, select the latest rdate in case there are two rdate in a qtr;
proc sql;
   create table tfnfunds as
   select distinct *
   from tfnfunds
   group by fundno, qdate
   having rdate=max(rdate)
   order by fundno, rdate;
quit; 
*688819 obs;


/* Step 2.2. Merge tfnfunds with Portfolio Identifier from MFLINKS */
proc sql;
   create table tfnfunds as
   select distinct b.wficn, a.fundno, a.rdate, a.fdate, a.qdate
   from tfnfunds as a, mfl.mflink2 as b
   where a.fundno=b.fundno and a.fdate=b.fdate and missing(b.wficn)=0
   order by wficn, rdate;
quit; 
*238898 obs;

*Sanity checks;
proc sort data=tfnfunds nodupkey; by wficn rdate; run; 
*238631 obs;
proc sort data=tfnfunds nodupkey; by wficn qdate; run;
/* Step 2.3. Extract Holdings by Merging tfnfunds and S12type3 */
proc sql;
    create view holding1 as
    select distinct a.*, b.cusip, b.shares as shares_fdate label='Shares (fdate)'
    from tfnfunds as a, s12.s12type3 as b
    where a.fdate=b.fdate and a.fundno=b.fundno;

    * Select permno, cusip from CRSP;
    create view cusip_permno as
	select distinct ncusip, permno
	from crspa.msenames
	where missing(ncusip)=0; 

    * Map TR-MF's Historical CUSIP to CRSP Unique Identifier PERMNO;
    create view holding2 as
    select distinct a.wficn, a.fundno, a.rdate, a.fdate, a.qdate, b.permno, a.shares_fdate         
    from holding1 as a, cusip_permno as b
    where a.cusip=b.ncusip; 
quit;


/* Step 2.4. Extract quarterly holdings by first adjusting shares as of rdate from fdate if rdate^=fdate */
/* and then from rdate to qdate if rdate^=qdate */
proc sql;    
    create view holding2a as
    select distinct a.*, b.dateff as dateff_fdate format=date9. /*Shrout on dateff are same as shrout on fdate*/
    from holding2 as a left join ff.factors_monthly as b
    on year(a.fdate)=year(b.dateff) and month(a.fdate)=month(b.dateff);

    create view holding2b as
    select distinct a.*, b.dateff as dateff_rdate format=date9.  
    from holding2a as a left join ff.factors_monthly as b
    on year(a.rdate)=year(b.dateff) and month(a.rdate)=month(b.dateff);

    create view holding2c as
    select distinct a.*, b.dateff as dateff_qdate format=date9.  
    from holding2b as a left join ff.factors_monthly as b
    on year(a.qdate)=year(b.dateff) and month(a.qdate)=month(b.dateff);
quit;
proc sql;  
    create view holding2d as
    select distinct a.*, b.cfacshr as cfacshr_fdate label='Adj. Factor (fdate)'
    from holding2c as a left join crspa.msf as b
    on a.permno=b.permno and a.dateff_fdate=b.date;

    create view holding2e as
    select distinct a.*, b.cfacshr as cfacshr_rdate label='Adj. Factor (rdate)'
    from holding2d as a left join crspa.msf as b
    on a.permno=b.permno and a.dateff_rdate=b.date;

    create view holding2f as
    select distinct a.*, b.cfacshr as cfacshr_qdate label='Adj. Factor (qdate)'
    from holding2e as a left join crspa.msf as b
    on a.permno=b.permno and a.dateff_qdate=b.date;
quit;
proc sql; 
    create view holding2g as
    select distinct *,
           case
             when fdate=rdate then shares_fdate
             when fdate^=rdate and missing(cfacshr_fdate)=0 and missing(cfacshr_rdate)=0 
                  then shares_fdate*(cfacshr_fdate/cfacshr_rdate)
			 when fdate^=rdate and (missing(cfacshr_fdate)=1 or missing(cfacshr_rdate)=1) then .                  
           end as adjshares_rdate label='Adjusted Shares (rdate)'
    from holding2f;

    create view holding2h as
    select distinct *,
           case
             when rdate=qdate then adjshares_rdate
             when rdate^=qdate and missing(cfacshr_rdate)=0 and missing(cfacshr_qdate)=0 
                  then adjshares_rdate*(cfacshr_rdate/cfacshr_qdate)
			 when rdate^=qdate and (missing(cfacshr_rdate)=1 or missing(cfacshr_qdate)=1) then .                  
           end as adjshares_qdate label='Adjusted Shares (qdate)'
    from holding2g;
quit;
proc sql;
  create table fund_holdings_qtrly as
  select distinct wficn, rdate, qdate, permno, adjshares_rdate, adjshares_qdate
  from holding2h
  where missing(adjshares_rdate)=0;
quit;
* 27705301 obs;

proc sort data=fund_holdings_qtrly nodupkey; by qdate wficn permno; run;
/*data ash.fund_holdings_qtrly; set fund_holdings_qtrly; run;*/


/* Step 2.5. Calculate portfolio as of rdate */
proc sql;
  create table fund_holdings_qtrly as
  select distinct a.*, a.adjshares_rdate*abs(b.prc) as value
  from fund_holdings_qtrly as a, crspa.msf as b
  where a.permno=b.permno and year(a.rdate)=year(b.date) and month(a.rdate)=month(b.date);

  create table fund_holdings_qtrly as
  select distinct wficn, rdate, qdate, permno, adjshares_rdate, adjshares_qdate, value/sum(value) as wt_rdate
  from fund_holdings_qtrly
  group by wficn, rdate;
quit;
*27673815 obs;

proc sort data=fund_holdings_qtrly nodupkey; by qdate wficn permno; run;


/* Step 2.6. Adjust qtrly holding by bringing last qtr portfolio ahead if no portfolio disclosed this qtr */
proc sql;
  create table temp as
  select distinct wficn, qdate
  from fund_holdings_qtrly; 
quit;
*215681 obs;
 
proc sort data=temp; by wficn descending qdate; run;
data temp;
  set temp;
  by wficn descending qdate;
  lead_qdate=lag(qdate);
  if first.wficn=1 then lead_qdate=.;
  format lead_qdate date9.;
  label lead_qdate='Lead qdate';

  if year(lead_qdate)=year(qdate) then diffmonth=month(lead_qdate)-month(qdate);
  else if year(lead_qdate)>year(qdate) then diffmonth=(month(lead_qdate)-month(qdate)) + 
                                                        12*(year(lead_qdate)-year(qdate));
  label diffmonth='Diff b/w qdate & lead_qdate';
run;
*215681 obs;
data temp1 temp2;
  set temp;
  if diffmonth=6 then output temp1;
  else output temp2;
run;
*Select portfolios as of qdate for temp1;
proc sql;
  create table temp1_qdate_port as
  select distinct a.*
  from fund_holdings_qtrly as a, temp1 as b
  where a.qdate=b.qdate and a.wficn=b.wficn;
quit;
*4693388 obs;
 
*Bring portfolios as of qdate ahead by 3 months;
data temp1;
  set temp1;
  nqdate=intnx("month",qdate,3,"E");
  format nqdate date9.;
  drop lead_qdate diffmonth;
run;
*36118 obs;

proc sql;
  create table temp1a as
  select distinct a.wficn, b.rdate, a.qdate, a.nqdate, b.permno, b.adjshares_rdate, b.adjshares_qdate, b.wt_rdate
  from temp1 as a, temp1_qdate_port as b
  where a.qdate=b.qdate and a.wficn=b.wficn;
quit;
*Adjustment factors as of qdate and nqdate;
proc sql;
  create table temp1a as
  select distinct a.*, b.dateff as dateff_qdate format=date9.         
  from temp1a as a left join ff.factors_monthly as b
  on year(a.qdate)=year(b.dateff) and month(a.qdate)=month(b.dateff);

  create table temp1a as
  select distinct a.*, b.dateff as dateff_nqdate format=date9.         
  from temp1a as a left join ff.factors_monthly as b
  on year(a.nqdate)=year(b.dateff) and month(a.nqdate)=month(b.dateff);
quit;
*4693388 obs;

proc sql;
  create table temp1a as
  select distinct a.*, b.cfacshr as cfacshr_qdate label='Adj. Factor (qdate)'                  
  from temp1a as a left join crspa.msf as b
  on a.permno=b.permno and a.dateff_qdate=b.date;

  create table temp1a as
  select distinct a.*, b.cfacshr as cfacshr_nqdate label='Adj. Factor (nqdate)'                  
  from temp1a as a left join crspa.msf as b
  on a.permno=b.permno and a.dateff_nqdate=b.date;
quit;
*4693388 obs;

proc sql;
  create table temp1a as
  select distinct wficn, rdate, qdate, nqdate, permno, adjshares_rdate, adjshares_qdate, wt_rdate, 
                  case
                  when missing(cfacshr_qdate)=0 and missing(cfacshr_nqdate)=0 then 
                       (adjshares_qdate)*(cfacshr_qdate/cfacshr_nqdate)
                  when missing(cfacshr_qdate)=1 or missing(cfacshr_nqdate)=1 then .
                  end as adjshares_nqdate 
                  label='Adjshares held at nqdate'
  from temp1a;
quit;
*4693388 obs;

data temp1a; *temp1a dataset contains the portfolios brought ahead;
  set temp1a;
  drop qdate adjshares_qdate;
  rename nqdate=qdate;
  rename adjshares_nqdate=adjshares_qdate;
  broughtahead=1; *indicator variable for portfolio brought ahead;
run;
*4693388 obs;

/* Step 2.7. Combine quarterly portfolios with portfolios brough ahead */
data fund_holdings_qtrly;
  set fund_holdings_qtrly;
  broughtahead=0;
run;
data fund_holdings_qtrly;
  set fund_holdings_qtrly temp1a;
run;
*32367203 obs;

proc sort data=fund_holdings_qtrly nodupkey; by qdate wficn permno; run;



/* Step 2.8. SCREEN: Remove funds in their first quarter */
proc sql;
  create table tfn_fund_qtrs as
  select distinct wficn, rdate, qdate, broughtahead
  from fund_holdings_qtrly
  group by wficn
  having qdate>min(qdate);
quit;
*245064 obs;

/* Step 2.9. SCREEN: Remove funds with less than 10 stocks in the qtr */
proc sql;
  create table tfn_fund_qtrs as
  select distinct a.*, count(distinct b.permno) as nstocks label='No. of Stocks held (qdate)'
  from tfn_fund_qtrs as a, fund_holdings_qtrly as b
  where a.wficn=b.wficn and a.qdate=b.qdate
  group by b.wficn, b.qdate
  having count(distinct b.permno)>=10;
quit;
*226074 obs;

/* Step 2.10. SCREEN: Remove funds with size of less than $5 million */
proc sql;
  create table tfn_fund_qtrs as 
  select distinct a.*, b.mtna label='Total Net Assets as of qdate'
  from tfn_fund_qtrs as a left join fund_chrs_monthly as b
  on a.wficn=b.wficn and year(a.qdate)=year(b.date) and month(a.qdate)=month(b.date);
quit;
*226074 obs;

data tfn_fund_qtrs;
  set tfn_fund_qtrs;
  if missing(mtna)=0 and mtna<=5 then delete;
run;
*219324 obs;


/* Step 2.11. Update fund_holdings_qtrly and house cleaning */
proc sql;
  create table fund_holdings_qtrly as
  select distinct a.wficn, a.qdate, a.permno, a.wt_rdate
  from fund_holdings_qtrly as a, tfn_fund_qtrs as b
  where a.wficn=b.wficn and a.qdate=b.qdate;
quit;
*31173993 obs;

proc sort data=fund_holdings_qtrly nodupkey; by qdate wficn permno; run;
*House cleaning;
proc sql;
  drop view cusip_permno, holding1, holding2, holding2a, holding2b, holding2c, holding2d, holding2e, holding2f, 
            holding2g, holding2h;
  drop table temp, temp1, temp1a, temp1_qdate_port, temp2, tfnfunds;
quit;


/*******************************************/
/*** Match CRSP and TFN Holding Datasets ***/
/*******************************************/

/* Step 3.1. Make a common list of matched funds */
proc sql;
  create table crsp_tfn_matched_funds as
  select distinct a.wficn
  from (select distinct wficn from fund_chrs_monthly) as a, (select distinct wficn from tfn_fund_qtrs) as b
  where a.wficn=b.wficn;
quit;
*3767 obs;

/* Step 3.2. Update CRSP datasets from the matched list of funds */
proc sql;
  create table Equity_Funds as
  select distinct a.*
  from Equity_Funds as a, crsp_tfn_matched_funds as b
  where a.wficn=b.wficn;
  *41447 obs;

  create table Fund_Hdr_Hist as
  select distinct a.*
  from Fund_Hdr_Hist as a, crsp_tfn_matched_funds as b
  where a.wficn=b.wficn;
  *117164 obs;

  create table Fund_Style as
  select distinct a.*
  from Fund_Style as a, crsp_tfn_matched_funds as b
  where a.wficn=b.wficn;
  *19849 obs;

  create table Fund_Chrs_Monthly as
  select distinct a.*
  from Fund_Chrs_Monthly as a, crsp_tfn_matched_funds as b
  where a.wficn=b.wficn;
  *711546 obs;
quit;


/* Step 3.3. Update TFN datasets from the matched list of funds */
proc sql;
  create table Tfn_Fund_Qtrs as
  select distinct a.*
  from Tfn_Fund_Qtrs as a, crsp_tfn_matched_funds as b
  where a.wficn=b.wficn;
  *166594 obs;

  create table Fund_Holdings_Qtrly as
  select distinct a.*
  from Fund_Holdings_Qtrly as a, Tfn_Fund_Qtrs as b
  where a.wficn=b.wficn and a.qdate=b.qdate;
  *19388591 obs;
quit;


/* Step 3.4. Include SICCD in Fund_Holdings_Qtrly */
proc sql;
  create table SICCD as
  select distinct permno, NAMEDT, NAMEENDT, SICCD, HSICCD
  from crspa.MSENAMES;
  *97239 obs;

  create table Fund_Holdings_Qtrly as
  select distinct a.*, b.SICCD
  from Fund_Holdings_Qtrly as a left join SICCD as b
  on a.permno=b.permno and b.NAMEDT<=a.qdate<=b.NAMEENDT;
  *19388591 obs;
quit;
proc sort data=Fund_Holdings_Qtrly; by qdate wficn permno; run;

proc sql;
  drop table SICCD, CRSP_TFN_Matched_Funds;
quit; 

proc datasets nolist;
  copy in=work out=ASH;
  select Equity_Funds Fund_Chrs_Monthly Fund_Hdr_Hist Fund_Style Fund_Holdings_Qtrly Tfn_Fund_Qtrs;
quit;
/*
rsubmit;
proc datasets nolist;
  copy in=ash.out=Work;
  select Equity_Funds Fund_Hdr_Hist Fund_Style Fund_Chrs_Monthly Tfn_Fund_Qtrs Fund_Holdings_Qtrly;
quit;
endrsubmit;
*/

/**********************************************************************/
/*** Calculate stock level size, raw bm, ind-adj bm, momentum chrs  ***/
/**********************************************************************/

/* Step 4.1. Extract stocks listed on NYSE, AMEX and Nasdaq at the end of each quarter and obtain log(size) */
proc sql;
  create table CRSP_StkQtrs_Size as
  select distinct intnx("month",a.date,0,"E") as qdate format=date9. label='Qtr End Date', a.permno, b.EXCHCD, 
                  log((abs(a.prc)*a.shrout*1000)/1000000) as LogSize label='Log of Size ($Million)'
  from crspa.msf as a, crspa.mseall as b
  where a.permno=b.permno and year(a.date)=year(b.date) and month(a.date)=month(b.date) and month(a.date) in (3,6,9,12) and
        year(&crspbegdate)<=year(a.date)<=year(&crspenddate) and missing(a.prc)=0 and missing(a.shrout)=0 and (abs(a.prc)*a.shrout)>0 and
        b.EXCHCD in (1,2,3) and b.SHRCD in (10,11);
quit;
*684157 obs;

/* Step 4.2 For stk-qtrs extracted in 4.1, calculate momentum */
data CRSP_StkQtrs_Mom;
  set CRSP_StkQtrs_Size;
  date_12=intnx("month",qdate,-12,"E");
  format date_12 date9.;
run;  
*684157 obs;
proc sql;
  create table CRSP_StkQtrs_Mom as
  select distinct a.*, b.date format date9., b.ret
  from CRSP_StkQtrs_Mom as a, crspa.msf as b
  where a.permno=b.permno and a.date_12<b.date<a.qdate and missing(b.ret)=0;
  *Past 11 months return exluding the quarter ending month;
  * 7322504 obs;

  create table CRSP_StkQtrs_Mom as
  select distinct qdate, permno, EXCHCD, exp(sum(log(1+ret))) - 1 as Mom label='Prior 11 Month CumRet'
  from CRSP_StkQtrs_Mom
  group by qdate, permno
  having count(date)>=10; *at least 10 months of ret should be there;
  *629220 obs;

quit;

*Winsorize Mom at 1/99 percentile;
proc univariate data=CRSP_StkQtrs_Mom noprint;
  by qdate;
  var Mom;
  output out=MomPctl pctlpts=1 99 pctlpre=Mom;
quit;
*138 obs;
proc sql;
  create table CRSP_StkQtrs_Mom as
  select distinct a.*, b.Mom1, b.Mom99
  from CRSP_StkQtrs_Mom as a, MomPctl as b
  where a.qdate=b.qdate;
quit;
* 629220 obs;

data CRSP_StkQtrs_Mom;
  set CRSP_StkQtrs_Mom;
  if missing(Mom)=0 and Mom<Mom1 then Mom=Mom1;
  if missing(Mom)=0 and Mom>Mom99 then Mom=Mom99;
  drop Mom1 Mom99;
run;



/* Step 4.3. Calculate raw and industry-adjusted BM Ratio */
/* Step 4.3.1. Obtain Book-Equity from Compustat */
%let crspbegdate = '01Jan1979'd; *redfine crspbegdate to obtain BE for 1980;
data comp_extract;
   format gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc TXDITC BE0 BE1 EPSPX EBITDA SALE DVC CSHO;
   set compa.funda
   (where=(fyr>0 and at>0 and consol='C' and
           indfmt='INDL' and datafmt='STD' and popsrc='D'));

   **BE as in Daniel and Titman (JF, 2006); 
   *Shareholder equity(she);
   if missing(SEQ)=0 then she=SEQ; 
   *SEQ=Stockholders Equity (Total);
   else if missing(CEQ)=0 and missing(PSTK)=0 then she=CEQ+PSTK; 
        *CEQ=Common/Ordinary Equity (Total), PSTK=Preferred/Preference Stock (Capital) (Total);
        else if missing(AT)=0 and missing(LT)=0 then she=AT-LT;
		     *AT=Assets (Total), LT=Liabilities (Total), MIB=Minority Interest (Balance Sheet); 
             else she=.;

   if missing(PSTKRV)=0 then BE0=she-PSTKRV; *PSTKRV=Preferred Stock (Redemption Value);
   else if missing(PSTKL)=0 then BE0=she-PSTKL; *PSTKL=Preferred Stock (Liquidating Value);
        else if missing(PSTK)=0 then BE0=she-PSTK; *PSTK=Preferred/Preference Stock (Capital) (Total);
             else BE0=.;
  
   **BE as in Kayhan and Titman (2003);
   *Book_Equity = Total Assets - [Total Liabilities + Preferred Stock] + Peferred Taxes + Conv. Debt;  
   if AT>0 and LT>0 then BE1=AT-(LT+PSTKL)+TXDITC+DCVT;
   else BE1=.;

   *Converts fiscal year into calendar year data;
   if (1<=fyr<=5) then date_fyend=intnx('month',mdy(fyr,1,fyear+1),0,'end');
   else if (6<=fyr<=12) then date_fyend=intnx('month',mdy(fyr,1,fyear),0,'end');
   calyear=year(date_fyend);
   format date_fyend date9.;

   *Accounting data since calendar year 't-1';
   if year(&crspbegdate)-1<=year(date_fyend)<=year(&crspenddate)+1;
   keep gvkey datadate date_fyend calyear fyear fyr indfmt consol datafmt popsrc TXDITC BE0 BE1 EPSPX EBITDA SALE DVC CSHO;
   *Note that datadate is same as date_fyend;
run;
*329843 obs;

/* Step 4.3.2 Calculate BE as in Daniel and Titman (JF, 2006) */
proc sql; 
  create table comp_extract_be as 
  select distinct a.gvkey, a.calyear, a.fyr, a.date_fyend,
         case 
            when missing(TXDITC)=0 and missing(PRBA)=0 then BE0+TXDITC-PRBA 
            else BE0
         end as BE, BE1
  from comp_extract as a left join pn.aco_pnfnda (keep=gvkey indfmt consol datafmt popsrc datadate prba) as b
  on a.gvkey=b.gvkey and a.indfmt=b.indfmt and a.consol=b.consol and a.datafmt=b.datafmt and 
     a.popsrc=b.popsrc and a.datadate=b.datadate;
quit;
*For 2 fiscal year ends within the same cal year (for whatever reason), chose latest fiscal year end;
proc sql;
  create table comp_extract_be as
  select distinct *
  from comp_extract_be
  group by gvkey, calyear
  having fyr=max(fyr);
quit;
*328955 obs;

/* Step 4.3.3 Obtain Book to Market (BM) ratios by dividing with ME at December of calendar year */
proc sql;
   create table BM0  as 
   select distinct a.gvkey, a.calyear, c.permno, c.hexcd, c.date format=date9., a.BE, a.BE1, 
          a.BE/(abs(c.prc)*(c.shrout/1000)) as BM
   from comp_extract_be as a, a_ccm.CCMXPF_LINKTABLE as b, crspa.msf (where=(month(date)=12)) as c
   where a.gvkey=b.gvkey and ((b.linkdt<=c.date<=b.linkenddt) or (b.linkdt<=c.date and b.linkenddt=.E)
         or (c.date<=b.linkenddt and b.linkdt=.B)) and b.lpermno=c.permno and a.calyear=year(c.date) and 
         (abs(c.prc)*c.shrout)>0;
quit;
*215994 obs;
 
proc sort data=BM0 nodupkey; by gvkey calyear; run;
*Keep only those cases with valid stock market size in June;
*Crsp stock size at June;
proc sql;
   create table size_june as 
   select distinct permno, date format=date9., (abs(prc)*shrout)/1000 as size
   from crspa.msf
   where year(&crspbegdate)<=year(date)<=year(&crspenddate) and month(date)=6;
quit;
* 250711 obs;

proc sql;
   create table BM as 
   select distinct a.gvkey, a.permno, a.calyear, a.date as decdate, a.hexcd, a.bm label='BM (decdate)', 
                   b.date format=date9.
   from BM0 as a, size_june as b
   where a.permno=b.permno and intck('month',a.date,b.date)=6 and b.size>0;
quit;
*197394 obs;
 
proc sort data=BM nodupkey; by permno calyear; run;
*Add date1 and date2 to BM;
data BM;
  set BM;
  date1=intnx("month",date,0,"E");
  date2=intnx("month",date,12,"E");
  format date1 date9. date2 date9.;
  drop date;
run;
data BM;
  format gvkey permno calyear decdate hexcd bm date1 date2;
  set BM;
  label date1='Date1 (BM valid from this date)';
  label date2='Date2 (BM valid to this date)';
run;


/* Step 4.3.4 Take log of BM */
data BM;
  set BM;
  if BM<0 then BM=0;
  LogBM=log(1+BM);
run;



/* Step 4.3.5 Winsorize LogBM at 1/99 */
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



/* Step 4.3.6 Include 4-digit SIC industry code and classify into FF48 industries */
proc sql;
  create table BM as
  select distinct a.*, b.SICCD label='SIC Code as of date1'
  from BM as a, (select distinct permno, NAMEDT, NAMEENDT, SICCD from crspa.MSENAMES) as b
  where a.permno=b.permno and b.NAMEDT<=a.date1<=b.NAMEENDT;
quit;

data BM;
 set BM;
* 1 Agric  Agriculture;
          if SICCD ge 0100 and SICCD le 0199  then FF48='AGRIC';
          if SICCD ge 0200 and SICCD le 0299  then FF48='AGRIC';
          if SICCD ge 0700 and SICCD le 0799  then FF48='AGRIC';
          if SICCD ge 0910 and SICCD le 0919  then FF48='AGRIC';
          if SICCD ge 2048 and SICCD le 2048  then FF48='AGRIC';
	if FF48='AGRIC' then FF48IndCode=1;

* 2 Food   Food Products;
          if SICCD ge 2000 and SICCD le 2009  then FF48='FOOD';
          if SICCD ge 2010 and SICCD le 2019  then FF48='FOOD';
          if SICCD ge 2020 and SICCD le 2029  then FF48='FOOD';
          if SICCD ge 2030 and SICCD le 2039  then FF48='FOOD';
          if SICCD ge 2040 and SICCD le 2046  then FF48='FOOD';
          if SICCD ge 2050 and SICCD le 2059  then FF48='FOOD';
          if SICCD ge 2060 and SICCD le 2063  then FF48='FOOD';
          if SICCD ge 2070 and SICCD le 2079  then FF48='FOOD';
          if SICCD ge 2090 and SICCD le 2092  then FF48='FOOD';
          if SICCD ge 2095 and SICCD le 2095  then FF48='FOOD';
          if SICCD ge 2098 and SICCD le 2099  then FF48='FOOD';
	if FF48='FOOD' then FF48IndCode=2;

* 3 Soda   Candy & Soda;
          if SICCD ge 2064 and SICCD le 2068  then FF48='SODA';
          if SICCD ge 2086 and SICCD le 2086  then FF48='SODA';
          if SICCD ge 2087 and SICCD le 2087  then FF48='SODA';
          if SICCD ge 2096 and SICCD le 2096  then FF48='SODA';
          if SICCD ge 2097 and SICCD le 2097  then FF48='SODA';
	if FF48='SODA' then FF48IndCode=3;

* 4 Beer   Beer & Liquor;
          if SICCD ge 2080 and SICCD le 2080  then FF48='BEER';
          if SICCD ge 2082 and SICCD le 2082  then FF48='BEER';
          if SICCD ge 2083 and SICCD le 2083  then FF48='BEER';
          if SICCD ge 2084 and SICCD le 2084  then FF48='BEER';
          if SICCD ge 2085 and SICCD le 2085  then FF48='BEER'; 
	if FF48='BEER' then FF48IndCode=4;

* 5 Smoke  Tobacco Products;
          if SICCD ge 2100 and SICCD le 2199  then FF48='SMOKE'; 
	if FF48='SMOKE' then FF48IndCode=5;

* 6 Toys   Recreation;
          if SICCD ge 0920 and SICCD le 0999  then FF48='TOYS'; 
          if SICCD ge 3650 and SICCD le 3651  then FF48='TOYS'; 
          if SICCD ge 3652 and SICCD le 3652  then FF48='TOYS';
          if SICCD ge 3732 and SICCD le 3732   then FF48='TOYS';
          if SICCD ge 3930 and SICCD le 3931   then FF48='TOYS';
          if SICCD ge 3940 and SICCD le 3949   then FF48='TOYS';
	if FF48='TOYS' then FF48IndCode=6;

* 7 Fun    Entertainment;
          if SICCD ge 7800 and SICCD le 7829   then FF48='FUN';
          if SICCD ge  7830 and SICCD le 7833   then FF48='FUN';
          if SICCD ge 7840 and SICCD le 7841   then FF48='FUN';
          if SICCD ge 7900 and SICCD le 7900   then FF48='FUN';
          if SICCD ge 7910 and SICCD le 7911   then FF48='FUN';
          if SICCD ge 7920 and SICCD le 7929   then FF48='FUN';
          if SICCD ge 7930 and SICCD le 7933   then FF48='FUN';
          if SICCD ge 7940 and SICCD le 7949   then FF48='FUN';
          if SICCD ge 7980 and SICCD le 7980   then FF48='FUN';
          if SICCD ge 7990 and SICCD le 7999   then FF48='FUN';
	if FF48='FUN' then FF48IndCode=7;

* 8 Books  Printing and Publishing;
          if SICCD ge 2700 and SICCD le 2709   then FF48='BOOKS';
          if SICCD ge 2710 and SICCD le 2719   then FF48='BOOKS';
          if SICCD ge 2720 and SICCD le 2729   then FF48='BOOKS';
          if SICCD ge 2730 and SICCD le 2739   then FF48='BOOKS';
          if SICCD ge 2740 and SICCD le 2749   then FF48='BOOKS';
          if SICCD ge 2770 and SICCD le 2771   then FF48='BOOKS';
          if SICCD ge 2780 and SICCD le 2789   then FF48='BOOKS';
          if SICCD ge 2790 and SICCD le 2799   then FF48='BOOKS';
	if FF48='BOOKS' then FF48IndCode=8;

* 9 Hshld  Consumer Goods;
          if SICCD ge 2047 and SICCD le 2047   then FF48='HSHLD';
          if SICCD ge 2391 and SICCD le 2392   then FF48='HSHLD';
          if SICCD ge 2510 and SICCD le 2519   then FF48='HSHLD';
          if SICCD ge 2590 and SICCD le 2599   then FF48='HSHLD';
          if SICCD ge 2840 and SICCD le 2843   then FF48='HSHLD';
          if SICCD ge 2844 and SICCD le 2844   then FF48='HSHLD';
          if SICCD ge 3160 and SICCD le 3161   then FF48='HSHLD';
          if SICCD ge 3170 and SICCD le 3171   then FF48='HSHLD';
          if SICCD ge 3172 and SICCD le 3172   then FF48='HSHLD';
          if SICCD ge 3190 and SICCD le 3199   then FF48='HSHLD';
          if SICCD ge 3229 and SICCD le 3229   then FF48='HSHLD';
          if SICCD ge 3260 and SICCD le 3260   then FF48='HSHLD';
          if SICCD ge 3262 and SICCD le 3263   then FF48='HSHLD';
          if SICCD ge 3269 and SICCD le 3269   then FF48='HSHLD';
          if SICCD ge 3230 and SICCD le 3231   then FF48='HSHLD';
          if SICCD ge 3630 and SICCD le 3639   then FF48='HSHLD';
          if SICCD ge 3750 and SICCD le 3751   then FF48='HSHLD';
          if SICCD ge 3800 and SICCD le 3800   then FF48='HSHLD';
          if SICCD ge 3860 and SICCD le 3861   then FF48='HSHLD';
          if SICCD ge 3870 and SICCD le 3873   then FF48='HSHLD';
          if SICCD ge 3910 and SICCD le 3911   then FF48='HSHLD';
          if SICCD ge 3914 and SICCD le 3914   then FF48='HSHLD';
          if SICCD ge 3915 and SICCD le 3915   then FF48='HSHLD';
          if SICCD ge 3960 and SICCD le 3962   then FF48='HSHLD';
          if SICCD ge 3991 and SICCD le 3991   then FF48='HSHLD';
          if SICCD ge 3995 and SICCD le 3995   then FF48='HSHLD';
	if FF48='HSHLD' then FF48IndCode=9;

*10 Clths  Apparel;
          if SICCD ge 2300 and SICCD le 2390   then FF48='CLTHS';
          if SICCD ge 3020 and SICCD le 3021   then FF48='CLTHS';
          if SICCD ge 3100 and SICCD le 3111   then FF48='CLTHS';
          if SICCD ge 3130 and SICCD le 3131   then FF48='CLTHS';
          if SICCD ge 3140 and SICCD le 3149   then FF48='CLTHS';
          if SICCD ge 3150 and SICCD le 3151   then FF48='CLTHS';
          if SICCD ge 3963 and SICCD le 3965   then FF48='CLTHS';
	if FF48='CLTHS' then FF48IndCode=10;

*11 Hlth   Healthcare;
          if SICCD ge 8000 and SICCD le 8099   then FF48='HLTH';
	if FF48='HLTH' then FF48IndCode=11;

*12 MedEq  Medical Equipment;
          if SICCD ge 3693 and SICCD le 3693   then FF48='MEDEQ';
          if SICCD ge 3840 and SICCD le 3849   then FF48='MEDEQ';
          if SICCD ge 3850 and SICCD le 3851   then FF48='MEDEQ';
	if FF48='MEDEQ' then FF48IndCode=12;

*13 Drugs  Pharmaceutical Products;
          if SICCD ge 2830 and SICCD le 2830   then FF48='DRUGS';
          if SICCD ge 2831 and SICCD le 2831   then FF48='DRUGS';
          if SICCD ge 2833 and SICCD le 2833   then FF48='DRUGS';
          if SICCD ge 2834 and SICCD le 2834   then FF48='DRUGS';
          if SICCD ge 2835 and SICCD le 2835   then FF48='DRUGS';
          if SICCD ge 2836 and SICCD le 2836   then FF48='DRUGS';
	if FF48='DRUGS' then FF48IndCode=13;

*14 Chems  Chemicals;
          if SICCD ge 2800 and SICCD le 2809   then FF48='CHEM';
          if SICCD ge 2810 and SICCD le 2819   then FF48='CHEM';
          if SICCD ge 2820 and SICCD le 2829   then FF48='CHEM';
          if SICCD ge 2850 and SICCD le 2859   then FF48='CHEM';
          if SICCD ge 2860 and SICCD le 2869   then FF48='CHEM';
          if SICCD ge 2870 and SICCD le 2879   then FF48='CHEM';
          if SICCD ge 2890 and SICCD le 2899   then FF48='CHEM';
	if FF48='CHEM' then FF48IndCode=14;

*15 Rubbr  Rubber and Plastic Products;
          if SICCD ge 3031 and SICCD le 3031   then FF48='RUBBR';
          if SICCD ge 3041 and SICCD le 3041   then FF48='RUBBR';
          if SICCD ge 3050 and SICCD le 3053   then FF48='RUBBR';
          if SICCD ge 3060 and SICCD le 3069   then FF48='RUBBR';
          if SICCD ge 3070 and SICCD le 3079   then FF48='RUBBR';
          if SICCD ge 3080 and SICCD le 3089   then FF48='RUBBR';
          if SICCD ge 3090 and SICCD le 3099   then FF48='RUBBR';
	if FF48='RUBBR' then FF48IndCode=15;

*16 Txtls  Textiles;
          if SICCD ge 2200 and SICCD le 2269   then FF48='TXTLS';
          if SICCD ge 2270 and SICCD le 2279   then FF48='TXTLS';
          if SICCD ge 2280 and SICCD le 2284   then FF48='TXTLS';
          if SICCD ge 2290 and SICCD le 2295   then FF48='TXTLS';
          if SICCD ge 2297 and SICCD le 2297   then FF48='TXTLS';
          if SICCD ge 2298 and SICCD le 2298   then FF48='TXTLS';
          if SICCD ge 2299 and SICCD le 2299   then FF48='TXTLS';
          if SICCD ge 2393 and SICCD le 2395   then FF48='TXTLS';
          if SICCD ge 2397 and SICCD le 2399   then FF48='TXTLS';
	if FF48='TXTLS' then FF48IndCode=16;

*17 BldMt  Construction Materials;
          if SICCD ge 0800 and SICCD le 0899   then FF48='BLDMT';
          if SICCD ge 2400 and SICCD le 2439   then FF48='BLDMT';
          if SICCD ge 2450 and SICCD le 2459   then FF48='BLDMT';
          if SICCD ge 2490 and SICCD le 2499   then FF48='BLDMT';
          if SICCD ge 2660 and SICCD le 2661   then FF48='BLDMT';
          if SICCD ge 2950 and SICCD le 2952   then FF48='BLDMT';
          if SICCD ge 3200 and SICCD le 3200   then FF48='BLDMT';
          if SICCD ge 3210 and SICCD le 3211   then FF48='BLDMT';
          if SICCD ge 3240 and SICCD le 3241   then FF48='BLDMT';
          if SICCD ge 3250 and SICCD le 3259   then FF48='BLDMT';
          if SICCD ge 3261 and SICCD le 3261   then FF48='BLDMT';
          if SICCD ge 3264 and SICCD le 3264   then FF48='BLDMT';
          if SICCD ge 3270 and SICCD le 3275   then FF48='BLDMT';
          if SICCD ge 3280 and SICCD le 3281   then FF48='BLDMT';
          if SICCD ge 3290 and SICCD le 3293   then FF48='BLDMT';
          if SICCD ge 3295 and SICCD le 3299   then FF48='BLDMT';
          if SICCD ge 3420 and SICCD le 3429   then FF48='BLDMT';
          if SICCD ge 3430 and SICCD le 3433   then FF48='BLDMT';
          if SICCD ge 3440 and SICCD le 3441   then FF48='BLDMT';
          if SICCD ge 3442 and SICCD le 3442   then FF48='BLDMT';
          if SICCD ge 3446 and SICCD le 3446   then FF48='BLDMT';
          if SICCD ge 3448 and SICCD le 3448   then FF48='BLDMT';
          if SICCD ge 3449 and SICCD le 3449   then FF48='BLDMT';
          if SICCD ge 3450 and SICCD le 3451   then FF48='BLDMT';
          if SICCD ge 3452 and SICCD le 3452   then FF48='BLDMT';
          if SICCD ge 3490 and SICCD le 3499   then FF48='BLDMT';
          if SICCD ge 3996 and SICCD le 3996   then FF48='BLDMT';
	if FF48='BLDMT' then FF48IndCode=17;

*18 Cnstr  Construction;
          if SICCD ge 1500 and SICCD le 1511   then FF48='CNSTR';
          if SICCD ge 1520 and SICCD le 1529   then FF48='CNSTR';
          if SICCD ge 1530 and SICCD le 1539   then FF48='CNSTR';
          if SICCD ge 1540 and SICCD le 1549   then FF48='CNSTR';
          if SICCD ge 1600 and SICCD le 1699   then FF48='CNSTR';
          if SICCD ge 1700 and SICCD le 1799   then FF48='CNSTR';
	if FF48='CNSTR' then FF48IndCode=18;

*19 Steel  Steel Works Etc;
          if SICCD ge 3300 and SICCD le 3300   then FF48='STEEL';
          if SICCD ge 3310 and SICCD le 3317   then FF48='STEEL';
          if SICCD ge 3320 and SICCD le 3325   then FF48='STEEL';
          if SICCD ge 3330 and SICCD le 3339   then FF48='STEEL';
          if SICCD ge 3340 and SICCD le 3341   then FF48='STEEL';
          if SICCD ge 3350 and SICCD le 3357   then FF48='STEEL';
          if SICCD ge 3360 and SICCD le 3369   then FF48='STEEL';
          if SICCD ge 3370 and SICCD le 3379   then FF48='STEEL';
          if SICCD ge 3390 and SICCD le 3399   then FF48='STEEL';
	if FF48='STEEL' then FF48IndCode=19;

*20 FabPr  Fabricated Products;
          if SICCD ge 3400 and SICCD le 3400   then FF48='FABPR';
          if SICCD ge 3443 and SICCD le 3443   then FF48='FABPR';
          if SICCD ge 3444 and SICCD le 3444   then FF48='FABPR';
          if SICCD ge 3460 and SICCD le 3469   then FF48='FABPR';
          if SICCD ge 3470 and SICCD le 3479   then FF48='FABPR';
	if FF48='FABPR' then FF48IndCode=20;

*21 Mach   Machinery;
          if SICCD ge 3510 and SICCD le 3519   then FF48='MACH';
          if SICCD ge 3520 and SICCD le 3529   then FF48='MACH';
          if SICCD ge 3530 and SICCD le 3530   then FF48='MACH';
          if SICCD ge 3531 and SICCD le 3531   then FF48='MACH';
          if SICCD ge 3532 and SICCD le 3532   then FF48='MACH';
          if SICCD ge 3533 and SICCD le 3533   then FF48='MACH';
          if SICCD ge 3534 and SICCD le 3534   then FF48='MACH';
          if SICCD ge 3535 and SICCD le 3535   then FF48='MACH';
          if SICCD ge 3536 and SICCD le 3536   then FF48='MACH';
          if SICCD ge 3538 and SICCD le 3538   then FF48='MACH';
          if SICCD ge 3540 and SICCD le 3549   then FF48='MACH';
          if SICCD ge 3550 and SICCD le 3559   then FF48='MACH';
          if SICCD ge 3560 and SICCD le 3569   then FF48='MACH';
          if SICCD ge 3580 and SICCD le 3580   then FF48='MACH';
          if SICCD ge 3581 and SICCD le 3581   then FF48='MACH';
          if SICCD ge 3582 and SICCD le 3582   then FF48='MACH';
          if SICCD ge 3585 and SICCD le 3585   then FF48='MACH';
          if SICCD ge 3586 and SICCD le 3586   then FF48='MACH';
          if SICCD ge 3589 and SICCD le 3589   then FF48='MACH';
          if SICCD ge 3590 and SICCD le 3599   then FF48='MACH';
	if FF48='MACH' then FF48IndCode=21;

*22 ElcEq  Electrical Equipment;
          if SICCD ge 3600 and SICCD le 3600   then FF48='ELCEQ';
          if SICCD ge 3610 and SICCD le 3613   then FF48='ELCEQ';
          if SICCD ge 3620 and SICCD le 3621   then FF48='ELCEQ';
          if SICCD ge 3623 and SICCD le 3629   then FF48='ELCEQ';
          if SICCD ge 3640 and SICCD le 3644   then FF48='ELCEQ';
          if SICCD ge 3645 and SICCD le 3645   then FF48='ELCEQ'; 
          if SICCD ge 3646 and SICCD le 3646   then FF48='ELCEQ';
          if SICCD ge 3648 and SICCD le 3649   then FF48='ELCEQ';
          if SICCD ge 3660 and SICCD le 3660   then FF48='ELCEQ';
          if SICCD ge 3690 and SICCD le 3690   then FF48='ELCEQ';
          if SICCD ge 3691 and SICCD le 3692   then FF48='ELCEQ';
          if SICCD ge 3699 and SICCD le 3699   then FF48='ELCEQ';
	if FF48='ELCEQ' then FF48IndCode=22;

*23 Autos  Automobiles and Trucks;
          if SICCD ge 2296 and SICCD le 2296   then FF48='AUTOS';
          if SICCD ge 2396 and SICCD le 2396   then FF48='AUTOS';
          if SICCD ge 3010 and SICCD le 3011   then FF48='AUTOS';
          if SICCD ge 3537 and SICCD le 3537   then FF48='AUTOS';
          if SICCD ge 3647 and SICCD le 3647   then FF48='AUTOS';
          if SICCD ge 3694 and SICCD le 3694   then FF48='AUTOS';
          if SICCD ge 3700 and SICCD le 3700   then FF48='AUTOS';
          if SICCD ge 3710 and SICCD le 3710   then FF48='AUTOS';
          if SICCD ge 3711 and SICCD le 3711   then FF48='AUTOS';
          if SICCD ge 3713 and SICCD le 3713   then FF48='AUTOS';
          if SICCD ge 3714 and SICCD le 3714   then FF48='AUTOS';
          if SICCD ge 3715 and SICCD le 3715   then FF48='AUTOS';
          if SICCD ge 3716 and SICCD le 3716   then FF48='AUTOS';
          if SICCD ge 3792 and SICCD le 3792   then FF48='AUTOS';
          if SICCD ge 3790 and SICCD le 3791   then FF48='AUTOS';
          if SICCD ge 3799 and SICCD le 3799   then FF48='AUTOS';
	if FF48='AUTOS' then FF48IndCode=23;

*24 Aero   Aircraft;
          if SICCD ge 3720 and SICCD le 3720   then FF48='AERO';
          if SICCD ge 3721 and SICCD le 3721   then FF48='AERO';
          if SICCD ge 3723 and SICCD le 3724   then FF48='AERO';
          if SICCD ge 3725 and SICCD le 3725   then FF48='AERO';
          if SICCD ge 3728 and SICCD le 3729   then FF48='AERO';
	if FF48='AERO' then FF48IndCode=24;

*25 Ships  Shipbuilding, Railroad Equipment;
          if SICCD ge 3730 and SICCD le 3731   then FF48='SHIPS';
          if SICCD ge 3740 and SICCD le 3743   then FF48='SHIPS';
	if FF48='SHIPS' then FF48IndCode=25;

*26 Guns   Defense;
          if SICCD ge 3760 and SICCD le 3769   then FF48='GUNS';
          if SICCD ge 3795 and SICCD le 3795   then FF48='GUNS';
          if SICCD ge 3480 and SICCD le 3489   then FF48='GUNS';
	if FF48='GUNS' then FF48IndCode=26;

*27 Gold   Precious Metals;
          if SICCD ge 1040 and SICCD le 1049   then FF48='GOLD';
	if FF48='GOLD' then FF48IndCode=27;

*28 Mines  Non and SICCD le Metallic and Industrial Metal Mining;;
          if SICCD ge 1000 and SICCD le 1009   then FF48='MINES';
          if SICCD ge 1010 and SICCD le 1019   then FF48='MINES';
          if SICCD ge 1020 and SICCD le 1029   then FF48='MINES';
          if SICCD ge 1030 and SICCD le 1039   then FF48='MINES';
          if SICCD ge 1050 and SICCD le 1059   then FF48='MINES';
          if SICCD ge 1060 and SICCD le 1069   then FF48='MINES';
          if SICCD ge 1070 and SICCD le 1079   then FF48='MINES';
          if SICCD ge 1080 and SICCD le 1089   then FF48='MINES';
          if SICCD ge 1090 and SICCD le 1099   then FF48='MINES';
          if SICCD ge 1100 and SICCD le 1119   then FF48='MINES';
          if SICCD ge 1400 and SICCD le 1499   then FF48='MINES';
	if FF48='MINES' then FF48IndCode=28;

*29 Coal   Coal;
          if SICCD ge 1200 and SICCD le 1299   then FF48='COAL';
	if FF48='COAL' then FF48IndCode=29;

*30 Oil    Petroleum and Natural Gas;
          if SICCD ge 1300 and SICCD le 1300   then FF48='OIL';
          if SICCD ge 1310 and SICCD le 1319   then FF48='OIL';
          if SICCD ge 1320 and SICCD le 1329   then FF48='OIL';
          if SICCD ge 1330 and SICCD le 1339   then FF48='OIL';
          if SICCD ge 1370 and SICCD le 1379   then FF48='OIL';
          if SICCD ge 1380 and SICCD le 1380   then FF48='OIL';
          if SICCD ge 1381 and SICCD le 1381   then FF48='OIL';
          if SICCD ge 1382 and SICCD le 1382   then FF48='OIL';
          if SICCD ge 1389 and SICCD le 1389   then FF48='OIL';
          if SICCD ge 2900 and SICCD le 2912   then FF48='OIL';
          if SICCD ge 2990 and SICCD le 2999   then FF48='OIL';
	if FF48='OIL' then FF48IndCode=30;

*31 Util   Utilities;
          if SICCD ge 4900 and SICCD le 4900   then FF48='UTIL';
          if SICCD ge 4910 and SICCD le 4911   then FF48='UTIL';
          if SICCD ge 4920 and SICCD le 4922   then FF48='UTIL';
          if SICCD ge 4923 and SICCD le 4923   then FF48='UTIL';
          if SICCD ge 4924 and SICCD le 4925   then FF48='UTIL';
          if SICCD ge 4930 and SICCD le 4931   then FF48='UTIL';
          if SICCD ge 4932 and SICCD le 4932   then FF48='UTIL';
          if SICCD ge 4939 and SICCD le 4939   then FF48='UTIL';
          if SICCD ge 4940 and SICCD le 4942   then FF48='UTIL';
	if FF48='UTIL' then FF48IndCode=31;

*32 Telcm  Communication;
          if SICCD ge 4800 and SICCD le 4800   then FF48='TELCM';
          if SICCD ge 4810 and SICCD le 4813   then FF48='TELCM';
          if SICCD ge 4820 and SICCD le 4822   then FF48='TELCM';
          if SICCD ge 4830 and SICCD le 4839   then FF48='TELCM';
          if SICCD ge 4840 and SICCD le 4841   then FF48='TELCM';
          if SICCD ge 4880 and SICCD le 4889   then FF48='TELCM';
          if SICCD ge 4890 and SICCD le 4890   then FF48='TELCM';
          if SICCD ge 4891 and SICCD le 4891   then FF48='TELCM';
          if SICCD ge 4892 and SICCD le 4892   then FF48='TELCM';
          if SICCD ge 4899 and SICCD le 4899   then FF48='TELCM';
	if FF48='TELCM' then FF48IndCode=32;

*33 PerSv  Personal Services;
          if SICCD ge 7020 and SICCD le 7021   then FF48='PERSV';
          if SICCD ge 7030 and SICCD le 7033   then FF48='PERSV';
          if SICCD ge 7200 and SICCD le 7200   then FF48='PERSV';
          if SICCD ge 7210 and SICCD le 7212   then FF48='PERSV';
          if SICCD ge 7214 and SICCD le 7214   then FF48='PERSV';
          if SICCD ge 7215 and SICCD le 7216   then FF48='PERSV';
          if SICCD ge 7217 and SICCD le 7217   then FF48='PERSV';
          if SICCD ge 7219 and SICCD le 7219   then FF48='PERSV';
          if SICCD ge 7220 and SICCD le 7221   then FF48='PERSV';
          if SICCD ge 7230 and SICCD le 7231   then FF48='PERSV';
          if SICCD ge 7240 and SICCD le 7241   then FF48='PERSV';
          if SICCD ge 7250 and SICCD le 7251   then FF48='PERSV';
          if SICCD ge 7260 and SICCD le 7269   then FF48='PERSV';
          if SICCD ge 7270 and SICCD le 7290   then FF48='PERSV';
          if SICCD ge 7291 and SICCD le 7291   then FF48='PERSV';
          if SICCD ge 7292 and SICCD le 7299   then FF48='PERSV';
          if SICCD ge 7395 and SICCD le 7395   then FF48='PERSV';
          if SICCD ge 7500 and SICCD le 7500   then FF48='PERSV';
          if SICCD ge 7520 and SICCD le 7529   then FF48='PERSV';
          if SICCD ge 7530 and SICCD le 7539   then FF48='PERSV';
          if SICCD ge 7540 and SICCD le 7549   then FF48='PERSV';
          if SICCD ge 7600 and SICCD le 7600   then FF48='PERSV';
          if SICCD ge 7620 and SICCD le 7620   then FF48='PERSV';
          if SICCD ge 7622 and SICCD le 7622   then FF48='PERSV';
          if SICCD ge 7623 and SICCD le 7623   then FF48='PERSV';
          if SICCD ge 7629 and SICCD le 7629   then FF48='PERSV';
          if SICCD ge 7630 and SICCD le 7631   then FF48='PERSV';
          if SICCD ge 7640 and SICCD le 7641   then FF48='PERSV';
          if SICCD ge 7690 and SICCD le 7699   then FF48='PERSV';
          if SICCD ge 8100 and SICCD le 8199   then FF48='PERSV';
          if SICCD ge 8200 and SICCD le 8299   then FF48='PERSV';
          if SICCD ge 8300 and SICCD le 8399   then FF48='PERSV';
          if SICCD ge 8400 and SICCD le 8499   then FF48='PERSV';
          if SICCD ge 8600 and SICCD le 8699   then FF48='PERSV';
          if SICCD ge 8800 and SICCD le 8899   then FF48='PERSV';
          if SICCD ge 7510 and SICCD le 7515   then FF48='PERSV';
	if FF48='PERSV' then FF48IndCode=33;

*34 BusSv  Business Services;
          if SICCD ge 2750 and SICCD le 2759   then FF48='BUSSV';
          if SICCD ge 3993 and SICCD le 3993   then FF48='BUSSV';
          if SICCD ge 7218 and SICCD le 7218   then FF48='BUSSV';
          if SICCD ge 7300 and SICCD le 7300   then FF48='BUSSV';
          if SICCD ge 7310 and SICCD le 7319   then FF48='BUSSV';
          if SICCD ge 7320 and SICCD le 7329   then FF48='BUSSV';
          if SICCD ge 7330 and SICCD le 7339   then FF48='BUSSV';
          if SICCD ge 7340 and SICCD le 7342   then FF48='BUSSV';
          if SICCD ge 7349 and SICCD le 7349   then FF48='BUSSV';
          if SICCD ge 7350 and SICCD le 7351   then FF48='BUSSV';
          if SICCD ge 7352 and SICCD le 7352   then FF48='BUSSV';
          if SICCD ge 7353 and SICCD le 7353   then FF48='BUSSV';
          if SICCD ge 7359 and SICCD le 7359   then FF48='BUSSV';
          if SICCD ge 7360 and SICCD le 7369   then FF48='BUSSV';
          if SICCD ge 7370 and SICCD le 7372   then FF48='BUSSV';
          if SICCD ge 7374 and SICCD le 7374   then FF48='BUSSV';
          if SICCD ge 7375 and SICCD le 7375   then FF48='BUSSV';
          if SICCD ge 7376 and SICCD le 7376   then FF48='BUSSV';
          if SICCD ge 7377 and SICCD le 7377   then FF48='BUSSV';
          if SICCD ge 7378 and SICCD le 7378   then FF48='BUSSV';
          if SICCD ge 7379 and SICCD le 7379   then FF48='BUSSV';
          if SICCD ge 7380 and SICCD le 7380   then FF48='BUSSV';
          if SICCD ge 7381 and SICCD le 7382   then FF48='BUSSV';
          if SICCD ge 7383 and SICCD le 7383   then FF48='BUSSV';
          if SICCD ge 7384 and SICCD le 7384   then FF48='BUSSV';
          if SICCD ge 7385 and SICCD le 7385   then FF48='BUSSV';
          if SICCD ge 7389 and SICCD le 7390   then FF48='BUSSV';
          if SICCD ge 7391 and SICCD le 7391   then FF48='BUSSV';
          if SICCD ge 7392 and SICCD le 7392   then FF48='BUSSV';
          if SICCD ge 7393 and SICCD le 7393   then FF48='BUSSV';
          if SICCD ge 7394 and SICCD le 7394   then FF48='BUSSV';
          if SICCD ge 7396 and SICCD le 7396   then FF48='BUSSV';
          if SICCD ge 7397 and SICCD le 7397   then FF48='BUSSV';
          if SICCD ge 7399 and SICCD le 7399   then FF48='BUSSV';
          if SICCD ge 7519 and SICCD le 7519   then FF48='BUSSV';
          if SICCD ge 8700 and SICCD le 8700   then FF48='BUSSV';
          if SICCD ge 8710 and SICCD le 8713   then FF48='BUSSV';
          if SICCD ge 8720 and SICCD le 8721   then FF48='BUSSV';
          if SICCD ge 8730 and SICCD le 8734   then FF48='BUSSV';
          if SICCD ge 8740 and SICCD le 8748   then FF48='BUSSV';
          if SICCD ge 8900 and SICCD le 8910   then FF48='BUSSV';
          if SICCD ge 8911 and SICCD le 8911   then FF48='BUSSV';
          if SICCD ge 8920 and SICCD le 8999   then FF48='BUSSV';
          if SICCD ge 4220 and SICCD le 4229  then FF48='BUSSV';
	if FF48='BUSSV' then FF48IndCode=34;

*35 Comps  Computers;
          if SICCD ge 3570 and SICCD le 3579   then FF48='COMPS';
          if SICCD ge 3680 and SICCD le 3680   then FF48='COMPS';
          if SICCD ge 3681 and SICCD le 3681   then FF48='COMPS';
          if SICCD ge 3682 and SICCD le 3682   then FF48='COMPS';
          if SICCD ge 3683 and SICCD le 3683   then FF48='COMPS';
          if SICCD ge 3684 and SICCD le 3684   then FF48='COMPS';
          if SICCD ge 3685 and SICCD le 3685   then FF48='COMPS';
          if SICCD ge 3686 and SICCD le 3686   then FF48='COMPS';
          if SICCD ge 3687 and SICCD le 3687   then FF48='COMPS';
          if SICCD ge 3688 and SICCD le 3688   then FF48='COMPS';
          if SICCD ge 3689 and SICCD le 3689   then FF48='COMPS';
          if SICCD ge 3695 and SICCD le 3695   then FF48='COMPS';
          if SICCD ge 7373 and SICCD le 7373   then FF48='COMPS';
	if FF48='COMPS' then FF48IndCode=35;

*36 Chips  Electronic Equipment;
          if SICCD ge 3622 and SICCD le 3622   then FF48='CHIPS';
          if SICCD ge 3661 and SICCD le 3661   then FF48='CHIPS';
          if SICCD ge 3662 and SICCD le 3662   then FF48='CHIPS';
          if SICCD ge 3663 and SICCD le 3663   then FF48='CHIPS';
          if SICCD ge 3664 and SICCD le 3664   then FF48='CHIPS';
          if SICCD ge 3665 and SICCD le 3665   then FF48='CHIPS';
          if SICCD ge 3666 and SICCD le 3666   then FF48='CHIPS';
          if SICCD ge 3669 and SICCD le 3669   then FF48='CHIPS';
          if SICCD ge 3670 and SICCD le 3679   then FF48='CHIPS';
          if SICCD ge 3810 and SICCD le 3810   then FF48='CHIPS';
          if SICCD ge 3812 and SICCD le 3812   then FF48='CHIPS';
	if FF48='CHIPS' then FF48IndCode=36;

*37 LabEq  Measuring and Control Equipment;
          if SICCD ge 3811 and SICCD le 3811   then FF48='LABEQ';
          if SICCD ge 3820 and SICCD le 3820   then FF48='LABEQ';
          if SICCD ge 3821 and SICCD le 3821   then FF48='LABEQ';
          if SICCD ge 3822 and SICCD le 3822   then FF48='LABEQ';
          if SICCD ge 3823 and SICCD le 3823   then FF48='LABEQ';
          if SICCD ge 3824 and SICCD le 3824   then FF48='LABEQ';
          if SICCD ge 3825 and SICCD le 3825   then FF48='LABEQ';
          if SICCD ge 3826 and SICCD le 3826   then FF48='LABEQ';
          if SICCD ge 3827 and SICCD le 3827   then FF48='LABEQ';
          if SICCD ge 3829 and SICCD le 3829   then FF48='LABEQ';
          if SICCD ge 3830 and SICCD le 3839   then FF48='LABEQ';
	if FF48='LABEQ' then FF48IndCode=37;

*38 Paper  Business Supplies;
          if SICCD ge 2520 and SICCD le 2549   then FF48='PAPER';
          if SICCD ge 2600 and SICCD le 2639   then FF48='PAPER';
          if SICCD ge 2670 and SICCD le 2699   then FF48='PAPER';
          if SICCD ge 2760 and SICCD le 2761   then FF48='PAPER';
          if SICCD ge 3950 and SICCD le 3955   then FF48='PAPER';
	if FF48='PAPER' then FF48IndCode=38;

*39 Boxes  Shipping Containers;
          if SICCD ge 2440 and SICCD le 2449   then FF48='BOXES';
          if SICCD ge 2640 and SICCD le 2659   then FF48='BOXES';
          if SICCD ge 3220 and SICCD le 3221   then FF48='BOXES';
          if SICCD ge 3410 and SICCD le 3412   then FF48='BOXES';
	if FF48='BOXES' then FF48IndCode=39;

*40 Trans  Transportation;
          if SICCD ge 4000 and SICCD le 4013   then FF48='TRANS';
          if SICCD ge 4040 and SICCD le 4049   then FF48='TRANS';
          if SICCD ge 4100 and SICCD le 4100   then FF48='TRANS';
          if SICCD ge 4110 and SICCD le 4119   then FF48='TRANS';
          if SICCD ge 4120 and SICCD le 4121   then FF48='TRANS';
          if SICCD ge 4130 and SICCD le 4131   then FF48='TRANS';
          if SICCD ge 4140 and SICCD le 4142   then FF48='TRANS';
          if SICCD ge 4150 and SICCD le 4151   then FF48='TRANS';
          if SICCD ge 4170 and SICCD le 4173   then FF48='TRANS';
          if SICCD ge 4190 and SICCD le 4199   then FF48='TRANS';
          if SICCD ge 4200 and SICCD le 4200   then FF48='TRANS';
          if SICCD ge 4210 and SICCD le 4219   then FF48='TRANS';
          if SICCD ge 4230 and SICCD le 4231   then FF48='TRANS';
          if SICCD ge 4240 and SICCD le 4249   then FF48='TRANS';
          if SICCD ge 4400 and SICCD le 4499   then FF48='TRANS';
          if SICCD ge 4500 and SICCD le 4599   then FF48='TRANS';
          if SICCD ge 4600 and SICCD le 4699   then FF48='TRANS';
          if SICCD ge 4700 and SICCD le 4700   then FF48='TRANS';
          if SICCD ge 4710 and SICCD le 4712   then FF48='TRANS';
          if SICCD ge 4720 and SICCD le 4729   then FF48='TRANS';
          if SICCD ge 4730 and SICCD le 4739   then FF48='TRANS';
          if SICCD ge 4740 and SICCD le 4749   then FF48='TRANS';
          if SICCD ge 4780 and SICCD le 4780   then FF48='TRANS';
          if SICCD ge 4782 and SICCD le 4782   then FF48='TRANS';
          if SICCD ge 4783 and SICCD le 4783   then FF48='TRANS';
          if SICCD ge 4784 and SICCD le 4784   then FF48='TRANS';
          if SICCD ge 4785 and SICCD le 4785   then FF48='TRANS';
          if SICCD ge 4789 and SICCD le 4789   then FF48='TRANS';
	if FF48='TRANS' then FF48IndCode=40;

*41 Whlsl  Wholesale;
          if SICCD ge 5000 and SICCD le 5000   then FF48='WHLSL';
          if SICCD ge 5010 and SICCD le 5015   then FF48='WHLSL';
          if SICCD ge 5020 and SICCD le 5023   then FF48='WHLSL';
          if SICCD ge 5030 and SICCD le 5039   then FF48='WHLSL';
          if SICCD ge 5040 and SICCD le 5042   then FF48='WHLSL';
          if SICCD ge 5043 and SICCD le 5043   then FF48='WHLSL';
          if SICCD ge 5044 and SICCD le 5044   then FF48='WHLSL';
          if SICCD ge 5045 and SICCD le 5045   then FF48='WHLSL';
          if SICCD ge 5046 and SICCD le 5046   then FF48='WHLSL';
          if SICCD ge 5047 and SICCD le 5047   then FF48='WHLSL';
          if SICCD ge 5048 and SICCD le 5048   then FF48='WHLSL';
          if SICCD ge 5049 and SICCD le 5049   then FF48='WHLSL';
          if SICCD ge 5050 and SICCD le 5059   then FF48='WHLSL';
          if SICCD ge 5060 and SICCD le 5060   then FF48='WHLSL';
          if SICCD ge 5063 and SICCD le 5063   then FF48='WHLSL';
          if SICCD ge 5064 and SICCD le 5064   then FF48='WHLSL';
          if SICCD ge 5065 and SICCD le 5065   then FF48='WHLSL';
          if SICCD ge 5070 and SICCD le 5078   then FF48='WHLSL';
          if SICCD ge 5080 and SICCD le 5080   then FF48='WHLSL';
          if SICCD ge 5081 and SICCD le 5081   then FF48='WHLSL';
          if SICCD ge 5082 and SICCD le 5082   then FF48='WHLSL';
          if SICCD ge 5083 and SICCD le 5083   then FF48='WHLSL';
          if SICCD ge 5084 and SICCD le 5084   then FF48='WHLSL';
          if SICCD ge 5085 and SICCD le 5085   then FF48='WHLSL';
          if SICCD ge 5086 and SICCD le 5087   then FF48='WHLSL';
          if SICCD ge 5088 and SICCD le 5088   then FF48='WHLSL';
          if SICCD ge 5090 and SICCD le 5090   then FF48='WHLSL';
          if SICCD ge 5091 and SICCD le 5092   then FF48='WHLSL';
          if SICCD ge 5093 and SICCD le 5093   then FF48='WHLSL';
          if SICCD ge 5094 and SICCD le 5094   then FF48='WHLSL';
          if SICCD ge 5099 and SICCD le 5099   then FF48='WHLSL';
          if SICCD ge 5100 and SICCD le 5100   then FF48='WHLSL';
          if SICCD ge 5110 and SICCD le 5113   then FF48='WHLSL';
          if SICCD ge 5120 and SICCD le 5122   then FF48='WHLSL';
          if SICCD ge 5130 and SICCD le 5139   then FF48='WHLSL';
          if SICCD ge 5140 and SICCD le 5149   then FF48='WHLSL';
          if SICCD ge 5150 and SICCD le 5159   then FF48='WHLSL';
          if SICCD ge 5160 and SICCD le 5169   then FF48='WHLSL';
          if SICCD ge 5170 and SICCD le 5172   then FF48='WHLSL';
          if SICCD ge 5180 and SICCD le 5182   then FF48='WHLSL';
          if SICCD ge 5190 and SICCD le 5199   then FF48='WHLSL';
	if FF48='WHLSL' then FF48IndCode=41;

*42 Rtail  Retail ;
          if SICCD ge 5200 and SICCD le 5200   then FF48='RTAIL';
          if SICCD ge 5210 and SICCD le 5219   then FF48='RTAIL';
          if SICCD ge 5220 and SICCD le 5229   then FF48='RTAIL';
          if SICCD ge 5230 and SICCD le 5231   then FF48='RTAIL';
          if SICCD ge 5250 and SICCD le 5251   then FF48='RTAIL';
          if SICCD ge 5260 and SICCD le 5261   then FF48='RTAIL';
          if SICCD ge 5270 and SICCD le 5271   then FF48='RTAIL';
          if SICCD ge 5300 and SICCD le 5300   then FF48='RTAIL';
          if SICCD ge 5310 and SICCD le 5311   then FF48='RTAIL';
          if SICCD ge 5320 and SICCD le 5320   then FF48='RTAIL';
          if SICCD ge 5330 and SICCD le 5331   then FF48='RTAIL';
          if SICCD ge 5334 and SICCD le 5334   then FF48='RTAIL';
          if SICCD ge 5340 and SICCD le 5349   then FF48='RTAIL';
          if SICCD ge 5390 and SICCD le 5399   then FF48='RTAIL';
          if SICCD ge 5400 and SICCD le 5400   then FF48='RTAIL';
          if SICCD ge 5410 and SICCD le 5411   then FF48='RTAIL';
          if SICCD ge 5412 and SICCD le 5412   then FF48='RTAIL';
          if SICCD ge 5420 and SICCD le 5429   then FF48='RTAIL';
          if SICCD ge 5430 and SICCD le 5439   then FF48='RTAIL';
          if SICCD ge 5440 and SICCD le 5449   then FF48='RTAIL';
          if SICCD ge 5450 and SICCD le 5459   then FF48='RTAIL';
          if SICCD ge 5460 and SICCD le 5469   then FF48='RTAIL';
          if SICCD ge 5490 and SICCD le 5499   then FF48='RTAIL';
          if SICCD ge 5500 and SICCD le 5500   then FF48='RTAIL';
          if SICCD ge 5510 and SICCD le 5529   then FF48='RTAIL';
          if SICCD ge 5530 and SICCD le 5539   then FF48='RTAIL';
          if SICCD ge 5540 and SICCD le 5549   then FF48='RTAIL';
          if SICCD ge 5550 and SICCD le 5559   then FF48='RTAIL';
          if SICCD ge 5560 and SICCD le 5569   then FF48='RTAIL';
          if SICCD ge 5570 and SICCD le 5579   then FF48='RTAIL';
          if SICCD ge 5590 and SICCD le 5599   then FF48='RTAIL';
          if SICCD ge 5600 and SICCD le 5699   then FF48='RTAIL';
          if SICCD ge 5700 and SICCD le 5700   then FF48='RTAIL';
          if SICCD ge 5710 and SICCD le 5719   then FF48='RTAIL';
          if SICCD ge 5720 and SICCD le 5722   then FF48='RTAIL';
          if SICCD ge 5730 and SICCD le 5733   then FF48='RTAIL';
          if SICCD ge 5734 and SICCD le 5734   then FF48='RTAIL';
          if SICCD ge 5735 and SICCD le 5735   then FF48='RTAIL';
          if SICCD ge 5736 and SICCD le 5736   then FF48='RTAIL';
          if SICCD ge 5750 and SICCD le 5799   then FF48='RTAIL';
          if SICCD ge 5900 and SICCD le 5900   then FF48='RTAIL';
          if SICCD ge 5910 and SICCD le 5912   then FF48='RTAIL';
          if SICCD ge 5920 and SICCD le 5929   then FF48='RTAIL';
          if SICCD ge 5930 and SICCD le 5932   then FF48='RTAIL';
          if SICCD ge 5940 and SICCD le 5940   then FF48='RTAIL';
          if SICCD ge 5941 and SICCD le 5941   then FF48='RTAIL';
          if SICCD ge 5942 and SICCD le 5942   then FF48='RTAIL';
          if SICCD ge 5943 and SICCD le 5943   then FF48='RTAIL';
          if SICCD ge 5944 and SICCD le 5944   then FF48='RTAIL';
          if SICCD ge 5945 and SICCD le 5945   then FF48='RTAIL';
          if SICCD ge 5946 and SICCD le 5946   then FF48='RTAIL';
          if SICCD ge 5947 and SICCD le 5947   then FF48='RTAIL';
          if SICCD ge 5948 and SICCD le 5948   then FF48='RTAIL';
          if SICCD ge 5949 and SICCD le 5949   then FF48='RTAIL';
          if SICCD ge 5950 and SICCD le 5959   then FF48='RTAIL';
          if SICCD ge 5960 and SICCD le 5969   then FF48='RTAIL';
          if SICCD ge 5970 and SICCD le 5979   then FF48='RTAIL';
          if SICCD ge 5980 and SICCD le 5989   then FF48='RTAIL';
          if SICCD ge 5990 and SICCD le 5990   then FF48='RTAIL';
          if SICCD ge 5992 and SICCD le 5992   then FF48='RTAIL';
          if SICCD ge 5993 and SICCD le 5993   then FF48='RTAIL';
          if SICCD ge 5994 and SICCD le 5994   then FF48='RTAIL';
          if SICCD ge 5995 and SICCD le 5995   then FF48='RTAIL';
          if SICCD ge 5999 and SICCD le 5999   then FF48='RTAIL';
	if FF48='RTAIL' then FF48IndCode=42;

*43 Meals  Restaraunts, Hotels, Motels;
          if SICCD ge 5800 and SICCD le 5819   then FF48='MEALS';
          if SICCD ge 5820 and SICCD le 5829   then FF48='MEALS';
          if SICCD ge 5890 and SICCD le 5899   then FF48='MEALS';
          if SICCD ge 7000 and SICCD le 7000   then FF48='MEALS';
          if SICCD ge 7010 and SICCD le 7019   then FF48='MEALS';
          if SICCD ge 7040 and SICCD le 7049   then FF48='MEALS';
          if SICCD ge 7213 and SICCD le 7213   then FF48='MEALS';
	if FF48='MEALS' then FF48IndCode=43;


*44 Banks  Banking;
          if SICCD ge 6000 and SICCD le 6000   then FF48='BANKS';
          if SICCD ge 6010 and SICCD le 6019   then FF48='BANKS';
          if SICCD ge 6020 and SICCD le 6020   then FF48='BANKS';
          if SICCD ge 6021 and SICCD le 6021   then FF48='BANKS';
          if SICCD ge 6022 and SICCD le 6022   then FF48='BANKS';
          if SICCD ge 6023 and SICCD le 6024   then FF48='BANKS';
          if SICCD ge 6025 and SICCD le 6025   then FF48='BANKS';
          if SICCD ge 6026 and SICCD le 6026   then FF48='BANKS';
          if SICCD ge 6027 and SICCD le 6027   then FF48='BANKS';
          if SICCD ge 6028 and SICCD le 6029   then FF48='BANKS';
          if SICCD ge 6030 and SICCD le 6036   then FF48='BANKS';
          if SICCD ge 6040 and SICCD le 6059   then FF48='BANKS';
          if SICCD ge 6060 and SICCD le 6062   then FF48='BANKS';
          if SICCD ge 6080 and SICCD le 6082   then FF48='BANKS';
          if SICCD ge 6090 and SICCD le 6099   then FF48='BANKS';
          if SICCD ge 6100 and SICCD le 6100   then FF48='BANKS';
          if SICCD ge 6110 and SICCD le 6111   then FF48='BANKS';
          if SICCD ge 6112 and SICCD le 6113   then FF48='BANKS';
          if SICCD ge 6120 and SICCD le 6129   then FF48='BANKS';
          if SICCD ge 6130 and SICCD le 6139   then FF48='BANKS';
          if SICCD ge 6140 and SICCD le 6149   then FF48='BANKS';
          if SICCD ge 6150 and SICCD le 6159   then FF48='BANKS';
          if SICCD ge 6160 and SICCD le 6169   then FF48='BANKS';
          if SICCD ge 6170 and SICCD le 6179   then FF48='BANKS';
          if SICCD ge 6190 and SICCD le 6199   then FF48='BANKS';
	if FF48='BANKS' then FF48IndCode=44;

*45 Insur  Insurance;
          if SICCD ge 6300 and SICCD le 6300   then FF48='INSUR';
          if SICCD ge 6310 and SICCD le 6319   then FF48='INSUR';
          if SICCD ge 6320 and SICCD le 6329   then FF48='INSUR';
          if SICCD ge 6330 and SICCD le 6331   then FF48='INSUR';
          if SICCD ge 6350 and SICCD le 6351   then FF48='INSUR';
          if SICCD ge 6360 and SICCD le 6361   then FF48='INSUR';
          if SICCD ge 6370 and SICCD le 6379   then FF48='INSUR';
          if SICCD ge 6390 and SICCD le 6399   then FF48='INSUR';
          if SICCD ge 6400 and SICCD le 6411   then FF48='INSUR';
	if FF48='INSUR' then FF48IndCode=45;

*46 RlEst  Real Estate;
          if SICCD ge 6500 and SICCD le 6500   then FF48='RLEST';
          if SICCD ge 6510 and SICCD le 6510   then FF48='RLEST';
          if SICCD ge 6512 and SICCD le 6512   then FF48='RLEST';
          if SICCD ge 6513 and SICCD le 6513   then FF48='RLEST';
          if SICCD ge 6514 and SICCD le 6514   then FF48='RLEST';
          if SICCD ge 6515 and SICCD le 6515   then FF48='RLEST';
          if SICCD ge 6517 and SICCD le 6519   then FF48='RLEST';
          if SICCD ge 6520 and SICCD le 6529   then FF48='RLEST';
          if SICCD ge 6530 and SICCD le 6531   then FF48='RLEST';
          if SICCD ge 6532 and SICCD le 6532   then FF48='RLEST';
          if SICCD ge 6540 and SICCD le 6541   then FF48='RLEST';
          if SICCD ge 6550 and SICCD le 6553   then FF48='RLEST';
          if SICCD ge 6590 and SICCD le 6599   then FF48='RLEST';
          if SICCD ge 6610 and SICCD le 6611   then FF48='RLEST';
	if FF48='RLEST' then FF48IndCode=46;

*47 Fin    Trading;
          if SICCD ge 6200 and SICCD le 6299   then FF48='FIN';
          if SICCD ge 6700 and SICCD le 6700   then FF48='FIN';
          if SICCD ge 6710 and SICCD le 6719   then FF48='FIN';
          if SICCD ge 6720 and SICCD le 6722   then FF48='FIN';
          if SICCD ge 6723 and SICCD le 6723   then FF48='FIN';
          if SICCD ge 6724 and SICCD le 6724   then FF48='FIN';
          if SICCD ge 6725 and SICCD le 6725   then FF48='FIN';
          if SICCD ge 6726 and SICCD le 6726   then FF48='FIN';
          if SICCD ge 6730 and SICCD le 6733   then FF48='FIN';
          if SICCD ge 6740 and SICCD le 6779   then FF48='FIN';
          if SICCD ge 6790 and SICCD le 6791   then FF48='FIN';
          if SICCD ge 6792 and SICCD le 6792   then FF48='FIN';
          if SICCD ge 6793 and SICCD le 6793   then FF48='FIN';
          if SICCD ge 6794 and SICCD le 6794   then FF48='FIN';
          if SICCD ge 6795 and SICCD le 6795   then FF48='FIN';
          if SICCD ge 6798 and SICCD le 6798   then FF48='FIN';
          if SICCD ge 6799 and SICCD le 6799   then FF48='FIN';
	if FF48='FIN' then FF48IndCode=47;

*48 Other  Almost Nothing;
          if SICCD ge 4950 and SICCD le 4959   then FF48='OTHER';
          if SICCD ge 4960 and SICCD le 4961   then FF48='OTHER';
          if SICCD ge 4970 and SICCD le 4971   then FF48='OTHER';
          if SICCD ge 4990 and SICCD le 4991   then FF48='OTHER'; 
          if SICCD ge 9990 and SICCD le 9999   then FF48='OTHER'; 
  	if FF48='OTHER' then FF48IndCode=48;
run;

proc sort data=BM; by date1 FF48IndCode; run;
proc means data=BM noprint;
  where missing(FF48IndCode)=0;
  by date1 FF48IndCode;
  var LogBM;
  output out=MedLogBM median=LogBM_IndMed N=Obs;
quit;
*1680 obs;
proc sql;
  create table BM as
  select distinct a.*, b.LogBM_IndMed, a.LogBM-b.LogBM_IndMed as LogBM_IndAdj label='FF48 IndAdj LogBM'
  from BM as a left join MedLogBM as b
  on a.date1=b.date1 and a.FF48IndCode=b.FF48IndCode;
quit;
*197122 obs;


/* Step 4.3.7 For stk-qtrs extracted in 4.1, calculate BM */
proc sql;
  create table CRSP_StkQtrs_BM as
  select distinct a.qdate, a.permno, a.EXCHCD, b.BM, b.LogBM, b.LogBM_IndMed, b.LogBM_IndAdj
  from CRSP_StkQtrs_Size as a left join BM as b
  on a.permno=b.permno and b.date1<a.qdate<=b.date2;
quit;
*684157 obs;

/* Step 4.4. Calculate Dividend yield */
/* Step 4.4.1 Calculate dividend as of fiscal-yr end from Compustat extract */
proc sql;
   create table DivYield  as 
   select distinct a.gvkey, a.date_fyend, c.permno, c.hexcd, (a.DVC/a.CSHO)/abs(c.prc) as DY label='Dividend Yield (as of fiscal-yr end)' 
   from comp_extract as a, a_ccm.CCMXPF_LINKTABLE as b, crspa.msf as c
   where a.gvkey=b.gvkey and ((b.linkdt<=c.date<=b.linkenddt) or (b.linkdt<=c.date and b.linkenddt=.E) or (c.date<=b.linkenddt and b.linkdt=.B)) and 
         b.lpermno=c.permno and year(a.date_fyend)=year(c.date) and month(a.date_fyend)=month(c.date) and (abs(c.prc)*c.shrout)>0;
quit;
*215999 obs;

/* Step 4.4.2 Dividend Yields as of qdate for stock-quarters in CRSP_StkQtrs_Size */
proc sql;
  create table CRSP_StkQtrs_DivYield as
  select distinct a.qdate, a.permno, a.EXCHCD, b.date_fyend, b.DY label='Dividend Yield (as of fiscal-yr ended before qdate)'
  from (select distinct qdate, permno, EXCHCD from CRSP_StkQtrs_Size) as a, DivYield as b
  where a.permno=b.permno and b.date_fyend<a.qdate /* for each permno-qdate, pick immediate date_fyend before qdate */
  group by a.qdate, a.permno
  having a.qdate-b.date_fyend=min(a.qdate-b.date_fyend);
quit;
* 617475 obs;

/* Step 4.4.3 Winsorize Dividend Yields at 1/99 */
proc univariate data=CRSP_StkQtrs_DivYield noprint;
  by qdate;
  var DY;
  output out=DYPctl pctlpts=1 99 pctlpre=DY;
quit;
*136 obs;
proc sql;
  create table CRSP_StkQtrs_DivYield as
  select distinct a.*, b.DY1, b.DY99
  from CRSP_StkQtrs_DivYield as a, DYPctl as b
  where a.qdate=b.qdate;
quit;
*617475 obs;

data CRSP_StkQtrs_DivYield;
  set CRSP_StkQtrs_DivYield;
  if missing(DY)=0 and DY<DY1 then DY=DY1;
  if missing(DY)=0 and DY>DY99 then DY=DY99;
  drop DY1 DY99;
run;



/* Step 4.5. Consolidate all characteristics in a single dataset */
proc sql;
  create table CRSP_StkQtrs_Chrs as
  select distinct a.*, b.bm as BM, b.LogBM, b.LogBM_IndMed label='LogBM (FF48Ind Median, decdate)', b.LogBM_IndAdj label='LogBM (FF48 Ind-Adj, decdate)'
  from CRSP_StkQtrs_Size as a left join CRSP_StkQtrs_BM as b
  on a.qdate=b.qdate and a.permno=b.permno;
  *684157 obs;

  create table CRSP_StkQtrs_Chrs as
  select distinct a.*, b.Mom
  from CRSP_StkQtrs_Chrs as a left join CRSP_StkQtrs_Mom as b
  on a.qdate=b.qdate and a.permno=b.permno;

  create table CRSP_StkQtrs_Chrs as
  select distinct a.*, b.DY
  from CRSP_StkQtrs_Chrs as a left join CRSP_StkQtrs_DivYield as b
  on a.qdate=b.qdate and a.permno=b.permno;
quit;
proc sort data=CRSP_StkQtrs_Chrs nodupkey; by qdate permno; run;
data ash.CRSP_StkQtrs_Chrs; set CRSP_StkQtrs_Chrs; run;
proc sql;
  drop table BM0, Comp_Extract, Comp_Extract_BE, CRSP_StkQtrs_BM, CRSP_StkQtrs_DivYield, CRSP_StkQtrs_Mom, CRSP_StkQtrs_Size, 
             DivYield, DYPctl, MedLogBM, LogBMPctl, MomPctl, Size_June;
quit; 


/************************************************************************************************************************/
/************************************************************************************************************************/
/*************************************** METHOD 4: Z-SCORES WITH ORTHOGONALIZATION **************************************/
/************************************************************************************************************************/
/************************************************************************************************************************/

/****************************************************************/
/********************* Stock Level 2D Vector ********************/
/****************************************************************/
*** Step 1. Obtain stock characteristics, i.e. use CRSP_StkQtrs_Chrs;
*** Step 2. Keep firm with non-missing LogSize and LogBM;
data CRSP_StkQtrs_Chrs_2D_ZS; /* ZS = Orthogonalization, Z-Scores */
  set CRSP_StkQtrs_Chrs;
  if missing(LogSize)=0 and missing(LogBM)=0;
  keep qdate permno EXCHCD LogSize LogBM;
run;
*577551 obs;

*** Step 3. Standardize LogSize and LogBM for NYSE stocks;
proc sort data=CRSP_StkQtrs_Chrs_2D_ZS nodupkey; by qdate permno; run;
proc univariate data=CRSP_StkQtrs_Chrs_2D_ZS noprint;
  where EXCHCD=1;
  by qdate;
  var LogSize LogBM;
  output out=Temp Mean=Mean_LogSize Mean_LogBM Std=Std_LogSize Std_LogBM;
quit;
*136 obs;

*Calculate standardized variables for ALL stocks;
proc sql;
  create table CRSP_StkQtrs_Chrs_2D_ZS as
  select distinct a.qdate, a.permno, a.EXCHCD, 
                  (a.LogSize-b.Mean_LogSize)/b.Std_LogSize as LogSize, 
                  (a.LogBM-b.Mean_LogBM)/b.Std_LogBM as LogBM
  from CRSP_StkQtrs_Chrs_2D_ZS as a, Temp as b
  where a.qdate=b.qdate;
quit;
*577551 obs;

*** Step 4. Regress LogBM on LogSize, ONLY for NYSE stocks and take the residuals, rLogBM;
data NYSE_Chrs_2D_ZS AMEXNASD_Chrs_2D_ZS;
  set CRSP_StkQtrs_Chrs_2D_ZS;
  if EXCHCD=1 then output NYSE_Chrs_2D_ZS;
  if EXCHCD in (2,3) then output AMEXNASD_Chrs_2D_ZS;
run;
proc sort data=NYSE_Chrs_2D_ZS; by qdate; run;
*172149 obs;

proc reg data=NYSE_Chrs_2D_ZS noprint outest=Parms_NYSE_2D; 
  by qdate; 
  model LogBM = LogSize; 
  output out=NYSE_Chrs_2D_ZS R=rLogBM;
quit;


*** Step 5. Predicted and residual, rLogBM for AMEX and NASD firms;
proc sql;
  create table AMEXNASD_Chrs_2D_ZS as
  select distinct a.*, a.LogBM - (b.Intercept + (b.LogSize*a.LogSize)) as rLogBM 
  from AMEXNASD_Chrs_2D_ZS as a, Parms_NYSE_2D as b
  where a.qdate=b.qdate;
quit; 
*577551 obs;

*Combine datasets;
data CRSP_StkQtrs_Chrs_2D_ZS;
  set NYSE_Chrs_2D_ZS AMEXNASD_Chrs_2D_ZS;
run;
proc sort data=CRSP_StkQtrs_Chrs_2D_ZS nodupkey; by qdate permno; run;




/****************************************************************/
/********************* Stock Level 3D Vector ********************/
/****************************************************************/

*** Step 1. Obtain stock characteristics, i.e. use CRSP_StkQtrs_Chrs;
*** Step 2. Keep firm with non-missing LogSize and LogBM;
data CRSP_StkQtrs_Chrs_3D_ZS;
  set CRSP_StkQtrs_Chrs;
  if missing(LogSize)=0 and missing(LogBM)=0 and missing(Mom)=0;
  keep qdate permno EXCHCD LogSize LogBM Mom;
run;
*575146 obs;

*** Step 3. Standardize LogSize, LogBM and Mom for NYSE stocks;
proc sort data=CRSP_StkQtrs_Chrs_3D_ZS nodupkey; by qdate permno; run;
proc univariate data=CRSP_StkQtrs_Chrs_3D_ZS noprint;
  where EXCHCD=1;
  by qdate;
  var LogSize LogBM Mom;
  output out=Temp Mean=Mean_LogSize Mean_LogBM Mean_Mom Std=Std_LogSize Std_LogBM Std_Mom;
quit;
*Calculate standardized variables for ALL stocks;
proc sql;
  create table CRSP_StkQtrs_Chrs_3D_ZS as
  select distinct a.qdate, a.permno, a.EXCHCD, 
                  (a.LogSize-b.Mean_LogSize)/b.Std_LogSize as LogSize, 
                  (a.LogBM-b.Mean_LogBM)/b.Std_LogBM as LogBM,
                  (a.Mom-b.Mean_Mom)/b.Std_Mom as Mom  
  from CRSP_StkQtrs_Chrs_3D_ZS as a, Temp as b
  where a.qdate=b.qdate;
quit;



*** Step 4. Regress and take the residuals;
data NYSE_Chrs_3D_ZS AMEXNASD_Chrs_3D_ZS;
  set CRSP_StkQtrs_Chrs_3D_ZS;
  if EXCHCD=1 then output NYSE_Chrs_3D_ZS;
  if EXCHCD in (2,3) then output AMEXNASD_Chrs_3D_ZS;
run;
proc sort data=NYSE_Chrs_3D_ZS; by qdate; run;
proc reg data=NYSE_Chrs_3D_ZS noprint outest=Parms1_NYSE_3D; 
  by qdate; 
  model LogBM = LogSize; 
  output out=NYSE_Chrs_3D_ZS R=rLogBM;
quit;
proc reg data=NYSE_Chrs_3D_ZS noprint outest=Parms2_NYSE_3D; 
  by qdate; 
  model Mom = LogSize LogBM;
  output out=NYSE_Chrs_3D_ZS R=rMom;
quit;



*** Step 5. Predicted and residual, rLogBM, rMom for AMEX and NASD firms;
proc sql;
  create table AMEXNASD_Chrs_3D_ZS as
  select distinct a.*, a.LogBM - (b.Intercept + (b.LogSize*a.LogSize)) as rLogBM, a.Mom - (c.Intercept + (c.LogSize*a.LogSize) + (c.LogBM*a.LogBM)) as rMom 
  from AMEXNASD_Chrs_3D_ZS as a, Parms1_NYSE_3D as b, Parms2_NYSE_3D as c
  where a.qdate=b.qdate=c.qdate;
quit; 
*Combine datasets;
data CRSP_StkQtrs_Chrs_3D_ZS;
  set NYSE_Chrs_3D_ZS AMEXNASD_Chrs_3D_ZS;
run;
proc sort data=CRSP_StkQtrs_Chrs_3D_ZS nodupkey; by qdate permno; run;




/****************************************************************/
/********************* Stock Level 4D Vector ********************/
/****************************************************************/
*** Step 1. Obtain stock characteristics, i.e. use CRSP_StkQtrs_Chrs;
*** Step 2. Keep firm with non-missing LogSize and LogBM;
data CRSP_StkQtrs_Chrs_4D_ZS;
  set CRSP_StkQtrs_Chrs;
  if missing(LogSize)=0 and missing(LogBM)=0 and missing(Mom)=0 and missing(DY)=0;
  keep qdate permno EXCHCD LogSize LogBM Mom DY;
run;
*572699 obs;

*** Step 3. Standardize LogSize, LogBM, Mom and DY for NYSE stocks;
proc sort data=CRSP_StkQtrs_Chrs_4D_ZS nodupkey; by qdate permno; run;
proc univariate data=CRSP_StkQtrs_Chrs_4D_ZS noprint;
  where EXCHCD=1;
  by qdate;
  var LogSize LogBM Mom DY;
  output out=Temp Mean=Mean_LogSize Mean_LogBM Mean_Mom Mean_DY Std=Std_LogSize Std_LogBM Std_Mom Std_DY;
quit;
*Calculate standardized variables for ALL stocks;
proc sql;
  create table CRSP_StkQtrs_Chrs_4D_ZS as
  select distinct a.qdate, a.permno, a.EXCHCD, 
                  (a.LogSize-b.Mean_LogSize)/b.Std_LogSize as LogSize, 
                  (a.LogBM-b.Mean_LogBM)/b.Std_LogBM as LogBM,
                  (a.Mom-b.Mean_Mom)/b.Std_Mom as Mom,
                  (a.DY-b.Mean_DY)/b.Std_DY as DY 
  from CRSP_StkQtrs_Chrs_4D_ZS as a, Temp as b
  where a.qdate=b.qdate;
quit;



*** Step 4. Regress and take the residuals;
data NYSE_Chrs_4D_ZS AMEXNASD_Chrs_4D_ZS;
  set CRSP_StkQtrs_Chrs_4D_ZS;
  if EXCHCD=1 then output NYSE_Chrs_4D_ZS;
  if EXCHCD in (2,3) then output AMEXNASD_Chrs_4D_ZS;
run;
proc sort data=NYSE_Chrs_4D_ZS; by qdate; run;
proc reg data=NYSE_Chrs_4D_ZS noprint outest=Parms1_NYSE_4D; 
  by qdate; 
  model LogBM = LogSize; 
  output out=NYSE_Chrs_4D_ZS R=rLogBM;
quit;
proc reg data=NYSE_Chrs_4D_ZS noprint outest=Parms2_NYSE_4D; 
  by qdate; 
  model Mom = LogSize LogBM;
  output out=NYSE_Chrs_4D_ZS R=rMom;
quit;
proc reg data=NYSE_Chrs_4D_ZS noprint outest=Parms3_NYSE_4D; 
  by qdate; 
  model DY = LogSize LogBM Mom;
  output out=NYSE_Chrs_4D_ZS R=rDY;
quit;


*** Step 5. Predicted and residual, rLogBM, rMom, rDY for AMEX and NASD firms;
proc sql;
  create table AMEXNASD_Chrs_4D_ZS as
  select distinct a.*, a.LogBM - (b.Intercept + (b.LogSize*a.LogSize)) as rLogBM, a.Mom - (c.Intercept + (c.LogSize*a.LogSize) + (c.LogBM*a.LogBM)) as rMom,
                  a.DY - (d.Intercept + (d.LogSize*a.LogSize) + (d.LogBM*a.LogBM) + (d.Mom*a.Mom)) as rDY 
  from AMEXNASD_Chrs_4D_ZS as a, Parms1_NYSE_4D as b, Parms2_NYSE_4D as c, Parms3_NYSE_4D as d
  where a.qdate=b.qdate=c.qdate=d.qdate;
quit; 
*Combine datasets;
data CRSP_StkQtrs_Chrs_4D_ZS;
  set NYSE_Chrs_4D_ZS AMEXNASD_Chrs_4D_ZS;
run;
proc sort data=CRSP_StkQtrs_Chrs_4D_ZS nodupkey; by qdate permno; run;




/***************************************************************/
/********************* Fund Level Vectors ********************/
/***************************************************************/

/*********************** 2D Vector *******************/
*** Rescale weights of quartely portfolios for stocks with non-missing LogSize, rLogBM;
proc sql;
  create table Fund_Chrs_2D_ZS as
  select distinct a.wficn, a.qdate, a.permno, a.wt_rdate, b.LogSize, b.rLogBM
  from Fund_Holdings_Qtrly as a, CRSP_StkQtrs_Chrs_2D_ZS as b
  where a.permno=b.permno and a.qdate=b.qdate and 
        missing(a.wt_rdate)=0 and missing(b.LogSize)=0 and missing(b.rLogBM)=0;
  *16944890 obs;

  *Rescale weights;
  create table Fund_Chrs_2D_ZS as
  select distinct wficn, qdate, permno, wt_rdate/sum(wt_rdate) as wt_rdate_rescaled, LogSize, rLogBM
  from Fund_Chrs_2D_ZS
  group by wficn, qdate;
quit;


*** Obtain Fund-level 2D Vector = [LogSize, rLogBM];
proc sql;
  create table Fund_Chrs_2D_ZS as
  select distinct wficn, qdate, count(distinct permno) as nonmiss_nstk, 
                  sum(wt_rdate_rescaled*LogSize) as LogSize label='Fund Level LogSize',
                  sum(wt_rdate_rescaled*rLogBM) as rLogBM label='Fund Level rLogBM'
  from Fund_Chrs_2D_ZS
  group by wficn, qdate;
  
  create table Fund_Chrs_2D_ZS as
  select distinct a.wficn, a.qdate, (b.nonmiss_nstk/a.nstocks)*100 as nstocks_nmiss format=6.2 label='% of Stocks Used to Obtain Fund Level Chrs', 
                  b.LogSize, b.rLogBM
  from Tfn_Fund_Qtrs as a, Fund_Chrs_2D_ZS as b
  where a.wficn=b.wficn and a.qdate=b.qdate;
  *165147 fund-qtrs;
quit;



/*********************** 3D Vector *******************/
*** Rescale weights of quartely portfolios for stocks with non-missing LogSize, rLogBM, rMom;
proc sql;
  create table Fund_Chrs_3D_ZS as
  select distinct a.wficn, a.qdate, a.permno, a.wt_rdate, b.LogSize, b.rLogBM, b.rMom
  from Fund_Holdings_Qtrly as a, CRSP_StkQtrs_Chrs_3D_ZS as b
  where a.permno=b.permno and a.qdate=b.qdate and 
        missing(a.wt_rdate)=0 and missing(b.LogSize)=0 and missing(b.rLogBM)=0 and missing(b.rMom)=0;
  *16925507 obs;

  *Rescale weights;
  create table Fund_Chrs_3D_ZS as
  select distinct wficn, qdate, permno, wt_rdate/sum(wt_rdate) as wt_rdate_rescaled, LogSize, rLogBM, rMom
  from Fund_Chrs_3D_ZS
  group by wficn, qdate;
quit;


*** Obtain Fund-level 3D Vector = [LogSize, rLogBM, rMom];
proc sql;
  create table Fund_Chrs_3D_ZS as
  select distinct wficn, qdate, count(distinct permno) as nonmiss_nstk, 
                  sum(wt_rdate_rescaled*LogSize) as LogSize label='Fund Level LogSize',
                  sum(wt_rdate_rescaled*rLogBM) as rLogBM label='Fund Level rLogBM',
				  sum(wt_rdate_rescaled*rMom) as rMom label='Fund Level rMom'
  from Fund_Chrs_3D_ZS
  group by wficn, qdate;
  * 165145 obs;

  create table Fund_Chrs_3D_ZS as
  select distinct a.wficn, a.qdate, (b.nonmiss_nstk/a.nstocks)*100 as nstocks_nmiss format=6.2 label='% of Stocks Used to Obtain Fund Level Chrs', 
                  b.LogSize, b.rLogBM, b.rMom
  from Tfn_Fund_Qtrs as a, Fund_Chrs_3D_ZS as b
  where a.wficn=b.wficn and a.qdate=b.qdate;
  *151,694 fund-qtrs;
quit;



/*********************** 4D Vector *******************/
*** Rescale weights of quartely portfolios for stocks with non-missing LogSize, rLogBM, rMom, rDY;
proc sql;
  create table Fund_Chrs_4D_ZS as
  select distinct a.wficn, a.qdate, a.permno, a.wt_rdate, b.LogSize, b.rLogBM, b.rMom, b.rDY
  from Fund_Holdings_Qtrly as a, CRSP_StkQtrs_Chrs_4D_ZS as b
  where a.permno=b.permno and a.qdate=b.qdate and 
        missing(a.wt_rdate)=0 and missing(b.LogSize)=0 and missing(b.rLogBM)=0 and missing(b.rMom)=0 and missing(b.rDY)=0;
  *16880233 obs;

  *Rescale weights;
  create table Fund_Chrs_4D_ZS as
  select distinct wficn, qdate, permno, wt_rdate/sum(wt_rdate) as wt_rdate_rescaled, LogSize, rLogBM, rMom, rDY
  from Fund_Chrs_4D_ZS
  group by wficn, qdate;
quit;


*** Obtain Fund-level 4D Vector = [LogSize, rLogBM, rMom];
proc sql;
  create table Fund_Chrs_4D_ZS as
  select distinct wficn, qdate, count(distinct permno) as nonmiss_nstk, 
                  sum(wt_rdate_rescaled*LogSize) as LogSize label='Fund Level LogSize',
                  sum(wt_rdate_rescaled*rLogBM) as rLogBM label='Fund Level rLogBM',
				  sum(wt_rdate_rescaled*rMom) as rMom label='Fund Level rMom',
				  sum(wt_rdate_rescaled*rDY) as rDY label='Fund Level rDY'
  from Fund_Chrs_4D_ZS
  group by wficn, qdate;
  *165142 obs;

  create table Fund_Chrs_4D_ZS as
  select distinct a.wficn, a.qdate, (b.nonmiss_nstk/a.nstocks)*100 as nstocks_nmiss format=6.2 label='% of Stocks Used to Obtain Fund Level Chrs', 
                  b.LogSize, b.rLogBM, b.rMom, b.rDY
  from Tfn_Fund_Qtrs as a, Fund_Chrs_4D_ZS as b
  where a.wficn=b.wficn and a.qdate=b.qdate;
  *165142 fund-qtrs;
quit;





/*****************************************************************************/
/*** Final Step: Combine Datasets, Update Them and Sort With No Duplicates ***/
/*****************************************************************************/
***TFN Datasets;
*Combine Tfn_Fund_Qtrs and fund characteristic vectors;
proc sql;
  create table Tfnfund_Chrs_Qtrly_ZS_2D as 
  select distinct a.wficn, a.rdate, a.qdate label='Qtr End Date', 
         a.broughtahead label='Dummy Var (Takes 1 if Port Brought Fwd from Last Qtr)', 
         a.nstocks label='No. of Stocks Held as of qdate', a.mtna, 
         b.nstocks_nmiss as nstocks_chrs label='% of Stocks Used to Obtain Fund Level Chrs', 
         b.LogSize, b.rLogBM
  from Tfn_Fund_Qtrs as a, Fund_Chrs_2D_ZS as b
  where a.qdate=b.qdate and a.wficn=b.wficn
  order by a.qdate, a.wficn;
  *165147 fund-qtrs;

  create table Tfnfund_Chrs_Qtrly_ZS_3D as 
  select distinct a.wficn, a.rdate, a.qdate label='Qtr End Date', 
         a.broughtahead label='Dummy Var (Takes 1 if Port Brought Fwd from Last Qtr)', 
         a.nstocks label='No. of Stocks Held as of qdate', a.mtna, 
         b.nstocks_nmiss as nstocks_chrs label='% of Stocks Used to Obtain Fund Level Chrs', 
         b.LogSize, b.rLogBM, b.rMom
  from Tfn_Fund_Qtrs as a, Fund_Chrs_3D_ZS as b
  where a.qdate=b.qdate and a.wficn=b.wficn
  order by a.qdate, a.wficn;
  *165145 fund-qtrs;

  create table Tfnfund_Chrs_Qtrly_ZS_4D as 
  select distinct a.wficn, a.rdate, a.qdate label='Qtr End Date', 
         a.broughtahead label='Dummy Var (Takes 1 if Port Brought Fwd from Last Qtr)', 
         a.nstocks label='No. of Stocks Held as of qdate', a.mtna, 
         b.nstocks_nmiss as nstocks_chrs label='% of Stocks Used to Obtain Fund Level Chrs', 
         b.LogSize, b.rLogBM, b.rMom, b.rDY
  from Tfn_Fund_Qtrs as a, Fund_Chrs_4D_ZS as b
  where a.qdate=b.qdate and a.wficn=b.wficn
  order by a.qdate, a.wficn;
  *165142 fund-qtrs;
quit;


***Screen: Exclude those Fund-Qtrs for which lipper_class is not available from 31Dec1999;
proc sql;
  create table Tfnfund_Chrs_Qtrly_ZS_2D as
  select distinct a.*, b.lipper_class 
  from Tfnfund_Chrs_Qtrly_ZS_2D as a left join Fund_Style as b
  on a.wficn=b.wficn and b.begdt<=a.qdate<=b.enddt;
quit;
*165642 obs;

proc sort data=Tfnfund_Chrs_Qtrly_ZS_2D; by wficn qdate descending lipper_class; run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_2D nodupkey; by wficn qdate; run;
data Tfnfund_Chrs_Qtrly_ZS_2D;
  set Tfnfund_Chrs_Qtrly_ZS_2D;
  if qdate>="31Dec1999"d and missing(lipper_class)=1 then delete;
run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_2D nodupkey; by wficn qdate; run;
* 147159 fund-qtrs;


proc sql;
  create table Tfnfund_Chrs_Qtrly_ZS_3D as
  select distinct a.*, b.lipper_class 
  from Tfnfund_Chrs_Qtrly_ZS_3D as a left join Fund_Style as b
  on a.wficn=b.wficn and b.begdt<=a.qdate<=b.enddt;
quit;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_3D; by wficn qdate descending lipper_class; run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_3D nodupkey; by wficn qdate; run;
data Tfnfund_Chrs_Qtrly_ZS_3D;
  set Tfnfund_Chrs_Qtrly_ZS_3D;
  if qdate>="31Dec1999"d and missing(lipper_class)=1 then delete;
run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_3D nodupkey; by wficn qdate; run;
*147158 fund-qtrs;


proc sql;
  create table Tfnfund_Chrs_Qtrly_ZS_4D as
  select distinct a.*, b.lipper_class 
  from Tfnfund_Chrs_Qtrly_ZS_4D as a left join Fund_Style as b
  on a.wficn=b.wficn and b.begdt<=a.qdate<=b.enddt;
quit;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_4D; by wficn qdate descending lipper_class; run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_4D nodupkey; by wficn qdate; run;
data Tfnfund_Chrs_Qtrly_ZS_4D;
  set Tfnfund_Chrs_Qtrly_ZS_4D;
  if qdate>="31Dec1999"d and missing(lipper_class)=1 then delete;
run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_4D nodupkey; by wficn qdate; run;
*147156 fund-qtrs;


***Copy to ash;
data ash.Tfnfund_Chrs_Qtrly_ZS_2D; set Tfnfund_Chrs_Qtrly_ZS_2D; run;
data ash.Tfnfund_Chrs_Qtrly_ZS_3D; set Tfnfund_Chrs_Qtrly_ZS_3D; run;
data ash.Tfnfund_Chrs_Qtrly_ZS_4D; set Tfnfund_Chrs_Qtrly_ZS_4D; run;





/***********************************************************************************************/
/***********************************************************************************************/
/********************************* FINAL CRSP AND THOMSON DATASETS *****************************/
/***********************************************************************************************/
/***********************************************************************************************/


***Union of unique funds;
proc sql;
  create table UniqueFunds as
  select distinct g.wficn from Tfnfund_Chrs_Qtrly_ZS_2D as g
  union
  select distinct h.wficn from Tfnfund_Chrs_Qtrly_ZS_3D as h
  union
  select distinct i.wficn from Tfnfund_Chrs_Qtrly_ZS_4D as i;
  *3632 Unique Funds; 
quit;


***Union of unique Fund-Qtrs;
proc sql;
  create table UniqueFundQtrs as
  select distinct g.wficn, g.qdate from Tfnfund_Chrs_Qtrly_ZS_2D as g
  union
  select distinct h.wficn, h.qdate from Tfnfund_Chrs_Qtrly_ZS_3D as h
  union
  select distinct i.wficn, i.qdate from Tfnfund_Chrs_Qtrly_ZS_4D as i;
  *147159 Fund-Qtrs; 
quit;


***TFN Holding Dataset;
***Select Holdings Unique Fund-Qtrs;
proc sql;  
  create table Tfnfund_Holdings_Qtrly as
  select distinct a.wficn, a.qdate label='Qtr End Date', a.permno, a.wt_rdate label='Stock Weight (rdate)', a.SICCD
  from Fund_Holdings_Qtrly as a, UniqueFundQtrs as b
  where a.qdate=b.qdate and a.wficn=b.wficn
  order by a.qdate, a.wficn, a.permno;
  *17570822 fund-qtrs-stocks, these are ALL holdings, non-rescaled;
quit;


***CRSP Datasets;
proc sql;
  create table CrspEquity_Funds as
  select distinct a.*
  from Equity_Funds as a, UniqueFunds as b
  where a.wficn=b.wficn;
  *41115 obs;

  create table Crspfund_Style as
  select distinct a.*
  from Fund_Style as a, UniqueFunds as b
  where a.wficn=b.wficn;
  *19663 obs;

  create table Crspfund_Hdr_Hist as
  select distinct a.*
  from Fund_Hdr_Hist as a, UniqueFunds as b
  where a.wficn=b.wficn;
  *113402 obs;

  create table Crspfund_Chrs_Monthly as
  select distinct a.*
  from Fund_Chrs_Monthly as a, UniqueFunds as b
  where a.wficn=b.wficn;
  *684103 obs;
quit;


*CRSP Datasets;
proc sort data=CrspEquity_Funds nodupkey; by wficn crsp_fundno begdt; run;
proc sort data=Crspfund_Style nodupkey; by wficn begdt; run;
proc sort data=Crspfund_Hdr_Hist nodupkey; by wficn crsp_fundno chgdt; run;
proc sort data=Crspfund_Chrs_Monthly nodupkey; by date wficn; run;

*TFN Datasets;
proc sort data=Tfnfund_Holdings_Qtrly nodupkey; by qdate wficn permno; run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_2D nodupkey; by qdate wficn; run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_3D nodupkey; by qdate wficn; run;
proc sort data=Tfnfund_Chrs_Qtrly_ZS_4D nodupkey; by qdate wficn; run;


*Auxillary Datasets;
proc sort data=CRSP_StkQtrs_Chrs nodupkey; by qdate permno; run;


*Drop all tables from ash.manually and then copy the following tables;
*Copy datasets to ash.and from there to loacl hard drive in FCApr14;
proc datasets nolist;
  copy in=work out=ash;
  select CrspEquity_Funds Crspfund_Style Crspfund_Hdr_Hist Crspfund_Chrs_Monthly Tfnfund_Holdings_Qtrly 
		 Tfnfund_Chrs_Qtrly_ZS_2D Tfnfund_Chrs_Qtrly_ZS_3D Tfnfund_Chrs_Qtrly_ZS_4D CRSP_StkQtrs_Chrs;
quit;


*Clean RWORK of unrequired datasets;
proc sql;
  drop table Equity_Funds, Fund_Chrs_Monthly, Fund_Hdr_Hist, Fund_Holdings_Qtrly, Fund_Chrs_2D, Fund_Chrs_3D, Fund_Chrs_4D, Fund_Style, Tfn_Fund_Qtrs;
quit;

proc download data=ash.Crspfund_Chrs_Monthly out=FCSep29.Crspfund_Chrs_Monthly; run;


/***************************************************************************************/
/*** Include flows and calculate family size based on the active funds in the sample ***/
/***************************************************************************************/

**Flows;
data Crspfund_Chrs_Monthly;
  set Crspfund_Chrs_Monthly;
  Flow=((mtna-(lag_mtna*(1+mret_afxp)))/lag_mtna)*100;
  label Flow='%Growth in Monthly Flow';
run;
*684103 obs;

**Family size based on the active funds in the sample;
proc sql;
  create table temp as
  select distinct wficn, date, mgmt_cd 
  from Crspfund_Chrs_Monthly 
  order by wficn, date;

  *Earliest non-missing FamilyCode;
  create table FamilyCodes as
  select distinct wficn, date, mgmt_cd 
  from temp
  where missing(mgmt_cd)=0
  group by wficn
  having min(date)=date;
quit;
*3128 obs;

proc sql;
  create table Crspfund_Chrs_Monthly as
  select distinct a.*, b.date as date1, b.mgmt_cd as mgmt_cd1
  from Crspfund_Chrs_Monthly as a left join FamilyCodes as b
  on a.wficn=b.wficn;
quit;
*684103 obs;
data Crspfund_Chrs_Monthly;
  set Crspfund_Chrs_Monthly;
  if date<date1 and missing(mgmt_cd)=1 then mgmt_cd=mgmt_cd1;
  drop date1 mgmt_cd1;
run;
*684103 obs;
proc sql;
  create table FamAssets as
  select distinct date, mgmt_cd, sum(mtna) as FamilyAssets1 label='FamilyAssets (Month=Date) ($M)'
  from Crspfund_Chrs_Monthly
  where missing(mgmt_cd)=0
  group by date, mgmt_cd;
quit;
* 148839 obs;

proc sql;
  create table Crspfund_Chrs_Monthly as
  select distinct a.*, b.FamilyAssets1
  from Crspfund_Chrs_Monthly as a left join FamAssets as b
  on a.date=b.date and a.mgmt_cd=b.mgmt_cd;
quit;
*684103 obs;


/***********************************************************************************************************************************/
/***********************************************************************************************************************************/
/************************************************* RESULTS WITH INDUSTRY-ADJUSTED BM RATIO *****************************************/
/***********************************************************************************************************************************/
/***********************************************************************************************************************************/




/******************************************************/
/*** Obtain 2D Vector of Rank - Industry-Adj LogBM ***/
/******************************************************/

***Transfer "FCApr14.CRSP_StkQtrs_Chrs", "FCApr14.Tfnfund_Chrs_Qtrly_2D", "FCApr14.Tfnfund_Holdings_Qtrly" via WinSCP to ash;

data CRSP_StkQtrs_Chrs; set ash.CRSP_StkQtrs_Chrs; run;
data Tfnfund_Holdings_Qtrly; set ash.Tfnfund_Holdings_Qtrly; run;


* Step 5.1 Keep firm with non-missing LogSize and LogBM_IndAdj;
data CRSP_StkQtrs_Chrs_2D_BMInd;
  set CRSP_StkQtrs_Chrs;
  if missing(LogSize)=0 and missing(LogBM_IndAdj)=0;
  keep qdate permno EXCHCD LogSize LogBM_IndAdj;
run;
data NYSE_Chrs_2D AMEXNASD_Chrs_2D;
  set CRSP_StkQtrs_Chrs_2D_BMInd;
  if EXCHCD=1 then output NYSE_Chrs_2D;
  if EXCHCD in (2,3) then output AMEXNASD_Chrs_2D;
run;


* Step 5.2 Regress LogBM_IndAdj on LogSize, ONLY for NYSE stocks and take the residuals, rLogBM_IndAdj;
proc sort data=NYSE_Chrs_2D; by qdate; run;
proc reg data=NYSE_Chrs_2D noprint outest=Parms_NYSE_2D; 
  by qdate; 
  model LogBM_IndAdj = LogSize; 
  output out=NYSE_Chrs_2D R=rLogBM_IndAdj;
quit;


* Step 5.3 Obtain 2D vector for NYSE firms;
proc rank data=NYSE_Chrs_2D fraction out=NYSE_Chrs_2D;
  by qdate;
  var LogSize rLogBM;
  ranks Rank_LogSize Rank_rLogBM;
quit;


* Step 5.4 Predicted and residual, rLogBM_IndAdj for AMEX and NASD firms;
proc sql;
  create table AMEXNASD_Chrs_2D as
  select distinct a.*, a.LogBM_IndAdj - (b.Intercept + (b.LogSize*a.LogSize)) as rLogBM_IndAdj 
  from AMEXNASD_Chrs_2D as a, Parms_NYSE_2D as b
  where a.qdate=b.qdate;
  *b.LogSize is the estimated slope for NYSE firms on LogSize;
quit; 


* Step 5.5.1 Assign raash. to LogSize for AMEX and NASD firms based on the closest match for NYSE firms;
***Extract matching NYSE firms with same size;
proc sql;
  create table AMEXNASD_Chrs_2D_Size1 as
  select distinct a.qdate, a.permno, a.EXCHCD, a.LogSize, b.permno as Matching_permno, b.LogSize as Matching_LogSize
  from AMEXNASD_Chrs_2D as a, NYSE_Chrs_2D as b
  where a.qdate=b.qdate and abs(b.LogSize-a.LogSize)<=0.05
  group by a.qdate, a.permno 
  having abs(b.LogSize-a.LogSize)=min(abs(b.LogSize-a.LogSize));
quit;
proc sort data=AMEXNASD_Chrs_2D_Size1 nodupkey; by qdate permno; run;


proc sql;
  create table Unmtached_AMEXNASD_Size as
  select distinct a.* from AMEXNASD_Chrs_2D(drop=LogBM_IndAdj rLogBM_IndAdj) as a
  except
  select distinct b.* from AMEXNASD_Chrs_2D_Size1(drop=Matching_permno Matching_LogSize) as b;

  create table AMEXNASD_Chrs_2D_Size2 as
  select distinct a.qdate, a.permno, a.EXCHCD, a.LogSize, b.permno as Matching_permno, b.LogSize as Matching_LogSize
  from Unmtached_AMEXNASD_Size as a, NYSE_Chrs_2D as b
  where a.qdate=b.qdate and abs(b.LogSize-a.LogSize)>0.05
  group by a.qdate, a.permno 
  having abs(b.LogSize-a.LogSize)=min(abs(b.LogSize-a.LogSize));
quit;
proc sort data=AMEXNASD_Chrs_2D_Size2 nodupkey; by qdate permno; run;


data AMEXNASD_Chrs_2D_Size;
  set AMEXNASD_Chrs_2D_Size1 AMEXNASD_Chrs_2D_Size2;
run; 
proc sort data=AMEXNASD_Chrs_2D_Size nodupkey; by qdate permno; run;


***Matching Raash.LogSize of NYSE firm for matched LogSizes of AMEX/NASD firms;
proc sql;
  create table AMEXNASD_Chrs_2D_Size as
  select distinct a.qdate, a.permno, a.EXCHCD, a.LogSize, b.Rank_LogSize
  from AMEXNASD_Chrs_2D_Size as a, NYSE_Chrs_2D as b
  where a.qdate=b.qdate and a.Matching_permno=b.permno;
quit;
proc sort data=AMEXNASD_Chrs_2D_Size nodupkey; by qdate permno; run;

proc sql;
  drop table AMEXNASD_Chrs_2D_Size1, AMEXNASD_Chrs_2D_Size2, Unmtached_AMEXNASD_Size;
quit;



* Step 5.5.2 Assign raash. to rLogBM_IndAdj for AMEX and NASD firms based on the closest match for NYSE firms;
***Extract matching NYSE firms with same rLogBM_IndAdj;
proc sql;
  create table AMEXNASD_Chrs_2D_LogBM_IndAdj1 as
  select distinct a.qdate, a.permno, a.EXCHCD, a.rLogBM_IndAdj, b.permno as Matching_permno, b.rLogBM_IndAdj as Matching_rLogBM_IndAdj
  from AMEXNASD_Chrs_2D as a, NYSE_Chrs_2D as b
  where a.qdate=b.qdate and abs(b.rLogBM_IndAdj-a.rLogBM_IndAdj)<=0.05
  group by a.qdate, a.permno 
  having abs(b.rLogBM_IndAdj-a.rLogBM_IndAdj)=min(abs(b.rLogBM_IndAdj-a.rLogBM_IndAdj));
quit;
proc sort data=AMEXNASD_Chrs_2D_LogBM_IndAdj1 nodupkey; by qdate permno; run;


proc sql;
  create table Unmtached_AMEXNASD_LogBM_IndAdj as
  select distinct a.* from AMEXNASD_Chrs_2D(drop=LogSize LogBM_IndAdj) as a
  except
  select distinct b.* from AMEXNASD_Chrs_2D_LogBM_IndAdj1(drop=Matching_permno Matching_rLogBM_IndAdj) as b;

  create table AMEXNASD_Chrs_2D_LogBM_IndAdj2 as
  select distinct a.qdate, a.permno, a.EXCHCD, a.rLogBM_IndAdj, b.permno as Matching_permno, b.rLogBM_IndAdj as Matching_rLogBM_IndAdj
  from Unmtached_AMEXNASD_LogBM_IndAdj as a, NYSE_Chrs_2D as b
  where a.qdate=b.qdate and abs(b.rLogBM_IndAdj-a.rLogBM_IndAdj)>0.05
  group by a.qdate, a.permno 
  having abs(b.rLogBM_IndAdj-a.rLogBM_IndAdj)=min(abs(b.rLogBM_IndAdj-a.rLogBM_IndAdj));
quit;
proc sort data=AMEXNASD_Chrs_2D_LogBM_IndAdj2 nodupkey; by qdate permno; run;


data AMEXNASD_Chrs_2D_LogBM_IndAdj;
  set AMEXNASD_Chrs_2D_LogBM_IndAdj1 AMEXNASD_Chrs_2D_LogBM_IndAdj2;
run; 
proc sort data=AMEXNASD_Chrs_2D_LogBM_IndAdj nodupkey; by qdate permno; run;


***Matching Raash.rLogBM_IndAdj of NYSE firm for matched rLogBM_IndAdjs of AMEX/NASD firms;
proc sql;
  create table AMEXNASD_Chrs_2D_LogBM_IndAdj as
  select distinct a.qdate, a.permno, a.EXCHCD, a.rLogBM_IndAdj, b.Rank_rLogBM_IndAdj
  from AMEXNASD_Chrs_2D_LogBM_IndAdj as a, NYSE_Chrs_2D as b
  where a.qdate=b.qdate and a.Matching_permno=b.permno;
quit;
proc sort data=AMEXNASD_Chrs_2D_LogBM_IndAdj nodupkey; by qdate permno; run;

proc sql;
  drop table AMEXNASD_Chrs_2D_LogBM_IndAdj1, AMEXNASD_Chrs_2D_LogBM_IndAdj2, Unmtached_AMEXNASD_LogBM_IndAdj;
quit;


* Step 5.6 Collect all data in single dataset for 2D vectors;
proc sql;
  create table AMEXNASD_Chrs_2D as
  select distinct a.*, b.Rank_LogSize, c.Rank_rLogBM_IndAdj
  from AMEXNASD_Chrs_2D as a, AMEXNASD_Chrs_2D_Size as b, AMEXNASD_Chrs_2D_LogBM_IndAdj as c
  where a.qdate=b.qdate=c.qdate and a.permno=b.permno=c.permno;
quit;
data CRSP_StkQtrs_Chrs_2D_BMInd;
  set NYSE_Chrs_2D AMEXNASD_Chrs_2D;
  label rLogBM_IndAdj='rLogBM_IndAdj';
run;
proc sort data=CRSP_StkQtrs_Chrs_2D_BMInd nodupkey; by qdate permno; run;
data ash.CRSP_StkQtrs_Chrs_2D_BMInd; set CRSP_StkQtrs_Chrs_2D_BMInd; run;

proc sql;
  drop table NYSE_Chrs_2D, AMEXNASD_Chrs_2D, AMEXNASD_Chrs_2D_Size, AMEXNASD_Chrs_2D_LogBM_IndAdj, Parms_NYSE_2D;
quit;

endrsubmit;
*135,718 fund-qtrs;
