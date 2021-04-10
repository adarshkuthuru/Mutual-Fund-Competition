libname FCSep29 'E:\Drive\Local Disk F\Mutual fund competition'; run;
libname mfglobal 'E:\Drive\Local Disk F\Mutual fund competition\Global'; run;
libname MF 'G:\MF competition'; run;



/* Step 0. Performance measures */
data Perf_Measures; 
  set mf.Perf_Measures; 
run;
*387,143 OBS;



/********************************************************/
/*** Short Term CS Alpha prediction: Future 3 Months  ***/
/********************************************************/

/* Step1. Sample for Short Term Alpha Prediction Over 1 Quarter */
data ShortTermPerf1;
  format wficn past1yrdate qdate nqdate date NPeer Rfund_RPeer_bexp Rfund_RPeer_afxp CS;
  set mf.Perf_Measures;
  if qtr(date)=1 then nqdate=mdy(3,31,year(date));
  if qtr(date)=2 then nqdate=mdy(6,30,year(date));
  if qtr(date)=3 then nqdate=mdy(9,30,year(date));
  if qtr(date)=4 then nqdate=mdy(12,31,year(date));
  qdate=intnx("month",nqdate,-3,"E");
  past1yrdate=intnx("month",qdate,-12,"E"); *t-12 month end date at the start of qdate;
  format qdate date9. nqdate date9. past1yrdate date9.; 
  keep wficn past1yrdate qdate nqdate date NPeer Rfund_RPeer_bexp Rfund_RPeer_afxp CS;
run;
proc sort data=ShortTermPerf1; by wficn past1yrdate qdate nqdate date NPeer; run;
proc transpose data=ShortTermPerf1 out=ShortTermPerf1;
  by wficn past1yrdate qdate nqdate date NPeer;
  var Rfund_RPeer_bexp Rfund_RPeer_afxp CS;
run;
data ShortTermPerf1;
  format wficn past1yrdate qdate nqdate date NPeer AlphaType col1;
  set ShortTermPerf1;
  rename col1=Alpha;
  label col1='Alpha for Month=Date';
  AlphaType=_Name_;
  drop _Name_ _label_;
run;
proc sort data=ShortTermPerf1 nodupkey; by wficn date AlphaType; run;



/* Step2. Average past 1 year performance at the start of each quarter */
proc sql;
  create table AvgPastPerf1 as
  select distinct a.wficn, a.qdate, a.nqdate, a.AlphaType, mean(b.Alpha) as AvgPastAlpha, mean(b.NPeer) as AvgNPeer
  from (select distinct wficn, past1yrdate, qdate, nqdate, AlphaType from ShortTermPerf1 where AlphaType='Rfund_RPeer_bexp') as a, 
       (select distinct wficn, date, AlphaType, Alpha, NPeer from ShortTermPerf1 where AlphaType='Rfund_RPeer_bexp') as b
  where a.wficn=b.wficn and a.AlphaType=b.AlphaType and a.past1yrdate<b.date<=a.qdate and missing(b.Alpha)=0
  group by a.wficn, a.qdate, a.AlphaType
  having count(distinct b.date)=12
  order by a.wficn, a.qdate, a.AlphaType;
quit;
data AvgPastPerf1;
  set AvgPastPerf1;
  if missing(AvgPastAlpha)=1 then delete;
  if missing(AvgNPeer)=1 then delete;
run;


*First sort by NPeer;
proc sort data=AvgPastPerf1; by qdate AlphaType; run;
proc rank data=AvgPastPerf1 groups=3 out=AvgPastPerf1;
  by qdate AlphaType;
  var AvgNPeer;
  ranks Rank_AvgNPeer;
run;
data AvgPastPerf1;
  set AvgPastPerf1;
  Rank_AvgNPeer=Rank_AvgNPeer+1;
run;

*Second conditional sort by past CPA;
proc sort data=AvgPastPerf1; by qdate AlphaType Rank_AvgNPeer; run;
proc rank data=AvgPastPerf1 groups=10 out=AvgPastPerf1;
  by qdate AlphaType Rank_AvgNPeer;
  var AvgPastAlpha;
  ranks Rank_AvgPastAlpha;
run;
data AvgPastPerf1;
  set AvgPastPerf1;
  Rank_AvgPastAlpha=Rank_AvgPastAlpha+1;
run;


/* Step3. Include future monthly return in AvgPastPerf1 */
proc sql;
  create table ShortTermPerf1a as
  select distinct a.*, b.date, 'CS' as FutAlphaType, b.CS as Alpha                                  
  from AvgPastPerf1 as a, Perf_Measures as b
  where a.wficn=b.wficn and a.qdate<b.date<=a.nqdate;
quit;
proc sort data=ShortTermPerf1a; by date Rank_AvgNPeer AlphaType Rank_AvgPastAlpha; run;


/* Step4. Form EW portfolios */
proc sql;
  create table ShortTermPerf1a_Port as
  select distinct qdate, date, Rank_AvgNPeer, AlphaType, Rank_AvgPastAlpha, mean(Alpha) as EW_Alpha, mean(AvgPastAlpha) as EW_PastAlpha
  from ShortTermPerf1a
  where missing(Alpha)=0
  group by date, Rank_AvgNPeer, AlphaType, Rank_AvgPastAlpha
  order by date, Rank_AvgNPeer, AlphaType, Rank_AvgPastAlpha;

  create table hedgeport as
  select distinct a.qdate, a.date, a.Rank_AvgNPeer, a.AlphaType, 11 as Rank_AvgPastAlpha, a.EW_Alpha-b.EW_Alpha as EW_Alpha, a.EW_PastAlpha-b.EW_PastAlpha as EW_PastAlpha
  from ShortTermPerf1a_Port(where=(Rank_AvgPastAlpha=10)) as a, ShortTermPerf1a_Port(where=(Rank_AvgPastAlpha=1)) as b
  where a.qdate=b.qdate and a.date=b.date and a.Rank_AvgNPeer=b.Rank_AvgNPeer and a.AlphaType=b.AlphaType;
quit;
data ShortTermPerf1a_Port;
  set ShortTermPerf1a_Port hedgeport; *Jul1981 to Jun2012;
run;
proc sql;
  create table hedgeport1 as
  select distinct a.qdate, a.date, 4 as Rank_AvgNPeer, a.AlphaType, a.Rank_AvgPastAlpha, a.EW_Alpha-b.EW_Alpha as EW_Alpha, a.EW_PastAlpha-b.EW_PastAlpha as EW_PastAlpha
  from ShortTermPerf1a_Port(where=(Rank_AvgNPeer=1)) as a, ShortTermPerf1a_Port(where=(Rank_AvgNPeer=3)) as b
  where a.qdate=b.qdate and a.date=b.date and a.AlphaType=b.AlphaType and a.Rank_AvgPastAlpha=b.Rank_AvgPastAlpha;
quit;
data ShortTermPerf1a_Port;
  set ShortTermPerf1a_Port hedgeport1; 
run;
proc sort data=ShortTermPerf1a_Port; by date Rank_AvgNPeer AlphaType Rank_AvgPastAlpha; run;



/* Step5. Persistency Check */
proc sort data=ShortTermPerf1a_Port; by Rank_AvgNPeer AlphaType Rank_AvgPastAlpha; run;

%let lags=4;
ods output parameterestimates=MEAN_PerfPers;
ods listing close;
proc model data=ShortTermPerf1a_Port;
  by Rank_AvgNPeer AlphaType Rank_AvgPastAlpha;  
  endo EW_Alpha;
  instruments / intonly;
  parms b0;
  EW_Alpha=b0;
  fit EW_Alpha / gmm kernel=(bart,%eval(&lags+1),0) vardef=n;
quit;
ods listing;


data AlphaPred1qtr;
  format duration;
  set MEAN_PerfPers;
  duration=3;
  estimate=estimate*100*12; *Annualize returns;
  format estimate 6.3 probt 6.3;
  rename estimate=Alpha;
  label estimate='Alpha (CS)';
  rename probt=PVALUE;
  rename Rank_AvgPastAlpha=Decile;
  keep duration Rank_AvgNPeer Rank_AvgPastAlpha estimate probt;
run;
proc sql;
  drop table AvgPastPerf1, hedgeport, hedgeport1, MEAN_PerfPers, 
             ShortTermPerf1, ShortTermPerf1a, ShortTermPerf1a_Port;
quit;
