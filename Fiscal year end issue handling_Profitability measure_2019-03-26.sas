/**********************/
/**********************/
/*** Step 2. Value  ***/
/**********************/
/**********************/


***Compustat Annual Data;
data ValueData3;
  set Jun0815.ValueData2_V1;

  **Earnings;
  if missing(IB)=0 and missing(DVP)=0 and missing(TXDI)=0 then E=IB-DVP+TXDI;
  else if missing(IB)=0 and missing(DVP)=0 and missing(TXDI)=1 then E=IB-DVP;
 
  **EBITDA;
  EBITDA = OIBDP + NOPI;

  **Free Cash Flow;
  FCF = NI + DP - WCAPCH - CAPX;

  **Book Equity;
   if missing(SEQ)=0 then she=SEQ; 
   *SEQ=Stockholders Equity (Total);
   else if missing(CEQ)=0 and missing(PSTK)=0 then she=CEQ+PSTK; 
        *CEQ=Common/Ordinary Equity (Total), PSTK=Preferred/Preference Stock (Capital) (Total);
        else if missing(AT)=0 and missing(LT)=0 then she=AT-LT;
		     *AT=Assets (Total), LT=Liabilities (Total), MIB=Minority Interest (Balance Sheet); 
             else she=.;

   if missing(PSTKRV)=0 then BE=she-PSTKRV; *PSTKRV=Preferred Stock (Redemption Value);
   else if missing(PSTKL)=0 then BE=she-PSTKL; *PSTKL=Preferred Stock (Liquidating Value);
        else if missing(PSTK)=0 then BE=she-PSTK; *PSTK=Preferred/Preference Stock (Capital) (Total);
             else BE=.;

  **Profitability;
  if missing(GP)=0 and missing(AT)=0 and AT^=0 then Prof = GP/AT; else Prof=.; 

  if missing(TXDITC)=0 then BE = BE + TXDITC;
  keep lpermno datadate E EBITDA FCF BE DLC DLTT PSTKRV CHE Prof;
run;


***Monthly Data;
data CRSPData2;
  format permno date pastdate;
  set Jun0815.CRSPData1;
  pastdate=intnx("month",date,-6,"E");  *Use at least 6 month or earlier fundamental data from Compustat;  
  format pastdate date9.;
run;
proc sql;
  create table Value as
  select distinct a.permno, a.date, a.pastdate, b.datadate, b.E, b.EBITDA, b.FCF, b.BE label='BE(datadate)', a.ME label='ME(date)', b.DLC, b.DLTT, b.PSTKRV, b.CHE, b.Prof
  from CRSPData2 as a left join ValueData3 as b
  on a.permno=b.lpermno and b.datadate<=a.pastdate
  group by a.date, a.permno
  having a.pastdate-b.datadate=min(a.pastdate-b.datadate);
quit;
data Value;
  set Value;
  if intck("month",datadate,date)>18 then delete; *Avoid stale data, keep only fundamental data from the past 18 months;
  TEV = ME + DLC + DLTT + PSTKRV - CHE;
  drop DLC DLTT PSTKRV CHE;
  if year(date)>=1971;
run;


***Valuation Ratios;
data Value;
  set Value;
  V1 = BE/ME;
  *if BM<0 then BM=0;
  V2 = E/ME;
  *if EM<0 then EM=0;
  if TEV^=0 then V3 = EBITDA/TEV;
  *if EBITDATEV<0 then EBITDATEV=0;
  if missing(V1)=1 or missing(V2)=1 or missing(V3)=1 then delete;
  keep permno date V1 V2 V3 Prof;
run;
*2,252,898 OBS;

proc sql;
  drop table CRSPData2, ValueData3;
quit;
