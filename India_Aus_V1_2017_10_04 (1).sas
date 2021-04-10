libname Mstar 'F:\Google Drive\Morningstar data\Equity funds\Australia and India Data\Raw Data'; run;
libname Mstar1 'F:\Google Drive\Morningstar data\Equity funds\Australia and India Data\Raw Data Analysed'; run;
libname MD 'F:\Google Drive\Morningstar data\Equity funds\Australia and India Data\List of funds in SAS format'; run;



/*******************************************/
/*** List of India and Australia FundIds ***/
/*******************************************/

data FundPortFolio_Aus_Ind;
  set MD.FundPortFolio_Aus_Ind;
run;
*2843 funds;

proc sort data=FundPortFolio_Aus_Ind nodupkey; by FundId; run;
*2843 unique funds;


/******************************/
/*** Portfolio Mapping Data ***/
/******************************/

PROC IMPORT OUT= WORK.FundMapping 
            DATAFILE= "F:\Google Drive\Morningstar data\Equity funds\Australia and India Data\Raw Data\Fund Mapping Report.xlsx" 
            DBMS=XLSX REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

proc sort data=FundMapping; by MasterPortfolioId; run;
*4096 OBS;

data FundMapping;
  set FundMapping;
  if missing(MasterPortfolioId)=1 then delete;
  drop TotalPortfolios Exported Success;
run;
*3988 OBS;

proc sort data=FundMapping nodupkey out=FundMapping_Cleaned; by MasterPortfolioId FundId FundShareClassId; run;
*2735 OBS;

proc sql;
  create table FundMapping_MasterPortId_FundId as
  select distinct MasterPortfolioId, FundId
  from FundMapping_Cleaned;
quit;
*2735 OBS, so 1 FundId is mapped to 1 MasterPortfolioId;

*Unique MasterPortfolioIds in the mapping provided;
proc sql;
  create table Unique_MasterPortfolioIds as
  select distinct MasterPortfolioId
  from FundMapping_MasterPortId_FundId;
quit;
*2043 Unique_MasterPortfolioIds;



/*****************************************/
/*** Map FundIds to MasterPortfolioIds ***/
/*****************************************/

proc sql;
  create table FundPortFolio_Aus_Ind1 as
  select distinct a.*, b.MasterPortfolioId
  from FundPortFolio_Aus_Ind as a left join FundMapping_MasterPortId_FundId as b
  on a.FundId = b.FundId;
quit;
*2843 funds;

*Funds with non-missing mapping;
proc sql;
  create table FundPortFolio_Aus_Ind_Mapped as
  select distinct *
  from FundPortFolio_Aus_Ind1
  where missing(MasterPortfolioId)=0;
quit;
*2735 funds;




/*****************************/
/*** Parsed Portfolio Data ***/
/*****************************/

data Portfolio_Parsed;
  set Mstar.Portfolio_Raw;
run;
*9,808,776 OBS;

*Select distinct rows;
proc sql;
  create table Portfolio_Parsed as
  select distinct *
  from Portfolio_Parsed;
quit;
*9,808,708 OBS;

*Unique MasterPortfolioIds;
proc sql;
  create table Unique_Funds as
  select distinct MasterPortfolioId
  from Portfolio_Parsed;
quit;
*1,213 portfolios;





/*************************/
/*** Composite mapping ***/
/*************************/

proc sql;
  create table Fundids_Mapped as
  select distinct a.Fundid, b.MasterPortfolioId
  from FundPortFolio_Aus_Ind as a left join FundMapping_Cleaned as b
  on a.FundId=b.FundId;
quit;
*2843 funds;

proc sql;
  create table Fundids_Mapped as
  select distinct a.*, b.MasterPortfolioId as MasterPortfolioId_PortfolioData
  from Fundids_Mapped as a left join Unique_Funds as b
  on a.MasterPortfolioId=b.MasterPortfolioId;
quit;
*2843 funds;

proc sort data=Fundids_Mapped; by descending MasterPortfolioId_PortfolioData; run;

PROC EXPORT DATA= Fundids_Mapped
            OUTFILE= "F:\Google Drive\Morningstar data\Equity funds\Australia and India Data\Raw Data Analysed\Fundids_Mapped.xlsx" 
            DBMS=XLSX REPLACE;
RUN;




/*******************************/
/*** Portfolio Data Analysis ***/
/*******************************/

data Portfolio_Parsed;
  format MasterPortfolioId Date Country Cusip ISIN SecurityName Weighting NumberOfShare MarketValue ShareChange;
  set Portfolio_Parsed;
run;

*Identify funds that short securities;
proc sql;
  create table Funds_That_Short as
  select distinct *
  from Portfolio_Parsed
  where MarketValue < 0;
quit;
*160,105 OBS;

*Identify funds with wrong weights attached;
proc sql;
  create table Funds_Wrong_Security_Weights as
  select distinct *
  from Portfolio_Parsed
  where NumberOfShare = 0 and Weighting > 0;
quit;
*15,995 OBS;


*** Correct data for shorting and wrong weights;
data Portfolio_Parsed1;
  set Portfolio_Parsed;
  if MarketValue < 0 then Weighting = -1 * Weighting; *If security is shorted, then change the weight from +ve to -ve;
  if NumberOfShare = 0 and Weighting > 0 then delete; *Correct for wrong security weights;
run;
*9,793,309 OBS;


/********************************************/
/* Initial sample to discuss with Girjinder */
/********************************************/

/*
proc sort data=Portfolio_Parsed1; by MasterPortfolioId Date; run;

data Portfolio_Parsed1_Sample;
  set Portfolio_Parsed1;
  if MasterPortfolioId in (1000240,191421);
run;
PROC EXPORT DATA= Portfolio_Parsed1_Sample
            OUTFILE= "F:\Google Drive\Morningstar data\Equity funds\Australia and India Data\Raw Data Analysed\Portfolio_Parsed1_Sample.xlsx" 
            DBMS=XLSX REPLACE;
RUN;
*/


*** Remove "portfolio-date" combinations from "Portfolio_Parsed1" for which abs(100 - TotalWeight) > 10;
*Check weights: they should sum to 100;
proc sql;
  create table SumWeights as
  select distinct MasterPortfolioId, Date, sum(Weighting) as TotalWeight
  from Portfolio_Parsed1
  group by MasterPortfolioId, Date;
quit;
*78,799 OBS;

data SumWeights;
  set SumWeights;
  Diff = 100 - TotalWeight;
  if abs(Diff) > 10 then delete;
run;
*77,598 OBS;

proc sql;
  create table Portfolio_Parsed2 as
  select distinct a.*
  from Portfolio_Parsed1 as a, SumWeights as b
  where a.MasterPortfolioId=b.MasterPortfolioId and a.date=b.date;
quit;
*9,729,899 OBS;


*** Rescale the weights, so that they sum to 100;
proc sql;
  create table Portfolio_Parsed3 as
  select distinct *, Weighting/sum(Weighting) as rescale_weighting
  from Portfolio_Parsed2
  group by MasterPortfolioId, Date;
quit;
data Portfolio_Parsed3;
  set Portfolio_Parsed3;
  rescale_weighting = rescale_weighting * 100;
run;
*9,729,899 OBS;


proc sql;
  create table SumWeights_Check as
  select distinct MasterPortfolioId, Date, sum(rescale_weighting) as TotalWeight
  from Portfolio_Parsed3
  group by MasterPortfolioId, Date;
quit;
*TotalWeight = 100 for all OBS;



*** Percentage of assets invested by asset class;
proc sql;
  create table PerAUM_AssetClass as
  select distinct MasterPortfolioId, Date, HoldingDetail__DetailHoldingType, sum(rescale_weighting) as PerAUM_By_Class
  from Portfolio_Parsed3
  group by MasterPortfolioId, Date, HoldingDetail__DetailHoldingType;
quit;
data PerAUM_AssetClass;
  set PerAUM_AssetClass;
  if HoldingDetail__DetailHoldingType = 'E';
run;

proc sql;
  create table PerAUM_AssetClass as
  select distinct *
  from PerAUM_AssetClass
  where PerAUM_By_Class > 75;
quit;
proc sort data=PerAUM_AssetClass nodupkey; by MasterPortfolioId Date; run;
*0 Obs deleted: data are OKAY.


*** Select funds with minimum of 75% assets in equities;
proc sql;
  create table Portfolio_Parsed4 as
  select distinct a.*
  from Portfolio_Parsed3 as a, PerAUM_AssetClass as b
  where a.MasterPortfolioId=b.MasterPortfolioId and a.date=b.date;
quit;
*9,535,754 OBS;


*** Select equity holdings only;
proc sql;
  create table Portfolio_Parsed5 as
  select distinct MasterPortfolioId, Date, Cusip, ISIN, Country, Currency, weighting, rescale_weighting, NumberOfShare, MarketValue, IndustryId, GlobalSector
  from Portfolio_Parsed4
  where HoldingDetail__DetailHoldingType = 'E';
quit;
*9,038,355 OBS;

*** Re-scale weights again so that they sum to 100;
proc sql;
  create table Portfolio_Parsed5 as
  select distinct *, (rescale_weighting/sum(rescale_weighting)) * 100 as rescale_weighting1
  from Portfolio_Parsed5
  group by MasterPortfolioId, Date;
quit;
*9,038,355 OBS;


*** Copy to "Raw Data Analysed" folder;
data Mstar1.Portfolio_Parsed5;
  set Portfolio_Parsed5;
run;



/******************/
/*** Price Data ***/
/******************/

*Total return index/Adj NAV data;
data PriceData;
  set Mstar.TRI;
  keep secid date Unit_BAS Unit_USD; 
run;

*Change the date format from text to numeric;
data PriceData;
  set PriceData;
  date1 = input(date,yymmdd10.);
  format date1 date9.;
run;
data PriceData;
  format secid date1;
  set PriceData;
  drop date;
  rename date1 = date;
run;

proc sort data=PriceData; by secid date; run;

*Calculate daily returns;
data PriceData;
  set PriceData;
  by secid date;
  lagUnit_BAS = lag(Unit_BAS);  
  if first.secid=1 then lagUnit_BAS=.;
run;
data PriceReturnData_Daily;
  set PriceData;
  if missing(Unit_BAS)=0 and missing(lagUnit_BAS)=0 then dret = (Unit_BAS-lagUnit_BAS)/lagUnit_BAS;
  else dret = .;
run;

*Calculate monthly returns;
proc sql;
  create table ReturnData_Monthly as
  select distinct secid, year(date) as year, month(date) as month, count(distinct date) as ntradingdays, exp(sum(log(1+dret))) - 1 as mret label='Monthly Return'
  from PriceReturnData_Daily
  group by secid, year(date), month(date)
  having count(distinct date) >= 20; *Needs to be modified for the acutal trading days in a month in that country; 
quit;

data Mstar1.PriceReturnData_Daily; set PriceReturnData_Daily; run;
data Mstar1.ReturnData_Monthly; set ReturnData_Monthly; run;


***Compare return data with MD;
proc sort data=Mstar1.ReturnData_Monthly1 out=CheckRet; by dummy; run;
*233 out of 10809 do not match, 233/10809 = 2.15% do not match;

*Export as XLSX;
PROC EXPORT DATA= Mstar1.ReturnData_Monthly1
            OUTFILE= "F:\Google Drive\Morningstar data\Equity funds\Australia and India Data\Raw Data Analysed\ReturnData_Monthly1.xlsx" 
            DBMS=XLSX REPLACE;
RUN;








