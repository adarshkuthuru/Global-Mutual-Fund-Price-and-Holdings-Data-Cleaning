proc datasets lib=work kill nolist memtype=data;
quit;

libname MF 'E:\Drive\Morningstar data\Equity funds\Australia and India Data\Raw Data Analysed'; run;
libname MF1 "E:\Drive\Morningstar data\Equity funds\Australia and India Data\Adarsh's SAS codes"; run;



**Estimated monthly returns data;
data Returndata_monthly;
	set mf.Returndata_monthly;
	yr=mod(year,100);
run;

***Imported data from Morningstar;
PROC IMPORT OUT= Net_exp_ratio
            DATAFILE= "E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\Expense ratio.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Net_Expense_Ratio";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= Gross_exp_ratio
            DATAFILE= "E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\Expense ratio.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Gross_Expense_Ratio";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;
**Just 2 values present;

PROC IMPORT OUT=Net_ret
            DATAFILE= "E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\Expense ratio.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Net_returns";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= Gross_ret
            DATAFILE= "E:\Drive\Local Disk F\Betting against beta\Time variation in BAB\Expense ratio.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Gross_returns";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

*converting Fund_Returns from wide to long form;
data Net_exp_ratio;
	set Net_exp_ratio;
	if missing(secid)=1 then delete;
run; 

proc transpose data=Net_exp_ratio out=Net_exp_ratio1;
  by SecId notsorted;
  var Y1998-Y2017;
run;

data Net_exp_ratio1;
	set Net_exp_ratio1;
	year=substr(_NAME_,4,2);
	rename col1=exp_ratio;
	exp_ratio_mon=col1/12;
run; 


data Gross_exp_ratio;
	set Gross_exp_ratio;
	if missing(secid)=1 then delete;
run; 

proc transpose data=Gross_exp_ratio out=Gross_exp_ratio1;
  by SecId notsorted;
  var Y1998-Y2017;
run;

data Gross_exp_ratio1;
	set Gross_exp_ratio1;
	year=substr(_NAME_,4,2);
	rename col1=exp_ratio;
	exp_ratio_mon=col1/12;
run;


data Net_ret;
	set Net_ret;
	if missing(secid)=1 then delete;
run; 

proc transpose data=Net_ret out=Net_ret1;
  by SecId notsorted;
  var Jan_98--Dec_17;
run;

data Net_ret1;
	set Net_ret1;
	Month=substr(_NAME_,1,3);
	year=substr(_name_,5,2);
	mon=month(input(cats('01',month,year),date9.));
	rename col1=net_ret;
run;




data Gross_ret;
	set Gross_ret;
	if missing(secid)=1 then delete;
run; 

proc transpose data=Gross_ret out=Gross_ret1;
  by SecId notsorted;
  var Jan_98--Dec_17;
run;

data Gross_ret1;
	set Gross_ret1;
	Month=substr(_NAME_,1,3);
	year=substr(_name_,5,2);
	mon=month(input(cats('01',month,year),date9.));
	rename col1=Gross_ret;
run;


***********************************************;
**Prof Nitin's data;

PROC IMPORT OUT= Fund_Returns
            DATAFILE= "E:\Drive\Morningstar data\Equity funds\Australia and India Data\Adarsh's SAS codes\Morningstar returns data.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= Month
            DATAFILE= "E:\Drive\Local Disk F\Prof Vikram\test.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

*converting Fund_Returns from wide to long form;

proc transpose data=Fund_Returns out=Fund_Returns1;
  by Group_Investment SecId FundId notsorted;
  var _all_;
run;

data Fund_Returns1;
	set Fund_Returns1;
	Ret=input(col1,12.);
	Mon=substr(_label_,1,3);
	Year=substr(_label_,5,2);
/*	Date=input(_LABEL_,MMDDYY10.);*/
/*	format Date date9. ;*/
	if _name_ in ('Group_Investment','SecId','FundId','Base_Currency','Global_Broad_Category_Group',
	'Domicile','Inception_Date') then delete;
/*	drop _name_ _label_ ;*/
run;

*Many characters dont get converted, so had to assign col1 column;
data Fund_Returns1;
	set Fund_Returns1;
	if missing(ret)=1 then ret=col1;
run;

data Fund_Returns1;
	set Fund_Returns1;
	if missing(ret)=1 then delete;
	drop col1 col2;
run;

data Fund_Returns1;
	set Fund_Returns1;
	yr=input(year,best12.);
run;

proc sql;
	create table Fund_Returns1 as
	select distinct a.*,b.month2 as month
	from Fund_Returns1 as a left join Month as b
	on a.mon=b.month1;
quit;




**************************************************************;
**Merging Morningstar Direct data with estimated returns;

proc sql;
	create table Returndata_msd as
	select distinct a.*,b.net_ret
	from Gross_ret1 as a left join Net_ret1 as b
	on a._name_=b._name_ and a.secid=b.secid;
quit;

proc sql;
	create table Returndata_msd as
	select distinct a.*,b.exp_ratio_mon
	from Returndata_msd as a left join Net_exp_ratio1 as b
	on a.year=b.year and a.secid=b.secid
	order by a.secid, a.year;
quit;

proc sql;
	create table Returndata_msd as
	select distinct a.*,b.mret as ret_estimated
	from Returndata_msd as a left join Returndata_monthly as b
	on a.mon=b.month and input(a.year,best12.)=b.yr and a.secid=b.secid;
quit;


/*data Returndata_msd1;*/
/*	set Returndata_msd;*/
/*	if missing(ret_estimated)=0;*/
/*	net_ret=net_ret/100;*/
/*	drop _NAME_ _label_ Month;*/
/*run;*/
/**/
/*data Returndata_msd1;*/
/*	set Returndata_msd1;*/
/*	if missing(net_ret)=0 and sum(net_ret,-ret_estimated)>=0.0005 then dummy=0;*/
/*	else dummy=1;*/
/*run;*/
/**/
/*data Returndata_msd2;*/
/*	set Returndata_msd1;*/
/*	if dummy=0;*/
/*run;*/


data Returndata_msd1;
	set Returndata_msd;
	if missing(net_ret)=0 and missing(ret_estimated)=0;
	net_ret=net_ret/100;
	drop _NAME_ _label_ Month;
run;

data Returndata_msd1;
	set Returndata_msd1;
	if missing(net_ret)=0 and sum(net_ret,-ret_estimated)>=0.0005 then dummy=0;
	else dummy=1;
run;

data Returndata_msd2;
	set Returndata_msd1;
	if dummy=0;
run;

*1511 obs out of 102822 have a difference greater than 5 basis points;

data Returndata_msd1;
	set Returndata_msd;
	if missing(gross_ret)=0 and missing(ret_estimated)=0;
	gross_ret=gross_ret/100;
	drop _NAME_ _label_ Month;
run;

data Returndata_msd1;
	set Returndata_msd1;
	if missing(gross_ret)=0 and sum(gross_ret,-ret_estimated)>=0.0005 then dummy=0;
	else dummy=1;
run;

data Returndata_msd2;
	set Returndata_msd1;
	if dummy=0;
run;

*21062 obs out of 21538 have a difference greater than 5 basis points;
