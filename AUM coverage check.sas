proc datasets lib=work kill nolist memtype=data;
quit;



libname Dec 'C:\Users\30970\Downloads\AUM feed'; run;
libname Dec1 'E:\Drive\Morningstar data\Global equity funds data\List of global funds pulled from MD\Data created by Nitin';run;

***Import xlsx;
PROC IMPORT OUT= AUM1
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 1.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM2
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 2.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM3
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 3.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM4
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 4.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM5
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 5.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM6
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 6.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM7
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 7.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM8
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 8.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

PROC IMPORT OUT= AUM9
            DATAFILE= "C:\Users\30970\Downloads\AUM feed\AUM Monthly Dump Batch 9.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

**Appending to the final dataset;
proc append data=AUM1 base=final; run;
proc append data=AUM2 base=final; run;
proc append data=AUM3 base=final; run;
proc append data=AUM4 base=final; run;
proc append data=AUM5 base=final; run;
proc append data=AUM6 base=final; run;
proc append data=AUM7 base=final; run;
proc append data=AUM8 base=final; run;
proc append data=AUM9 base=final; run;

data dec.final;
	set final;
	if missing(secid)=1 then delete;
run;

proc transpose data=final out=final1;
  by Group_Investment SecId notsorted;
  var _all_;
run;

data final1;
	set final1;
	if _name_ in ('Group_Investment','SecId') then delete;
	date=substr(_label_,2,8);
run;


**Converting mm/yyyy to date9 format;
data final1;
	set final1;
	format date1 date9.;
  	date1=input(date,anydtdte7.);
run;

data final1;
	set final1;
	if missing(AUM)=1 then AUM=trim(col1);
/*	format AUM best32.;*/
run;

data dec.AUM;
	set final1;
	drop _name_ _label_ col1 date;
	rename date1=date;
run;

data final1;
	set final1;
	if missing(AUM)=1 then delete;
run;


proc sql;
	create table dec.final1 as
	select distinct Secid, min(date1) as data_inception_date, max(date1) as data_final_date
	from final1
	group by secid;
quit;

**Merging final1 with data provided to MStar guys;

proc sql;
	create table final2 as
	select distinct a.*,b.*
	from dec.final1 as a left join dec1.dailynav as b
	on a.secid=b.secid;
quit;

data dec.final2;
	format fundid secid domicile Inception_Date Obsolete_Date data_inception_date data_final_date;
	set final2;
	format data_inception_date date9. data_final_date date9.;
run;

proc sort data=dec.final2; by Inception_Date; run;

data dec.final2;
	set dec.final2;
	months=intck('month',Inception_Date,data_inception_date);
run;

data test;
	set dec.final2;
	if months<-12;
run;
