
libname QAD odbc dsn=qad user=mohan pw=Work@1234567 schema=dbo; 

proc datasets lib=work kill nolist memtype=data;
quit;

LIBNAME p1 'G:\'; run;
**creating unique ISIN and CUSIP from holdings dataset;

data unique_identifiers1;
	set p1.unique_identifiers1;
	drop ncount;
	isin=upcase(isin);
	Country=scan(Country,2,"'");
	isin=scan(isin,2,"'");
	cusip=scan(cusip,2,"'");
run;


data US Global;
	set unique_identifiers1;
/*	drop ncount;*/
	if substr(isin,1,2)= 'US' then output US;
	else output Global;
	if missing(isin)=1 and missing(cusip)=1 then delete;
run;

/*data US;*/
/*	set US;*/
/*	Country=scan(Country,2,"'");*/
/*	isin=scan(isin,2,"'");*/
/*	cusip=scan(cusip,2,"'");*/
/*	if missing(isin)=1 and missing(cusip)=1 then delete;*/
/*run;*/
/**/
/*data Global;*/
/*	set Global;*/
/*	Country=scan(Country,2,"'");*/
/*	isin=scan(isin,2,"'");*/
/*	cusip=scan(cusip,2,"'");*/
/*	if missing(isin)=1 and missing(cusip)=1 then delete;*/
/*run;*/

proc sort data=US nodupkey; by country cusip isin; run;

proc sort data=US; by isin; run;

data US;
	set US;
	nrow=_N_;
run;


**Getting equity list from Gsecmstrx and Secmstrx;
/*proc sql;*/
/*	create table work.new1 as */
/*	select * */
/*	from qad.Gsecmstrx*/
/*	where Type_ in (1);*/
/*quit; */
/*/**/*/
/*proc sql;*/
/*	create table work.US as */
/*	select * */
/*	from qad.secmstrx*/
/*	where Type_ in (1);*/
/*quit; */
/**/
/*data equity;*/
/*	set qad.vw_securitymasterx;*/
/*	if typ=1;*/
/*run;*/
/**/
/* proc append data=us base=new1; *run;
/**/
/*proc sort data=new1 nodupkey; by id; run;
/**/
/*data Equity;*/
/*	set new1;*/
/*	if missing(Cusip)=1 and missing(isin)=1 then delete;*/
/*run;*/
/**/
/*proc sort data=equity nodupkey; by Id; run;*/
/**/
/*data Equity;*/
/*	set Equity;*/
/*	nrow=_N_;*/
/*run;*/
/**/
/**/
/*data Equity1;*/
/*	set Equity (obs=1000100 firstobs=1000000);*/
/*	if Type_=10;*/
/*	nrow=_N_;*/
/*run;*/


***Getting codes for all QAD databases;

proc printto log="E:\Drive\Local Disk F\Betting against beta\BAB FP paper\filename.log";
run;

proc sql noprint;
        select count(*) into :num from US;
quit;

options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=1 %to 1; *&num;

    proc sql noprint;
       select catt("'",isin,"'") into :isin from US where nrow=&i; 
    quit;
	

	%if &isin ne '' %then %do;

		proc sql;
			create table Equity2 as
			select distinct A1.*,A.ID, A.Cusip as CUSIP_QAD, A.Isin as ISIN_QAD, A.seccode /*,C.Infocode , D.Code as WS_Code 
/*, E.Code as IBES_Code */
			from US A1

			join qad.SecMstrX A
			on A1.Isin=A.isin and A1.Isin=&isin;
			join qad.SecMapX B
			on B.SecCode=A.SecCode
			and B.VenType=33 /*Datastream */
			and B.Rank=1;
			quit;
			%end;

/*			join qad.Ds2CtryQtInfo C*/
/*			on C.InfoCode=B.VenCode*/
/*			join qad.SecMapX B2*/
/*			on B2.SecCode=A.SecCode*/
/*			and B2.VenType=10  /*Worldscope */*/
/*			and B.Rank=1;*/

/*			join qad.WsInfo D*/
/*			on D.Code=B2.VenCode;*/
/*			join qad.SecMapX B3*/
/*			on B3.SecCode=A.SecCode*/
/*			and B3.VenType=2*/
/*			and B.Rank=1*/
/*			join qad.IBESInfo3 E*/
/*			on E.Code=B3.VenCode;



		/*	where A.Cusip='Y05473122';*/
 

**Appending to the final dataset;
/*proc append data=equity2 base=final; run;*/


%end;
%mend doit1;
%doit1




***************************************************************************;
                               *Testing;
***************************************************************************;

options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;

%macro doit1;
%do i=1 %to 1; *&num;

    proc sql noprint;
       select catt("'",cusip,"'") into :isin from US where nrow=&i; 
    quit;
	

	%if &isin ne '' %then %do;

		proc sql;
			create table Equity2 as
			select distinct A1.*,A.ID, A.Cusip as CUSIP_QAD, A.Isin as ISIN_QAD /*, B2.*,C.Infocode  , D.Code as WS_Code 
/*, E.Code as IBES_Code */
			from US A1

			join qad.VW_SECURITYMASTERX A
			on A1.cusip=A.cusip and A1.cusip=&isin;
/*			join qad.VW_SECURITYMAPPINGX  B*/
/*			on A.SecCode=B.SecCode*/
/*			and B.VenType=33 /*Datastream */*/
/*			and B.Rank=1;*/
			
/*			join qad.Ds2CtryQtInfo C*/
/*			on C.InfoCode=B.VenCode*/
/*			join qad.SecMapX B2*/
/*			on B2.SecCode=A.SecCode;*/
/*			and B2.VenType=10;  /*Worldscope */
/*			and B.Rank=1; */
			quit;
			%end;

/*			join qad.WsInfo D*/
/*			on D.Code=B2.VenCode;*/
/*			join qad.SecMapX B3*/
/*			on B3.SecCode=A.SecCode*/
/*			and B3.VenType=2*/
/*			and B.Rank=1*/
/*			join qad.IBESInfo3 E*/
/*			on E.Code=B3.VenCode;



 

**Appending to the final dataset;
/*proc append data=equity2 base=final; run;*/


%end;
%mend doit1;
%doit1
