proc datasets library=work kill;
run;

LIBNAME p1 'G:\'; run;

LIBNAME p2 'G:\SAS Temporary Files\_TD14192_HYD-30970_';run;

LIBNAME p3 'F:\'; run;




**********************************************************************************************
                                    Holdings Data;
*********************************************************************************************;





****Reading all CSV files in the folder;


/**********************************************************************************************************************/
/* Step 1 - Read all the file names and then read the contents using a macro  */
/**********************************************************************************************************************/
%macro DIRLISTWIN( PATH     /* Windows path of directory to examine */
                 , MAXDATE=  /* [optional] maximum date/time of file to report                   */
                 , MINDATE=  /* [optional] minimum date/time of file to report                   */
                 , MAXSIZE=  /* [optional] maximum size of file to report (bytes)                */
                 , MINSIZE=  /* [optional] minimum size of file to report (bytes)                */
                 , OUT=  taster    /* [optional] name of output file containing results of %DIRLISTWIN */
                 , REPORT=Y  /* [optional] flag controlling report creation                      */
                 , REPORT1=N /* [optional] flag controlling 1-line report creation               */
                 , SUBDIR=Y  /* [optional] include subdirectories in directory processing        */
                 ) ;

   /* PURPOSE: create listing of files in specified directory to make evident
    *
    * NOTE:    %DIRLISTWIN is designed to be run on SAS installations using the Windows O/S
    *
    * NOTE:    &PATH must contain valid Windows path, e.g., 'c:' or 'c:\documents and settings'
    *
    * NOTE:    &MAXDATE and &MINDATE must be SAS date/time constants in one of the following formats:
    *             'ddMONyy:HH:MM'dt *** datetime constant ***
    *             'ddMONyy'd        *** date     constant ***
    *             'HH:MM't          *** time     constant ***
    *
    * NOTE:    if &SUBDIR = Y then all subdirectories of &PATH will be searched
    *          otherwise, only the path named in &PATH will be searched
    *
    * NOTE:    uses Windows pipe option on file reference
    *
    * NOTE:    if %DIRLISTWIN is used successively in the same job, then
    *             the report will contain the cumulative directory listing of all directories searched
    *             a separate &OUT dataset will be created for each %DIRLISTWIN invocation
    *
    * USAGE:
    *  %DIRLISTWIN( c:/data1 )
    *  %DIRLISTWIN( c:/data1, MINDATE='01JAN04:00:00:00'dt, MAXDATE='16MAR04:23:59:59'dt )
    *  %DIRLISTWIN( c:/data1, MINDATE='00:00:00't, MAXDATE='23:59:59't, MINSIZE=1000000 )
    *  %DIRLISTWIN( d:/data2, REPORT=Y )
    *  %DIRLISTWIN( d:, OUT=LIBNAME.DSNAME, REPORT=N )
    *  %DIRLISTWIN( d:/documents and settings/robett/my documents/my sas files/v8 )
    *
    * ALGORITHM:
    *  use Windows pipe with file reference to execute 'dir' command to obtain directory contents
    *  parse pipe output as if it were a file to extract file names, other info
    *  [optional] select files that are within the time interval [ &MINDATE, &MAXDATE ]
    *  [optional] select files that are at least as large as &MINSIZE bytes and no larger than &MAXSIZE
    *  sort records by owner, path, filename
    *  [optional] create report of files per owner/path if requested
    *  [optional] create 1-line report of files per owner/path if requested
    */

   %let DELIM   = ' ' ;
   %let REPORT  = %eval( %upcase( &REPORT ) = Y ) ;
   %let REPORT1 = %eval( %upcase( &REPORT1 ) = Y ) ;

   %if %upcase( &SUBDIR ) = Y %then %let SUBDIR = /s ; %else %let SUBDIR = ;

   /*============================================================================*/
   /* external storage references
   /*============================================================================*/

   /* run Windows "dir" DOS command as pipe to get contents of data directory */

   filename DIRLIST pipe "dir /-c /q &SUBDIR /t:c ""&PATH""" ;

   /*############################################################################*/
   /* begin executable code
   /*############################################################################*/

   /* use Windows pipe to recursively find all files in &PATH
    * parse out extraneous data, including unreadable directory paths
    * process files >= &MINSIZE in size
    *
    * directory list structure:
    *    "Directory of" record precedes listing of contents of directory:
    *
    *    Directory of <volume:> \ <dir1> [ \ <dir2>\... ]
    *    mm/dd/yy hh:mm:ss [AM|PM] ['<DIR>' | size ] filename.type
    *
    *    example:
    *
    *       Volume in drive C is WXP
    *       Volume Serial Number is 18C2-3BAA
    *
    *       Directory of C:\Documents and Settings\robett\My Documents\My SAS Files\V8\Test
    *
    *       05/21/03  10:58 AM    <DIR>          CARYNT\robett          .
    *       05/21/03  10:58 AM    <DIR>          CARYNT\robett          ..
    *       12/24/03  10:22 AM    <DIR>          CARYNT\robett          Codebook
    *       04/23/01  02:42 PM               387 CARYNT\robett          printCharMat.sas
    *       10/09/03  11:35 AM             20582 CARYNT\robett          test.log
    *       10/28/03  08:02 AM             58682 CARYNT\robett          test.lst
    *       10/09/03  11:35 AM              1575 CARYNT\robett          test.sas
    */

   data dirlist ;
      length path filename $255 line $1024 owner $17 temp $16 ;
      retain path ;

      infile DIRLIST length=reclen ;
      input line $varying1024. reclen ;

      if reclen = 0 then delete ;

      if scan( line, 1, &DELIM ) = 'Volume'  | /* beginning of listing */
         scan( line, 1, &DELIM ) = 'Total'   | /* antepenultimate line */
         scan( line, 2, &DELIM ) = 'File(s)' | /* penultimate line     */
         scan( line, 2, &DELIM ) = 'Dir(s)'    /* ultimate    line     */
      then delete ;

      dir_rec = upcase( scan( line, 1, &DELIM )) = 'DIRECTORY' ;

      /* parse directory     record for directory path
       * parse non-directory record for filename, associated information
       */

      if dir_rec
      then
         path = left( substr( line, length( "Directory of" ) + 2 )) ;
      else do ;
         date = input( scan( line, 1, &DELIM ), mmddyy8. ) ;

         time = input( scan( line, 2, &DELIM ), time5. ) ;

         post_meridian = ( scan( line, 3, &DELIM ) = 'PM' ) ;

         if post_meridian then time = time + '12:00:00'T ; /* add 12 hours to represent on 24-hour clock */

         temp = scan( line, 4, &DELIM ) ;

         if temp = '<DIR>' then size = 0 ; else size = input( temp, best. ) ;

         owner = scan( line, 5, &DELIM ) ;

         /* scan delimiters cause filename parsing to require special treatment */

         filename = scan( line, 6, &DELIM ) ;

         if filename in ( '.' '..' ) then delete ;

         ndx = index( line, scan( filename, 1 )) ;

         filename = substr( line, ndx ) ;
      end ;

      /* date/time filter */

      %if %eval( %length( &MAXDATE ) + %length( &MINDATE ) > 0 )
      %then %do ;
         if not dir_rec
         then do ;
            datetime = input( put( date, date7. ) || ':' || put( time, time5. ), datetime13. )  ;

            %if %length( &MAXDATE ) > 0 %then %str( if datetime <= &MAXDATE ; ) ;
            %if %length( &MINDATE ) > 0 %then %str( if datetime >= &MINDATE ; ) ;
         end ;
      %end ;

      /* size filter */

      %if %length( &MAXSIZE ) > 0 %then %str( if size <= &MAXSIZE ; ) ;
      %if %length( &MINSIZE ) > 0 %then %str( if size >= &MINSIZE ; ) ;

      drop dir_rec line ndx post_meridian temp ;
   run ;

   proc sort data=dirlist out=dirlist ; by owner path filename ; run ;

   /*============================================================================*/
   /* create output dataset if requested
   /*============================================================================*/

   %if %length( &OUT ) > 0 %then %str( data &OUT ; set dirlist ; run ; ) ;

   /*============================================================================*/
   /* add data for current directory path to cumulative report dataset
   /*============================================================================*/

   proc append base=report data=dirlist ; run ;

   /*============================================================================*/
   /* break association to previous path prior to next %DIRLISTWIN invocation
   /*============================================================================*/

   filename DIRLIST clear ;

   /*============================================================================*/
   /* create report of files by owner, if requested
   /*============================================================================*/

  %if &REPORT
  %then %do ;
      title "Directory Listing" ;
      title1 "Path: &PATH" ;
      proc report center data=report headskip nowindows spacing=1 split='\' ;
         column owner path size date time filename ;

         define owner    / order   width=17        'Owner' ;
         define path     / order   width=32 flow   'Path' ;
         define size     / display format=comma19. 'Size/(bytes)' ;
         define date     / display format=mmddyy8. 'Date' ;
         define time     / display format=time5.   'Time' ;
         define filename / display width=32 flow   'File Name' ;
      run ;
      title ;

   %end ;

  %if &REPORT1
  %then %do ;
      /* create 1-line report: truncate path to fit landscape layout */

      data report1( keep= owner path1 size ) ;
         length path1 $287 ; /* 255 chars from above + 32 chars max filename */
         set report ;

         path1 = catx( '\', path, filename ) ;

         path1 = left( reverse( substr( left( reverse( path1 )), 1, 80 ))) ;
      run ;

      title "Directory Listing" ;
      title1 "Path: &PATH" ;
      proc report nocenter data=report1 headskip nowindows spacing=1 split='/' ;
         column owner path1 size ;

         define owner / order width=17 'Owner' ;
         define path1 / order width=80 'Path' ;
         define size  / display format=comma19. 'Size/(bytes)' ;
      run ;
      title ;

   %end ;
%mend DIRLISTWIN ;

%DIRLISTWIN(G:\Splitted datasets\python_files, OUT=filenames, REPORT=N, SUBDIR=Y);* Change the path

* Calling the macro. Enter the folder path here;

DATA filenames;
        SET filenames;
        IF size ~= 0;
        IF size ~= .;
RUN;

DATA filenames1;
        SET filenames;
       * filename=TRIM(path)||'\'||TRIM(filename);
/*        SMYD=SCAN(filename,-1,'#');*/
/*        SMYD1=SCAN(SMYD,1,'.');*/
/*        First=SCAN(SMYD1,1,'$');*/
/*        Last=SCAN(SMYD1,-1,'$') ;*/
/*        State=SCAN(First,1,'_') ;*/
/*        Month=SCAN(First,-1,'_') ;*/
/*        Year=SCAN(Last,1,'@') ;*/
/*        Dtofmn=SCAN(Last,-1,'@') ;*/

        filename="'"||TRIM(Path)||'\'||TRIM(filename)||"'";
        n=_N_;

       * KEEP filename party n;
RUN;



**FundID mapping;
PROC IMPORT OUT= mapping
            DATAFILE= "E:\Drive\Morningstar data\Global equity funds data\Raw Data Received from Girjinder\Portfolio\Report\report_20171219.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Report";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

proc sort data=mapping nodupkey out=mapping;by fundid; run;

data mapping;
	set mapping;
	keep fundid MasterPortfolioId;
run;

/*Fund domicile info*/
data WORK.DOMICILE    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'E:\Drive\Morningstar data\Global equity funds data\List of global funds pulled
 from MD\Data created by Adarsh\FundPortfolio_Global.csv' delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2
  ;
         informat FundId $10. ;
         informat Domicile $20. ;
         informat earliest_inception_date DATE8. ;
         informat earliest_inception_date_secid $10. ;
         informat last_obsolete_date DATE8. ;
         format FundId $10. ;
         format Domicile $20. ;
         format earliest_inception_date DATE8. ;
         format earliest_inception_date_secid $10. ;
         format last_obsolete_date DATE8. ;
      input
                  FundId $
                  Domicile $
                  earliest_inception_date
                  earliest_inception_date_secid $
                  last_obsolete_date
      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;



data domicile;
	set domicile;
	keep fundid domicile;
run;
proc sort data=domicile nodupkey; by fundid; run;

**Merge fundid with country;
proc sql;
	create table mapping as
	select distinct a.*,b.*
	from mapping as a left join domicile as b
	on a.fundid=b.fundid;
quit;
proc sort data=mapping nodupkey; by fundid; run;




/* Store the number of files in a macro variable "num" */
proc sql noprint;
        select count(*) into :num from filenames1;
quit;


**instead of printing log, it saves log file at mentioned location;
proc printto log="G:\Splitted datasets\filename.log";
run;

/* Create a macro to iterate over the filenames, read them in, and append to a data set.*/
options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;
%LET k=1;
%macro doit;
    %do i=1 %to &num;

        proc sql noprint;
            select filename into :filename from filenames1 where n=&i;
        quit;



/*PROC IMPORT OUT= test*/
/*          DATAFILE= "G:\Splitted datasets\python_files\f_0.csv" */
/*          DBMS=CSV REPLACE;*/
/*     GETNAMES=YES;*/
/*     DATAROW=2; */
/*RUN;*/


data WORK.TEST    ;
      %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile &filename delimiter = ',' MISSOVER DSD
 lrecl=32767 firstobs=2
  ;
         informat HoldingDetail__ExternalId $10. ;
         informat HoldingDetail_ExternalName $43. ;
         informat HoldingDetail__DetailHoldingType $5. ;
         informat HoldingDetail__Id $13. ;
         informat SecurityName $33. ;
         informat Weighting best32. ;
         informat NumberOfShare best32. ;
         informat MarketValue best32. ;
         informat ShareChange best32. ;
         informat ISIN $15. ;
         informat Currency $18. ;
         informat CUSIP $12. ;
         informat IndustryId best32. ;
         informat GlobalSector best32. ;
         informat Country $16. ;
         informat Date $13. ;
         informat MasterPortfolioId $9. ;
         informat i best32. ;
         format HoldingDetail__ExternalId $10. ;
         format HoldingDetail_ExternalName $43. ;
         format HoldingDetail__DetailHoldingType $5. ;
         format SecurityName $33. ;
         format Weighting best12. ;
         format NumberOfShare best12. ;
         format MarketValue best12. ;
         format ShareChange best12. ;
         format ISIN $15. ;
         format Currency $18. ;
         format CUSIP $12. ;
         format IndustryId best12. ;
         format GlobalSector best12. ;
         format Country $16. ;
         format Date $13. ;
         format MasterPortfolioId $9. ;
         format i best12. ;

      input
                  HoldingDetail__ExternalId $
                  HoldingDetail_ExternalName $
                  HoldingDetail__DetailHoldingType $
                  HoldingDetail__Id $
                  SecurityName $
                  Weighting
                  NumberOfShare
                  MarketValue
                  ShareChange
                  ISIN $
                  Currency $
                  CUSIP $
                  IndustryId
                  GlobalSector
                  Country $
                  Date $
                  MasterPortfolioId $

      ;
      if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
      run;


		data test;
			set test;
/*			HoldingDetail__ExternalId=input(scan(HoldingDetail__ExternalId,2,"'"),$20.);*/
/*			HoldingDetail_ExternalName=input(scan(HoldingDetail_ExternalName,2,"'"),$20.);*/
/*			HoldingDetail__DetailHoldingType=input(scan(HoldingDetail__DetailHoldingType,2,"'"),$20.);*/
/*			HoldingDetail__Id=input(scan(HoldingDetail__Id,2,"'"),$20.);*/
/*			SecurityName=input(scan(SecurityName,2,"'"),$20.);*/
/*			ISIN=input(scan(ISIN,2,"'"),$20.);*/
/*			Currency=input(scan(Currency,2,"'"),$20.);*/
/*			CUSIP=input(scan(CUSIP,2,"'"),$20.);*/
/*			Country=input(scan(Country,2,"'"),$20.);*/
			MasterPortfolioId=input(scan(MasterPortfolioId,2,"'"),best12.);
			Date1=input(scan(date,2,"'"),yymmdd10.);
			format date1 date9.;
			rename date1=Date;
			drop i Date;
/*			format HoldingDetail__ExternalId $10. HoldingDetail_ExternalName $43. HoldingDetail__DetailHoldingType $5.	*/
/*			HoldingDetail__Id $13. SecurityName	$33.	*/
/*			ISIN $15. Currency	$18. CUSIP $12.	Country Date MasterPortfolioId;*/
		run;

		      
		proc sql;
			create table test as
			select distinct a.*,b.Fundid,b.domicile as fund_domicile
			from test as a left join mapping as b
			on input(a.MasterPortfolioId,best12.)=b.MasterPortfolioId
			order by Fundid,date,weighting;
		quit;

		**Appending to the final dataset;
		proc append data=test force base=p2.final; run;



%end;
%mend doit;
%doit



proc sql;
	create table portf_check as
	select distinct fundId, min(Date) as Inception_date_new, max(Date) as Obsolete_date_new
	from p2.final
	group by fundId;
quit;

data portf_check;
	set portf_check;
	format Inception_date_new date9. Obsolete_date_new date9.;
run;

**Comparison with data we provided;
PROC IMPORT OUT= portf
            DATAFILE= "E:\Drive\Morningstar data\Global equity funds data\List of global funds pulled from MD\Data created by Adarsh\FundPortfolio_Global.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

proc sql;
	create table portf as
	select distinct a.*,b.*
	from portf as a left join portf_check as b
	on a.fundid=b.fundid;
quit; 

**Country-wise comparison;
proc sql;
	create table test1 as
	select Domicile, count(distinct fundid) as ncount
	from test
	group by Domicile;
quit;


*re-enabling the log;
PROC PRINTTO PRINT=PRINT LOG=LOG ;
RUN;



************************************************************************************************
         						PART-2
**********************************************************************************************;



/* Constructing a unique ISIN & CUSIP table from oldings data to form a QA Direct link table*/

proc sql;
    create table p1.unique_identifiers as
    select distinct fund_domicile, Country, cusip, isin, count(distinct Date) as ncount
    from p2.final
    group by fund_domicile, Country,cusip, isin;
quit;

/*Removing abnormal observations: where CUSIP & ISIN are for different companies */

data p1.unique_identifiers1;
    set p1.unique_identifiers;
    if missing(isin)=0 and missing(cusip)=0 and ncount=1 then delete;
run;

*Count number of funds by domicile;
/*proc sql;*/
/*	create table p1.unique_identifiers2 as*/
/*	select distinct fund_domicile, count(distinct FundId) as count*/
/*	from p2.final*/
/*	group by fund_domicile;*/
/*quit;*/


/*Deleting the rows with just one (deemed to be abnormal) observation;
	Deleting securities with NumberOfShare=0 => weighting=0 */

proc sql;
	create table p3.final1 as
	select distinct a.*
	from p2.final as a left join p1.unique_identifiers1 as b
	on a.fund_domicile=b.fund_domicile and a.cusip=b.cusip and a.isin=b.isin;
quit;


data p3.final1;
	set p3.final1;
	if NumberOfShare=0 then delete;
	if MarketValue < 0 then Weighting = -1 * Weighting; *If security is shorted, then change the weight from +ve to -ve;
run;
*235903520 OBS;

proc sort data=p3.final1; by FundId Date weighting; run;

/**Rescaling weights **/

proc sql;
	create table p3.final1 as
	select distinct *,Weighting/sum(Weighting) as rescale_weighting
	from p3.final1
	group by FundId, Date;
quit;
*235903520 OBS;

/*Check if weights are summing upto 100 */
proc sql;
  create table p3.SumWeights_Check as
  select distinct FundId, Date, sum(weighting) as TotalWeight
  from p3.final1
  group by FundId, Date;
quit;
* 2067669 OBS;

/*Remove funds if weighting is <90 & >110 */

data p3.SumWeights_Check1;
  set p3.SumWeights_Check;
  Diff = 100 - TotalWeight;
  if abs(Diff) > 10 then delete;
run;
*1986200 OBS;

*** Select funds within this range;
proc sql;
  create table p3.final2 as
  select distinct a.*
  from p3.final1 as a, p3.SumWeights_Check1 as b
  where a.FundId=b.FundId and a.date=b.date;
quit;



*** Percentage of assets invested by asset class;
proc sql;
  create table p3.PerAUM_AssetClass as
  select distinct FundId, Date, HoldingDetail__DetailHoldingType, sum(rescale_weighting) as PerAUM_By_Class
  from p3.final2
  group by FundId, Date, HoldingDetail__DetailHoldingType;
quit;

data p3.PerAUM_AssetClass;
  set p3.PerAUM_AssetClass;
  if HoldingDetail__DetailHoldingType = "b'E'";
run;

proc sql;
  create table p3.PerAUM_AssetClass as
  select distinct *
  from p3.PerAUM_AssetClass
  where PerAUM_By_Class > 0.75;
quit;
proc sort data=p3.PerAUM_AssetClass nodupkey; by FundId Date; run;


proc sort data=p3.final1 nodupkey; by FundId Date rescale_weighting isin cusip; run; 
*234921793 OBS,  981727 duplicates deleted;

*** Select funds with minimum of 75% assets in equities;
proc sql;
  create table p3.final2 as
  select distinct a.*
  from p3.final1 as a, p3.PerAUM_AssetClass as b
  where a.FundId=b.FundId and a.date=b.date;
quit;
*220357862 OBS;

*** Select equity holdings only;
proc sql;
  create table p3.final3 as
  select distinct FundId, fund_domicile, Date, Cusip, ISIN, Country, Currency, weighting, rescale_weighting, NumberOfShare, MarketValue, IndustryId, GlobalSector
  from p3.final2
  where HoldingDetail__DetailHoldingType = "b'E'";
quit;
* 207276387 OBS;

*** Re-scale weights again so that they sum to 100;
proc sql;
  create table p3.final3 as
  select distinct *, (rescale_weighting/sum(rescale_weighting)) * 100 as rescale_weighting1
  from p3.final3
  group by FundId, Date;
quit;

*207276387 OBS;

**USA dataset;
data p3.US;
	set p3.final3;
	if fund_domicile="UnitedStates";
run;
*91088682 OBS;

**Number of unique funds in the US;
proc sort data=p3.US nodupkey out=p3.unique_us; by fundid;run;



****************************************************************************************
								Some sample cleaning

	Calculating daily returns using Compustat and comparing with those of Bloomberg;
****************************************************************************************;



data isin;
	set p1.unique_identifiers1;
	isin1=scan(ISIN,2,"'");
	if missing(isin1)=1 then delete;
	keep isin1;
run;

data tmp1.abc;
	set tmp1.abc;
	if missing(ISIN)=1 then delete;
run;

*Daily returns estimation;

proc sort data=tmp1.abc ; by isin datadate ;run;

data tmp1.abc;
	set tmp1.abc;
	prc=(prccd/ajexdi)*trfd;
run;

data tmp1.abc;
	set tmp1.abc;
	by isin;
	if first.ISIN=1 then ret=.;
	else ret=log(prc/lag(prc));
run;

proc sort data=tmp1.abc nodupkey out=tmp1.abc2; by isin ;run;

data tmp1.abc2;
	set tmp1.abc2;
	keep isin;
run;

PROC IMPORT OUT= tmp1.bloomberg
            DATAFILE= "C:\Users\30970\Desktop\isin\new_isin.xlsx" 
            DBMS=EXCEL REPLACE;
sheet="Sheet1";
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;

data tmp1.bloomberg;
	set tmp1.bloomberg;
	__00=__00/100;
	rename __22_2018=date;
	rename __00=ret;
	rename AU000000WRM6=isin;
	keep date ret isin;
run; 

proc sql;
	create table tmp1.abc1 as
	select distinct a.*,b.ret as bb_ret
	from tmp1.abc as a, tmp1.bloomberg as b
	where a.isin=b.isin and a.DATADATE=b.date
	order by isin, date;
quit;
