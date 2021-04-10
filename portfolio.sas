libname final 'E:\Drive\Local Disk F\Morningstar\Data received from Girjinder\Final datasets'; run;

**instead of printing log, it saves log file at mentioned location;
proc printto log="E:\Local Disk F\Morningstar\Data received from Girjinder\Final datasets\filename.log";
run;


PROC IMPORT OUT= final.FundPortfolio_Aus_Ind
            DATAFILE= "E:\Local Disk F\Morningstar\Data for Shobhit 2017-05-17\FundPortfolio_Aus_Ind.csv" 
            DBMS=CSV REPLACE;
GETNAMES=YES;
/*MIXED=NO;*/
/*SCANTEXT=YES;*/
/*USEDATE=YES;*/
/*SCANTIME=YES;*/
RUN;



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

%DIRLISTWIN(E:\Local Disk F\Morningstar\Data received from Girjinder\portfolio\PortfolioSet, OUT=filenames, REPORT=N, SUBDIR=Y);* Change the path
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

        filename=TRIM(Path)||'\'||TRIM(filename);
        n=_N_;

       * KEEP filename party n;
RUN;

/* Store the number of files in a macro variable "num" */
proc sql noprint;
        select count(*) into :num from filenames1;
quit;


data filenames;
	set filenames;
	n=_N_;
run;


filename  SXLEMAP 'E:\Local Disk F\Morningstar\Data received from Girjinder\Final datasets\Adarsh.map';


options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;
%LET k=1;
%macro doit;
    %do i=1 %to &num;


        proc sql noprint;
            select filename into :filename from filenames1 where n=&i;
        quit;


		filename  SXLELIB "&filename";
	
		libname   SXLELIB xmlv2 xmlmap=SXLEMAP access=READONLY;

		DATA HoldingDetail; 
			SET SXLELIB.HoldingDetail;
			portfolioID=scan(scan("&filename",7,'\'),1,'.');
			Date=scan(scan(portfolioID,2,'('),1,')');
			MasterPortfolioId=scan(portfolioID,1,'(');
/*			format date date9.;*/
			drop portfolioID;

     		if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     	run;

		 %IF(&k=1)%THEN %DO;
                                PROC SQL;
                                        CREATE TABLE final.final
                                                LIKE HoldingDetail;
                                QUIT;
                        %END;
                        %let k=%eval(&k+1);

                proc append data=HoldingDetail base=final.final; run;
    %end;
%mend doit;

%doit


*re-enabling the log;
PROC PRINTTO PRINT=PRINT LOG=LOG ;
RUN;

data final.portfolio1;
	set final.portfolio;
	if MarketValue<0 then Weighting=-Weighting;
	if if NumberOfShare=0 and Weighting>0 then Weighting=0;
run;

/*proc sort data=final.test3 out=final.test4 nodupkey; by Date ISIN CUSIP; run;*/

proc sql;
	create table final.test as
	select distinct PortfolioId,Date,sum(Weighting) as Total_weight
	from final.portfolio1
	group by PortfolioId,Date;
quit;

proc sql;
	create table final.test1 as
	select min(Total_weight), median(Total_weight),max(Total_weight)
	from final.test;
quit;

proc univariate data=final.test noprint;
/*  by fyear;*/
  var Total_weight;
  output pctlpre=P_  out=final.test2 pctlpts=1,25,50,75,99; /*, 75 to 100 by 5*/
run;


**Merging fundid variable with portfolio data;
data final.portfolio1;
	set final.FundPortfolio_Aus_Ind;
	keep Fundid Domicile;
run;

proc sort data=final.mapping nodupkey; by FundShareClassId FundId MasterPortfolioId; run;

proc sql;
	create table final.portfolio1 as
	select distinct a.*,b.MasterPortfolioId
	from final.portfolio1 as a left join final.mapping as b
	on a.fundid=b.fundid;
quit; 

/* 108 out of 2843 do not have fundid mapping */

proc sql;
	create table final.portfolio2 as
	select distinct a.*,b.*
	from final.portfolio1 as a left join final.portfolio as b
	on a.MasterPortfolioId=input(b.MasterPortfolioId,best12.)
	order by a.Fundid,b.Date,b.Weighting;
quit;

proc sort data=final.portfolio2 nodupkey out=final.portfolio3; by fundid MasterPortfolioId date CUSIP; run;


proc sql;
	create table final.test as
	select distinct count(distinct fundid)
	from final.portfolio2;
quit; 

**Earliest inception date and last date;

proc sql;
	create table final.dates as
	select distinct fundid, domicile, masterportfolioid, min(Date) as inception_date, max(Date) as last_date
	from final.portfolio_final
	group by fundid;
quit;

data final.test1;
	set final.dates;
	if missing(inception_date)=1;
run;

data final.test;
	set final.portfolio2;
	if Domicile='India' and date='2017-04-30';
run;

proc sql;
	create table final.test2 as
	select distinct *,sum(MarketValue) as Size
	from final.test
	group by FundID;
quit;


proc sort data=final.test2; by Fundid Size; run;
