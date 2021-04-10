LIBNAME p1 'E:\Local Disk F\Morningstar\Data received from Girjinder\Price 17 July\Price_1'; run;
libname p2 'E:\Local Disk F\Morningstar\Data received from Girjinder\Price 17 July\Price_2'; run;
libname p3 'E:\Local Disk F\Morningstar\Data received from Girjinder\Price 17 July\Price_3'; run;
libname final 'E:\Local Disk F\Morningstar\Data received from Girjinder\Final datasets'; run;




**************************************Reading price data files*******************************;



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

%DIRLISTWIN(E:\Local Disk F\Morningstar\Data received from Girjinder 2017-06-06\Price 17 July\Price_3, OUT=filenames, REPORT=N, SUBDIR=Y);* Change the path

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

/* Create a macro to iterate over the filenames, read them in, and append to a data set.*/


options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;
%LET k=1;
%macro doit;
    %do i=1 %to &num;

/*        proc sql noprint;*/
/*            select filename, State, Month, Year, Dtofmn into :filename, :State, :Month, :Year, :Dtofmn from filenames1 where n=&i;*/
/*        quit;*/

        proc sql noprint;
            select filename into :filename from filenames1 where n=&i;
        quit;


		data WORK.Test    ;
     	%let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     	infile "&filename" delimiter = ';' MISSOVER DSD lrecl=32767 firstobs=2 ;
informat SecId $10.;
informat PerformanceId $10.;
informat Date $10.;
informat CurrencyISO $10.;
informat PreTaxNav best8.;
informat PreTaxBid best8.;
informat PreTaxOffer best8.;
informat PreTaxMid best8.;
informat PostTaxNav best8.;
informat PostTaxBid best8.;
informat PostTaxOffer best8.;
informat Deleted best8.;
informat LastUpdate $20.;
informat ClosePrice best8.;
informat GuaranteedNAV best8.;
informat PreliminaryNAV best8.;
informat Unsplit best8.;
informat AverageBidAskSpread best8.;
informat NumberOfObservations best8.;
informat CreationPrice best8.;
informat CancellationPrice best8.;
informat CumFairNAV best8.;
informat CumFairNAVType best8.;
informat exParNAV best8.;
informat exParNAVType best8.;
informat Discount best8.;
informat CumFairDiscount best8.;

format SecId $10.;
format PerformanceId $10.;
format Date $10.;
format CurrencyISO $10.;
format PreTaxNav best8.;
format PreTaxBid best8.;
format PreTaxOffer best8.;
format PreTaxMid best8.;
format PostTaxNav best8.;
format PostTaxBid best8.;
format PostTaxOffer best8.;
format Deleted best8.;
format LastUpdate $20.;
format ClosePrice best8.;
format GuaranteedNAV best8.;
format PreliminaryNAV best8.;
format Unsplit best8.;
format AverageBidAskSpread best8.;
format NumberOfObservations best8.;
format CreationPrice best8.;
format CancellationPrice best8.;
format CumFairNAV best8.;
format CumFairNAVType best8.;
format exParNAV best8.;
format exParNAVType best8.;
format Discount best8.;
format CumFairDiscount best8.;

		_infile_=compress(_infile_,",");
     	input

 SecId $
 PerformanceId $
 Date $
 CurrencyISO $
 PreTaxNav 
 PreTaxBid 
 PreTaxOffer 
 PreTaxMid 
 PostTaxNav 
 PostTaxBid 
 PostTaxOffer 
 Deleted 
 LastUpdate $
 ClosePrice 
 GuaranteedNAV 
 PreliminaryNAV 
 Unsplit 
 AverageBidAskSpread 
 NumberOfObservations 
 CreationPrice 
 CancellationPrice 
 CumFairNAV 
 CumFairNAVType 
 exParNAV 
 exParNAVType 
 Discount 
 CumFairDiscount 

     	;

		
	

/*		RETAIN old_Market;*/
/*		IF Market='' THEN Market=old_Market;*/
/*			ELSE old_Market=Market;*/
     	if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     	run;
		 %IF(&k=1)%THEN %DO;
                                PROC SQL;
                                        CREATE TABLE final
                                                LIKE test;
                                QUIT;
                        %END;
                        %let k=%eval(&k+1);

                proc append data=TEST base=final; run;
    %end;
%mend doit;

%doit

*distinct secIDs;
proc sql;
	create table test as
	select distinct count(distinct SecId)as ncount
	from p3.final2;
quit;

***Import data;
PROC IMPORT OUT= final.AUM
            DATAFILE= "E:\Local Disk F\Morningstar\Data received from Girjinder\AUM Historical data.xlsx" 
            DBMS=EXCEL REPLACE;
GETNAMES=YES;
MIXED=NO;
SCANTEXT=YES;
USEDATE=YES;
SCANTIME=YES;
RUN;


*converting to long form;

data final.aum;
	set final.aum;
	if missing(SecID)=1 then delete;
	drop Group_Investment;
run;

/*proc sort data=final.aum1; by SecId; run;*/

proc transpose data=final.aum out=final.aum;
  by SecId notsorted;
  var _all_;
run;

data final.aum;
	set final.aum;
	if _LABEL_='SecId' then delete;
	drop _NAME_;
run;



**********************Reading Total Returns Index(TRI) files*************************;



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

%DIRLISTWIN(E:\Local Disk F\Morningstar\Data received from Girjinder\IndexReturn, OUT=filenames, REPORT=N, SUBDIR=Y);* Change the path

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

/* Create a macro to iterate over the filenames, read them in, and append to a data set.*/


options mcompilenote=ALL;
options SYMBOLGEN MPRINT MLOGIC;
%LET k=1;
%macro doit;
    %do i=1 %to &num;

/*        proc sql noprint;*/
/*            select filename, State, Month, Year, Dtofmn into :filename, :State, :Month, :Year, :Dtofmn from filenames1 where n=&i;*/
/*        quit;*/

        proc sql noprint;
            select filename into :filename from filenames1 where n=&i;
        quit;


		data WORK.Test    ;
     	%let _EFIERR_ = 0; /* set the ERROR detection macro variable */
     	infile "&filename" delimiter = ';' MISSOVER DSD lrecl=32767 firstobs=2 ;
Format SecId $10.;
Format PerformanceId $10.;
Format Date $10.;
Format Deleted best12.;
Format Unit_BAS best12.;
Format Unit_USD best12.;
Format Unit_EUR best12.;
Format Unit_GBP best12.;
Format Unit_CHF best12.;
Format Unit_DKK best12.;
Format Unit_NOK best12.;
Format Unit_SEK best12.;
Format Unit_JPY best12.;
Format LastUpdate $20.;
Format Unit_SGD best12.;
Format ReturnType best12.;
Format Filled best12.;
Format Unit_TWD best12.;
Format Unit_HKD best12.;
Format Unit_MYR best12.;
Format Unit_CNY best12.;
Format Unit_ILS best12.;
Format Unit_INR best12.;
Format Unit_CAD best12.;
Format Unit_KWD best12.;
Format Unit_PLN best12.;
Format Unit_AUD best12.;
Format Unit_THB best12.;
Format Unit_KRW best12.;


Informat SecId $10.;
Informat PerformanceId $10.;
Informat Date $10.;
Informat Deleted best12.;
Informat Unit_BAS best12.;
Informat Unit_USD best12.;
Informat Unit_EUR best12.;
Informat Unit_GBP best12.;
Informat Unit_CHF best12.;
Informat Unit_DKK best12.;
Informat Unit_NOK best12.;
Informat Unit_SEK best12.;
Informat Unit_JPY best12.;
Informat LastUpdate $20.;
Informat Unit_SGD best12.;
Informat ReturnType best12.;
Informat Filled best12.;
Informat Unit_TWD best12.;
Informat Unit_HKD best12.;
Informat Unit_MYR best12.;
Informat Unit_CNY best12.;
Informat Unit_ILS best12.;
Informat Unit_INR best12.;
Informat Unit_CAD best12.;
Informat Unit_KWD best12.;
Informat Unit_PLN best12.;
Informat Unit_AUD best12.;
Informat Unit_THB best12.;
Informat Unit_KRW best12.;


		_infile_=compress(_infile_,",");
     	input

SecId $
PerformanceId $
Date $
Deleted
Unit_BAS
Unit_USD
Unit_EUR
Unit_GBP
Unit_CHF
Unit_DKK
Unit_NOK
Unit_SEK
Unit_JPY
LastUpdate $
Unit_SGD
ReturnType
Filled
Unit_TWD
Unit_HKD
Unit_MYR
Unit_CNY
Unit_ILS
Unit_INR
Unit_CAD
Unit_KWD
Unit_PLN
Unit_AUD
Unit_THB
Unit_KRW
;

		
	

/*		RETAIN old_Market;*/
/*		IF Market='' THEN Market=old_Market;*/
/*			ELSE old_Market=Market;*/
     	if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
     	run;
		 %IF(&k=1)%THEN %DO;
                                PROC SQL;
                                        CREATE TABLE final
                                                LIKE test;
                                QUIT;
                        %END;
                        %let k=%eval(&k+1);

                proc append data=TEST base=final; run;
    %end;
%mend doit;

%doit


data final.TRI;
	set final;
run;

**sanity checks;

proc sql;
	create table test as
	select distinct b.*,a.Obsolete_date
	from final.dailynav_aus_ind as a left join final.tri as b
	on input(a.SecId,$10.)=b.SecId;
quit;

proc sql;
	create table final.test as
	select distinct b.*,a.Obsolete_date
	from final.dailynav_aus_ind as a left join final.aum as b
	on input(a.SecId,$10.)=b.SecId;
quit;

**Mapping from MstarId to FundId;

proc sort data=final.mapping out=final.mapping1 nodupkey; by MasterPortfolioId; run;
proc sort data=final.mapping out=final.mapping2 nodupkey; by FundId; run;


proc sql;
	create table final.test as
	select distinct MasterPortfolioId,count(distinct FundId) as ncount
	from final.mapping
	group by MasterPortfolioId;
quit;


proc sql;
	create table final.test as
	select distinct a.*,b.FundId
	from final.portfolio as a left join final.mapping as b
	on input(a.PortfolioId,best12.)=b.MasterPortfolioId;
quit;


proc sql;
	create table final.test as
	select distinct count(distinct FundId) as ncount
	from final.mapping;
quit;
