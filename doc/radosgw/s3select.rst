===============
 Ceph s3 select 
===============

.. contents::

Overview
--------

    | The purpose of the **s3 select** engine is to create an efficient pipe between user client and storage nodes (the engine should be close as possible to storage).
    | It enables selection of a restricted subset of (structured) data stored in an S3 object using an SQL-like syntax.
    | It also enables for higher level analytic-applications (such as SPARK-SQL) , using that feature to improve their latency and throughput.

    | For example, a s3-object of several GB (CSV file), a user needs to extract a single column which filtered by another column.
    | As the following query:
    | ``select customer-id from s3Object where age>30 and age<65;``

    | Currently the whole s3-object must retrieve from OSD via RGW before filtering and extracting data.
    | By "pushing down" the query into OSD , it's possible to save a lot of network and CPU(serialization / deserialization).

    | **The bigger the object, and the more accurate the query, the better the performance**.
 
Basic workflow
--------------
    
    | S3-select query is sent to RGW via `AWS-CLI <https://docs.aws.amazon.com/cli/latest/reference/s3api/select-object-content.html>`_

    | It passes the authentication and permission process as an incoming message (POST).
    | **RGWSelectObj_ObjStore_S3::send_response_data** is the “entry point”, it handles each fetched chunk according to input object-key.
    | **send_response_data** is first handling the input query, it extracts the query and other CLI parameters.
   
    | Per each new fetched chunk (~4m), RGW executes s3-select query on it.    
    | The current implementation supports CSV objects and since chunks are randomly “cutting” the CSV rows in the middle, those broken-lines (first or last per chunk) are skipped while processing the query.   
    | Those “broken” lines are stored and later merged with the next broken-line (belong to the next chunk), and finally processed.
   
    | Per each processed chunk an output message is formatted according to `AWS specification <https://docs.aws.amazon.com/AmazonS3/latest/API/archive-RESTObjectSELECTContent.html#archive-RESTObjectSELECTContent-responses>`_ and sent back to the client.
    | RGW supports the following response: ``{:event-type,records} {:content-type,application/octet-stream} {:message-type,event}``.
    | For aggregation queries the last chunk should be identified as the end of input, following that the s3-select-engine initiates end-of-process and produces an aggregate result.  

        
Basic functionalities
~~~~~~~~~~~~~~~~~~~~~

    | **S3select** has a definite set of functionalities that should be implemented (if we wish to stay compliant with AWS), currently only a portion of it is implemented.
    
    | The implemented software architecture supports basic arithmetic expressions, logical and compare expressions, including nested function calls and casting operators, that alone enables the user reasonable flexibility. 
    | review the below s3-select-feature-table_.


Error Handling
~~~~~~~~~~~~~~

    | Any error occurs while the input query processing, i.e. parsing phase or execution phase, is returned to client as response error message.

    | Fatal severity (attached to the exception) will end query execution immediately, other error severity are counted, upon reaching 100, it ends query execution with an error message.



.. _s3-select-feature-table:

Features Support
----------------

  | Currently only part of `AWS select command <https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html>`_ is implemented, table below describes what is currently supported.
  | The following table describes the current implementation for s3-select functionalities:

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Detailed        | Example  / Description                                                |
+=================================+=================+=======================================================================+
| Arithmetic operators            | ^ * % / + - ( ) | select (int(_1)+int(_2))*int(_9) from s3object;                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | ``%`` modulo    | select count(*) from s3object where cast(_1 as int)%2 == 0;           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | ``^`` power-of  | select cast(2^10 as int) from s3object;                               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Compare operators               | > < >= <= == != | select _1,_2 from s3object where (int(_1)+int(_3))>int(_5);           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | AND OR NOT      | select count(*) from s3object where not (int(1)>123 and int(_5)<200); |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | is null         | return true/false for null indication in expression                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | is not null     | return true/false for null indication in expression                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator and NULL       | unknown state   | review null-handle_ observe how logical operator result with null.    |
|                                 |                 | the following query return **0**.                                     |
|                                 |                 | ``select count(*) from s3object where null and (3>2);``               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Arithmetic operator with NULL   | unknown state   | review null-handle_ observe the results of binary operations with NULL|
|                                 |                 | the following query return **0**.                                     |
|                                 |                 | ``select count(*) from s3object where (null+1) and (3>2);``           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| compare with NULL               | unknown state   | review null-handle_ observe results of compare operations with NULL   | 
|                                 |                 | the following query return **0**.                                     |
|                                 |                 | ``select count(*) from s3object where (null*1.5) != 3;``              |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| missing column                  | unknown state   | select count(*) from s3object where _1 is null;                       | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| projection column               | similar to      | select case                                                           | 
|                                 | if/then/else    |   when (1+1==(2+1)*3) then 'case_1'                                   |
|                                 |                 |   when ((4*3)==(12)) then 'case_2' else 'case_else' end,              |
|                                 |                 |   age*2 from s3object;                                                | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``coalesce`` :: return first non-null argumnet                                          | 
|                                 |                   select coalesce(nullif(5,5),nullif(1,1.0),age+12) from s3object;      |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``nullif`` :: return null in case both arguments are equal, or else the first one       |
|                                 |            nullif(1,1)=NULL nullif(null,1)=NULL nullif(2,1)=2                           |
|                                 |                     select nullif(cast(_1 as int),cast(_2 as int)) from s3object;       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``{expression} in ( .. {expression} ..)``                                               | 
|                                 |             select count(*) from s3object                                               | 
|                                 |                 where 'ben' in (trim(_5),substring(_1,char_length(_1)-3,3),last_name);  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``{expression} between {expression} and {expression}``                                  | 
|                                 |             select count(*) from stdin                                                  | 
|                                 |                 where substring(_3,char_length(_3),1) between "x" and trim(_1)          |
|                                 |                     and substring(_3,char_length(_3)-1,1) == ":";                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``{expression} like {match-pattern}``                                                   |
|                                 |                   select count(*) from s3object where first_name like '%de_';           | 
|                                 |                   select count(*) from s3object where _1 like \"%a[r-s]\;               | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast(123 as int)%2 from s3object;                                                |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast(123.456 as float)%2 from s3object;                                          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast('ABC0-9' as string),cast(substr('ab12cd',3,2) as int)*4  from s3object;     |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast(substring('publish on 2007-01-01',12,10) as timestamp) from s3object;       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| non AWS casting operator        | select int(_1),int( 1.2 + 3.4) from s3object;                                           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| non AWS casting operator        | select float(1.2) from s3object;                                                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| not AWS casting operator        | select timestamp('1999:10:10-12:23:44') from s3object;                                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | sum             | select sum(int(_1)) from s3object;                                    |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | avg             | select avg(cast(_1 a float) + cast(_2 as int)) from s3object;         |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | min             | select min( int(_1) * int(_5) ) from s3object;                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | max             | select max(float(_1)),min(int(_5)) from s3object;                     |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | count           | select count(*) from s3object where (int(1)+int(_3))>int(_5);         |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | extract         | select count(*) from s3object where                                   |
|                                 |                 | extract('year',timestamp(_2)) > 1950                                  |    
|                                 |                 | and extract('year',timestamp(_1)) < 1960;                             |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | dateadd         | select count(0) from s3object where                                   |
|                                 |                 | datediff('year',timestamp(_1),dateadd('day',366,timestamp(_1))) == 1; |  
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | datediff        | select count(0) from s3object where                                   |  
|                                 |                 | datediff('month',timestamp(_1),timestamp(_2))) == 2;                  | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | utcnow          | select count(0) from s3object where                                   |
|                                 |                 | datediff('hours',utcnow(),dateadd('day',1,utcnow())) == 24 ;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | substring       | select count(0) from s3object where                                   |
|                                 |                 | int(substring(_1,1,4))>1950 and int(substring(_1,1,4))<1960;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | trim            | select trim('   foobar   ') from s3object;                            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | trim            | select trim(trailing from '   foobar   ') from s3object;              |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | trim            | select trim(leading from '   foobar   ') from s3object;               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | trim            | select trim(both '12' from  '1112211foobar22211122') from s3objects;  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | lower/upper     | select lower('ABcD12#$e') from s3object;                              |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | char_length     | select count(*) from s3object where char_length(_3)==3;               |
|                                 | character_length|                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Complex queries                 |   select sum(cast(_1 as int)),                                                          |
|                                 |          max(cast(_3 as int)),                                                          |
|                                 |          substring('abcdefghijklm',                                                     |
|                                 |          (2-1)*3+sum(cast(_1 as int))/sum(cast(_1 as int))+1,                           |
|                                 |          (count() + count(0))/count(0)) from s3object;                                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| alias support                   |                 |  select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3                  | 
|                                 |                 |  from s3object where a3>100 and a3<300;                               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+

.. _null-handle:

NULL
~~~~
| NULL is a legit value in ceph-s3select systems similar to other DB systems, i.e. systems needs to handle the case where a value is NULL.
| The definition of NULL in our context, is missing/unknown, in that sense **NULL can not produce a value on ANY arithmetic operations** ( a + NULL will produce NULL value).
| The Same is with arithmetic comaprision, **any comparison to NULL is NULL**, i.e. unknown.
| Below is a truth table contains the NULL use-case.

+---------------------------------+-----------------------------+
| A is NULL                       | Result (NULL=UNKNOWN)       |
+=================================+=============================+
| NOT A                           |  NULL                       |
+---------------------------------+-----------------------------+
| A OR False                      |  NULL                       |
+---------------------------------+-----------------------------+
| A OR True                       |  True                       |
+---------------------------------+-----------------------------+
| A OR A                          |  NULL                       |
+---------------------------------+-----------------------------+
| A AND False                     |  False                      |
+---------------------------------+-----------------------------+
| A AND True                      |  NULL                       | 
+---------------------------------+-----------------------------+
| A and A                         |  NULL                       |
+---------------------------------+-----------------------------+

s3-select function interfaces
-----------------------------

Timestamp functions
~~~~~~~~~~~~~~~~~~~
    | The `timestamp functionalities <https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-date.html>`_ is partially implemented.
    | the casting operator( ``timestamp( string )`` ), converts string to timestamp basic type.
    | Currently it can convert the following pattern ``yyyy:mm:dd hh:mi:dd``

    | ``extract( date-part , timestamp)`` : function return integer according to date-part extract from input timestamp.
    | supported date-part : year,month,week,day.

    | ``dateadd(date-part , integer,timestamp)`` : function return timestamp, a calculation results of input timestamp and date-part.
    | supported data-part : year,month,day.

    | ``datediff(date-part,timestamp,timestamp)`` : function return an integer, a calculated result for difference between 2 timestamps according to date-part.
    | supported date-part : year,month,day,hours.  

    | ``utcnow()`` : return timestamp of current time.

Aggregation functions
~~~~~~~~~~~~~~~~~~~~~

    | ``count()`` : return integer according to number of rows matching condition(if such exist).

    | ``sum(expression)`` : return a summary of expression per all rows matching condition(if such exist).

    | ``avg(expression)`` : return a average  of expression per all rows matching condition(if such exist).

    | ``max(expression)`` : return the maximal result for all expressions matching condition(if such exist).

    | ``min(expression)`` : return the minimal result for all expressions matching condition(if such exist).

String functions
~~~~~~~~~~~~~~~~

    | ``substring(string,from,to)`` : return a string extract from input string according to from,to inputs.

    | ``char_length`` : return a number of characters in string (``character_length`` does the same).

    | ``trim`` : trims leading/trailing characters from target string, the default is blank character.

    | ``upper\lower`` : converts characters into lowercase/uppercase.


Alias
~~~~~
    | **Alias** programming-construct is an essential part of s3-select language, it enables much better programming especially with objects containing many columns or in the case of complex queries.
    
    | Upon parsing the statement containing alias construct, it replaces alias with reference to correct projection column, on query execution time the reference is evaluated as any other expression.

    | There is a risk that self(or cyclic) reference may occur causing stack-overflow(endless-loop), for that concern upon evaluating an alias, it is validated for cyclic reference.
    
    | Alias also maintains result-cache, meaning upon using the same alias more than once, it’s not evaluating the same expression again(it will return the same result),instead it uses the result from cache.

    | Of Course, per each new row the cache is invalidated.

Sending Query to RGW
--------------------

   | Any http-client can send s3-select request to RGW, it must be compliant with `AWS Request syntax <https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html#API_SelectObjectContent_RequestSyntax>`_.



   | Sending s3-select request to RGW using AWS cli, should follow `AWS command reference <https://docs.aws.amazon.com/cli/latest/reference/s3api/select-object-content.html>`_.
   | below is an example for it.

::

 aws --endpoint-url http://localhost:8000 s3api select-object-content 
  --bucket {BUCKET-NAME}  
  --expression-type 'SQL'     
  --input-serialization 
  '{"CSV": {"FieldDelimiter": "," , "QuoteCharacter": "\"" , "RecordDelimiter" : "\n" , "QuoteEscapeCharacter" : "\\" , "FileHeaderInfo": "USE" }, "CompressionType": "NONE"}' 
  --output-serialization '{"CSV": {}}' 
  --key {OBJECT-NAME} 
  --expression "select count(0) from s3object where int(_1)<10;" output.csv

Syntax
~~~~~~

    | **Input serialization** (Implemented), it let the user define the CSV definitions; the default values are {\\n} for row-delimiter {,} for field delimiter, {"} for quote, {\\} for escape characters.
    | it handle the **csv-header-info**, the first row in input object containing the schema.
    | **Output serialization** is currently not implemented, the same for **compression-type**.

    | s3-select engine contain a CSV parser, which parse s3-objects as follows.   
    | - each row ends with row-delimiter.
    | - field-separator separates between adjacent columns, successive field separator define NULL column.
    | - quote-character overrides field separator, meaning , field separator become as any character between quotes.
    | - escape character disables any special characters, except for row delimiter.
    
    | Below are examples for CSV parsing rules.


CSV parsing behavior
--------------------

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Description     | input ==> tokens                                                      |
+=================================+=================+=======================================================================+
|     NULL                        | successive      | ,,1,,2,    ==> {null}{null}{1}{null}{2}{null}                         |
|                                 | field delimiter |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     QUOTE                       | quote character | 11,22,"a,b,c,d",last ==> {11}{22}{"a,b,c,d"}{last}                    |
|                                 | overrides       |                                                                       |
|                                 | field delimiter |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     Escape                      | escape char     | 11,22,str=\\"abcd\\"\\,str2=\\"123\\",last                            |
|                                 | overrides       | ==> {11}{22}{str="abcd",str2="123"}{last}                             |
|                                 | meta-character. |                                                                       |
|                                 | escape removed  |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     row delimiter               | no close quote, | 11,22,a="str,44,55,66                                                 |
|                                 | row delimiter is| ==> {11}{22}{a="str,44,55,66}                                         |
|                                 | closing line    |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|     csv header info             | FileHeaderInfo  | "**USE**" value means each token on first line is column-name,        |
|                                 | tag             | "**IGNORE**" value means to skip the first line                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+       


BOTO3
-----

 | using BOTO3 is "natural" and easy due to AWS-cli support. 

::


 def run_s3select(bucket,key,query,column_delim=",",row_delim="\n",quot_char='"',esc_char='\\',csv_header_info="NONE"):
    s3 = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        region_name=region_name,
        aws_secret_access_key=secret_key)
        


    r = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        InputSerialization = {"CSV": {"RecordDelimiter" : row_delim, "FieldDelimiter" : column_delim,"QuoteEscapeCharacter": esc_char, "QuoteCharacter": quot_char, "FileHeaderInfo": csv_header_info}, "CompressionType": "NONE"},
        OutputSerialization = {"CSV": {}},
        Expression=query,)

    result = ""
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            result += records

    return result




  run_s3select(
  "my_bucket",
  "my_csv_object",
  "select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3 from s3object where a3>100 and a3<300;")

