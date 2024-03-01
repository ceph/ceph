===============
 Ceph s3 select 
===============

.. contents::

Overview
--------

The **S3 Select** engine creates an efficient pipe between clients and Ceph
back end nodes. The S3 Select engine works best when implemented as closely as
possible to back end storage.

The S3 Select engine makes it possible to use an SQL-like syntax to select a
restricted subset of data stored in an S3 object. The S3 Select engine
facilitates the use of higher level, analytic applications (for example:
SPARK-SQL). The ability of the S3 Select engine to target a proper subset of
structed data within an S3 object decreases latency and increases throughput.

For example: assume that a user needs to extract a single column that is
filtered by another column, and that these colums are stored in a CSV file in
an S3 object that is several GB in size. The following query performs this
extraction: ``select customer-id from s3Object where age>30 and age<65;``

Without the use of S3 Select, the whole S3 object must be retrieved from an OSD
via RGW before the data is filtered and extracted. Significant network and CPU
overhead are saved by "pushing down" the query into radosgw.

**The bigger the object and the more accurate the query,
the better the performance of s3select**.
 
Basic Workflow
--------------
    
S3 Select queries are sent to RGW via `AWS-CLI
<https://docs.aws.amazon.com/cli/latest/reference/s3api/select-object-content.html>`_

S3 Select passes the authentication and permission parameters as an incoming
message (POST). ``RGWSelectObj_ObjStore_S3::send_response_data`` is the entry
point and handles each fetched chunk according to the object key that was
input.  ``send_response_data`` is the first to handle the input query: it
extracts the query and other CLI parameters.
   
RGW executes an S3 Select query on each new fetched chunk (up to 4 MB). The
current implementation supports CSV objects. CSV rows are sometimes "cut" in
the middle by the limits of the chunks, and those broken-lines (the first or
last per chunk) are skipped while processing the query. Such broken lines are
stored and later merged with the next broken line (which belongs to the next
chunk), and only then processed.

For each processed chunk, an output message is formatted according to `aws
specification
<https://docs.aws.amazon.com/amazons3/latest/api/archive-restobjectselectcontent.html#archive-restobjectselectcontent-responses>`_
and sent back to the client. RGW supports the following response:
``{:event-type,records} {:content-type,application/octet-stream}
{:message-type,event}``. For aggregation queries, the last chunk should be
identified as the end of input. 

        
Basic Functionalities
~~~~~~~~~~~~~~~~~~~~~

**S3select** has a definite set of functionalities compliant with AWS.
    
The implemented software architecture supports basic arithmetic expressions,
logical and compare expressions, including nested function calls and casting
operators, which enables the user great flexibility. 

review the below s3-select-feature-table_.


Error Handling
~~~~~~~~~~~~~~

Upon an error being detected, RGW returns 400-Bad-Request and a specific error message sends back to the client.
Currently, there are 2 main types of error.

**Syntax error**: the s3select parser rejects user requests that are not aligned with parser syntax definitions, as     
described in this documentation.
Upon Syntax Error, the engine creates an error message that points to the location of the error.
RGW sends back the error message in a specific error response. 

**Processing Time error**: the runtime engine may detect errors that occur only on processing time, for that type of     
error, a different error message would describe that.
RGW sends back the error message in a specific error response.

.. _s3-select-feature-table:

Features Support
----------------

Currently only part of `AWS select command
<https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html>`_
is implemented, table below describes what is currently supported.

The following table describes the current implementation for s3-select
functionalities:

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Detailed        | Example  / Description                                                |
+=================================+=================+=======================================================================+
| Arithmetic operators            | ^ * % / + - ( ) | select (int(_1)+int(_2))*int(_9) from s3object;                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | ``%`` modulo    | select count(*) from s3object where cast(_1 as int)%2 = 0;            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | ``^`` power-of  | select cast(2^10 as int) from s3object;                               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Compare operators               | > < >= <= = !=  | select _1,_2 from s3object where (int(_1)+int(_3))>int(_5);           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | AND OR NOT      | select count(*) from s3object where not (int(_1)>123 and int(_5)<200);|
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | is null         | return true/false for null indication in expression                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | is not null     | return true/false for null indication in expression                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator and NULL       | unknown state   | review null-handle_ observe how logical operator result with null.    |
|                                 |                 | the following query return **0**.                                     |
|                                 |                 |                                                                       |
|                                 |                 | select count(*) from s3object where null and (3>2);                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Arithmetic operator with NULL   | unknown state   | review null-handle_ observe the results of binary operations with NULL|
|                                 |                 | the following query return **0**.                                     |
|                                 |                 |                                                                       |
|                                 |                 | select count(*) from s3object where (null+1) and (3>2);               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| compare with NULL               | unknown state   | review null-handle_ observe results of compare operations with NULL   | 
|                                 |                 | the following query return **0**.                                     |
|                                 |                 |                                                                       |
|                                 |                 | select count(*) from s3object where (null*1.5) != 3;                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| missing column                  | unknown state   | select count(*) from s3object where _1 is null;                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| query is filtering rows where predicate           | select count(*) from s3object where (_1 > 12 and _2 = 0) is not null; |
| is returning non null results.                    |                                                                       |
| this predicate will return null                   |                                                                       |
| upon _1 or _2 is null                             |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| projection column               | similar to      | select case                                                           | 
|                                 | switch/case     | cast(_1 as int) + 1                                                   |
|                                 | default         | when 2 then "a"                                                       |
|                                 |                 | when 3  then "b"                                                      |
|                                 |                 | else "c" end from s3object;                                           |
|                                 |                 |                                                                       | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| projection column               | similar to      | select case                                                           | 
|                                 | if/then/else    | when (1+1=(2+1)*3) then 'case_1'                                      |
|                                 |                 | when ((4*3)=(12)) then 'case_2'                                       |
|                                 |                 | else 'case_else' end,                                                 |
|                                 |                 | age*2 from s3object;                                                  | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``coalesce {expression,expression ...} :: return first non-null argument``              |
|                                 |                                                                                         |
|                                 | select coalesce(nullif(5,5),nullif(1,1.0),age+12) from s3object;                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``nullif {expr1,expr2} ::return null in case both arguments are equal,``                |
|                                 | ``or else the first one``                                                               |
|                                 |                                                                                         |
|                                 | select nullif(cast(_1 as int),cast(_2 as int)) from s3object;                           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``{expression} in ( .. {expression} ..)``                                               |
|                                 |                                                                                         |
|                                 | select count(*) from s3object                                                           | 
|                                 | where 'ben' in (trim(_5),substring(_1,char_length(_1)-3,3),last_name);                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``{expression} between {expression} and {expression}``                                  | 
|                                 |                                                                                         |
|                                 | select count(*) from s3object                                                           | 
|                                 | where substring(_3,char_length(_3),1) between "x" and trim(_1)                          |
|                                 | and substring(_3,char_length(_3)-1,1) = ":";                                            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | ``{expression} like {match-pattern}``                                                   |
|                                 |                                                                                         |
|                                 | select count(*) from s3object where first_name like '%de_';                             |
|                                 |                                                                                         |
|                                 | select count(*) from s3object where _1 like \"%a[r-s]\;                                 |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | ``{expression} like {match-pattern} escape {char}``                                     |
|                                 |                                                                                         |
| logical operator                | select count(*) from s3object where  "jok_ai" like "%#_ai" escape "#";                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| true / false                    | select (cast(_1 as int)>123 = true) from s3object                                       |
| predicate as a projection       | where address like '%new-york%';                                                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| an alias to                     | select (_1 like "_3_") as *likealias*,_1 from s3object                                  |
| predicate as a projection       | where *likealias* = true and cast(_1 as int) between 800 and 900;                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast(123 as int)%2 from s3object;                                                |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast(123.456 as float)%2 from s3object;                                          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast('ABC0-9' as string),cast(substr('ab12cd',3,2) as int)*4  from s3object;     |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast(5 as bool) from s3object;                                                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | select cast(substring('publish on 2007-01-01',12,10) as timestamp) from s3object;       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| non AWS casting operator        | select int(_1),int( 1.2 + 3.4) from s3object;                                           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| non AWS casting operator        | select float(1.2) from s3object;                                                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| not AWS casting operator        | select to_timestamp('1999-10-10T12:23:44Z') from s3object;                              |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | sum             | select sum(int(_1)) from s3object;                                    |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | avg             | select avg(cast(_1 a float) + cast(_2 as int)) from s3object;         |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | min             | select min( int(_1) * int(_5) ) from s3object;                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | max             | select max(float(_1)),min(int(_5)) from s3object;                     |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | count           | select count(*) from s3object where (int(_1)+int(_3))>int(_5);        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | extract         | select count(*) from s3object where                                   |
|                                 |                 | extract(year from to_timestamp(_2)) > 1950                            |
|                                 |                 | and extract(year from to_timestamp(_1)) < 1960;                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | date_add        | select count(0) from s3object where                                   |
|                                 |                 | date_diff(year,to_timestamp(_1),date_add(day,366,                     |
|                                 |                 | to_timestamp(_1))) = 1;                                               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | date_diff       | select count(0) from s3object where                                   |
|                                 |                 | date_diff(month,to_timestamp(_1),to_timestamp(_2))) = 2;              |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | utcnow          | select count(0) from s3object where                                   |
|                                 |                 | date_diff(hours,utcnow(),date_add(day,1,utcnow())) = 24;              |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | to_string       | select to_string(                                                     |
|                                 |                 | to_timestamp("2009-09-17T17:56:06.234567Z"),                          |
|                                 |                 | "yyyyMMdd-H:m:s") from s3object;                                      |
|                                 |                 |                                                                       |
|                                 |                 | ``result: "20090917-17:56:6"``                                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | substring       | select count(0) from s3object where                                   |
|                                 |                 | int(substring(_1,1,4))>1950 and int(substring(_1,1,4))<1960;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| substring with ``from`` negative number is valid  | select substring("123456789" from -4) from s3object;                  |
| considered as first                               |                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| substring with ``from`` zero ``for`` out-of-bound |  select substring("123456789" from 0 for 100) from s3object;          |
| number is valid just as (first,last)              |                                                                       |
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
| String Functions                | char_length     | select count(*) from s3object where char_length(_3)=3;                |
|                                 | character_length|                                                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Complex queries                 | select sum(cast(_1 as int)),                                                            |
|                                 | max(cast(_3 as int)),                                                                   |
|                                 | substring('abcdefghijklm',(2-1)*3+sum(cast(_1 as int))/sum(cast(_1 as int))+1,          |
|                                 | (count() + count(0))/count(0)) from s3object;                                           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| alias support                   |                 |  select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3                  | 
|                                 |                 |  from s3object where a3>100 and a3<300;                               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+

.. _null-handle:

NULL
~~~~
NULL is a legit value in ceph-s3select systems similar to other DB systems, i.e. systems needs to handle the case where a value is NULL.

The definition of NULL in our context, is missing/unknown, in that sense **NULL can not produce a value on ANY arithmetic operations** ( a + NULL will produce NULL value).

The Same is with arithmetic comparison, **any comparison to NULL is NULL**, i.e. unknown.
Below is a truth table contains the NULL use-case.

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

S3-select Function Interfaces
-----------------------------

Timestamp Functions
~~~~~~~~~~~~~~~~~~~
The timestamp functionalities as described in `AWS-specs
<https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-date.html>`_
is fully implemented.

 ``to_timestamp( string )`` : The casting operator converts string to timestamp
 basic type.  to_timestamp operator is able to convert the following
 ``YYYY-MM-DDTHH:mm:ss.SSSSSS+/-HH:mm`` , ``YYYY-MM-DDTHH:mm:ss.SSSSSSZ`` ,
 ``YYYY-MM-DDTHH:mm:ss+/-HH:mm`` , ``YYYY-MM-DDTHH:mm:ssZ`` ,
 ``YYYY-MM-DDTHH:mm+/-HH:mm`` , ``YYYY-MM-DDTHH:mmZ`` , ``YYYY-MM-DDT`` or
 ``YYYYT`` string formats into timestamp.  Where time (or part of it) is
 missing in the string format, zero's are replacing the missing parts. And for
 missing month and day, 1 is default value for them.  Timezone part is in
 format ``+/-HH:mm`` or ``Z`` , where the letter "Z" indicates Coordinated
 Universal Time (UTC). Value of timezone can range between -12:00 and +14:00.

 ``extract(date-part from timestamp)`` : The function extracts date-part from
 input timestamp and returns it as integer.  Supported date-part : year, month,
 week, day, hour, minute, second, timezone_hour, timezone_minute.

 ``date_add(date-part, quantity, timestamp)`` : The function adds quantity
 (integer) to date-part of timestamp and returns result as timestamp. It also
 includes timezone in calculation.  Supported data-part : year, month, day,
 hour, minute, second.

 ``date_diff(date-part, timestamp, timestamp)`` : The function returns an
 integer, a calculated result for difference between 2 timestamps according to
 date-part. It includes timezone in calculation.  supported date-part : year,
 month, day, hour, minute, second.

 ``utcnow()`` : return timestamp of current time.

 ``to_string(timestamp, format_pattern)`` : returns a string representation of
 the input timestamp in the given input string format.

to_string parameters
~~~~~~~~~~~~~~~~~~~~

+--------------+-----------------+-----------------------------------------------------------------------------------+
| Format       | Example         | Description                                                                       |
+==============+=================+===================================================================================+
|    yy        | 69              |  2-digit year                                                                     |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    y         | 1969            |  4-digit year                                                                     |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    yyyy      | 1969            |  Zero-padded 4-digit year                                                         |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    M         | 1               |  Month of year                                                                    |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    MM        | 01              |  Zero-padded month of year                                                        |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    MMM       | Jan             |  Abbreviated month year name                                                      |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    MMMM      | January         |  Full month of year name                                                          |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    MMMMM     | J               |  Month of year first letter (NOTE: not valid for use with to_timestamp function)  |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    d         | 2               |  Day of month (1-31)                                                              |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    dd        | 02              |  Zero-padded day of month (01-31)                                                 |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    a         | AM              |  AM or PM of day                                                                  |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    h         | 3               |  Hour of day (1-12)                                                               |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    hh        | 03              |  Zero-padded hour of day (01-12)                                                  |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    H         | 3               |  Hour of day (0-23)                                                               |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    HH        | 03              |  Zero-padded hour of day (00-23)                                                  |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    m         | 4               |  Minute of hour (0-59)                                                            |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    mm        | 04              |  Zero-padded minute of hour (00-59)                                               |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    s         | 5               |  Second of minute (0-59)                                                          |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    ss        | 05              |  Zero-padded second of minute (00-59)                                             |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    S         | 0               |  Fraction of second (precision: 0.1, range: 0.0-0.9)                              |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    SS        | 6               |  Fraction of second (precision: 0.01, range: 0.0-0.99)                            |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    SSS       | 60              |  Fraction of second (precision: 0.001, range: 0.0-0.999)                          |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    SSSSSS    | 60000000        |  Fraction of second (maximum precision: 1 nanosecond, range: 0.0-0999999999)      |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    n         | 60000000        |  Nano of second                                                                   |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    X         | +07 or Z        |  Offset in hours or "Z" if the offset is 0                                        |
+--------------+-----------------+-----------------------------------------------------------------------------------+
|    XX or XXXX| +0700 or Z      |  Offset in hours and minutes or "Z" if the offset is 0                            |
+--------------+-----------------+-----------------------------------------------------------------------------------+
| XXX or XXXXX | +07:00 or Z     |  Offset in hours and minutes or "Z" if the offset is 0                            |
+--------------+-----------------+-----------------------------------------------------------------------------------+
| X            | 7               |  Offset in hours                                                                  |
+--------------+-----------------+-----------------------------------------------------------------------------------+
| xx or xxxx   | 700             |  Offset in hours and minutes                                                      |
+--------------+-----------------+-----------------------------------------------------------------------------------+
| xxx or xxxxx | +07:00          |  Offset in hours and minutes                                                      |
+--------------+-----------------+-----------------------------------------------------------------------------------+


Aggregation Functions
~~~~~~~~~~~~~~~~~~~~~

``count()`` : return integer according to number of rows matching condition(if such exist).

``sum(expression)`` : return a summary of expression per all rows matching condition(if such exist).

``avg(expression)`` : return a average  of expression per all rows matching condition(if such exist).

``max(expression)`` : return the maximal result for all expressions matching condition(if such exist).

``min(expression)`` : return the minimal result for all expressions matching condition(if such exist).

String Functions
~~~~~~~~~~~~~~~~

``substring(string,from,to)`` : substring( string ``from`` start [ ``for`` length ] )
return a string extract from input string according to from,to inputs.
``substring(string from )`` 
``substring(string from for)`` 

``char_length`` : return a number of characters in string (``character_length`` does the same).

``trim`` : trim ( [[``leading`` | ``trailing`` | ``both`` remove_chars] ``from``] string )
trims leading/trailing(or both) characters from target string, the default is blank character.

``upper\lower`` : converts characters into lowercase/uppercase.

SQL Limit Operator
~~~~~~~~~~~~~~~~~~

The SQL LIMIT operator is used to limit the number of rows processed by the query.
Upon reaching the limit set by the user, the RGW stops fetching additional chunks.
TODO : add examples, for aggregation and non-aggregation queries.

Alias
~~~~~
**Alias** programming-construct is an essential part of s3-select language, it enables much better programming especially with objects containing many columns or in the case of complex queries.
    
Upon parsing the statement containing alias construct, it replaces alias with reference to correct projection column, on query execution time the reference is evaluated as any other expression.

There is a risk that self(or cyclic) reference may occur causing stack-overflow(endless-loop), for that concern upon evaluating an alias, it is validated for cyclic reference.
    
Alias also maintains a result cache, meaning that successive uses of a given alias do not evaluate the expression again.  The result is instead returned from the cache.

With each new row the cache is invalidated as the results may then differ.

Testing
~~~~~~~
    
``s3select`` contains several testing frameworks which provide a large coverage for its functionalities.

(1) Tests comparison against a trusted engine, meaning,  C/C++ compiler is a trusted expression evaluator, 
since the syntax for arithmetical and logical expressions are identical (s3select compare to C) 
the framework runs equal expressions and validates their results.
A dedicated expression generator produces different sets of expressions per each new test session. 

(2) Compares results of queries whose syntax is different but which are semantically equivalent.
This kind of test validates that different runtime flows produce an identical result 
on each run with a different, random dataset.

For example, on a dataset which contains a random numbers(1-1000)
the following queries will produce identical results.
``select count(*) from s3object where char_length(_3)=3;``
``select count(*) from s3object where cast(_3 as int)>99 and cast(_3 as int)<1000;``

(3) Constant dataset, the conventional way of testing. A query is processing a constant dataset, its result is validated against constant results.   

Additional Syntax Support
~~~~~~~~~~~~~~~~~~~~~~~~~

S3select syntax supports table-alias ``select s._1 from s3object s where s._2 = ‘4’;``

S3select syntax supports case insensitive ``Select SUM(Cast(_1 as int)) FROM S3Object;``

S3select syntax supports statements without closing semicolon  ``select count(*) from s3object``


Sending Query to RGW
--------------------

Any HTTP client can send an ``s3-select`` request to RGW, which must be compliant with `AWS Request syntax <https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html#API_SelectObjectContent_RequestSyntax>`_.



When sending an ``s3-select`` request to RGW using AWS CLI, clients must follow `AWS command reference <https://docs.aws.amazon.com/cli/latest/reference/s3api/select-object-content.html>`_.
Below is an example:

::

 aws --endpoint-url http://localhost:8000 s3api select-object-content 
  --bucket {BUCKET-NAME}  
  --expression-type 'SQL'
  --scan-range '{"Start" : 1000, "End" : 1000000}' 
  --input-serialization 
  '{"CSV": {"FieldDelimiter": "," , "QuoteCharacter": "\"" , "RecordDelimiter" : "\n" , "QuoteEscapeCharacter" : "\\" , "FileHeaderInfo": "USE" }, "CompressionType": "NONE"}' 
  --output-serialization '{"CSV": {"FieldDelimiter": ":", "RecordDelimiter":"\t", "QuoteFields": "ALWAYS"}}' 
  --key {OBJECT-NAME}
  --request-progress '{"Enabled": True}'
  --expression "select count(0) from s3object where int(_1)<10;" output.csv

Input Serialization
~~~~~~~~~~~~~~~~~~~

**FileHeaderInfo** -> (string)
Describes the first line of input. Valid values are:
 
**NONE** : The first line is not a header.
**IGNORE** : The first line is a header, but you can't use the header values to indicate the column in an expression.      
it's possible to use column position (such as _1, _2, …) to indicate the column (``SELECT s._1 FROM S3OBJECT s``).
**USE** : First line is a header, and you can use the header value to identify a column in an expression (``SELECT column_name FROM S3OBJECT``).

**QuoteEscapeCharacter** -> (string) 
A single character used for escaping the quotation mark character inside an already escaped value.

**RecordDelimiter** -> (string) 
A single character is used to separate individual records in the input. Instead of the default value, you can specify an arbitrary delimiter.

**FieldDelimiter** -> (string) 
A single character is used to separate individual fields in a record. You can specify an arbitrary delimiter.

Output Serialization
~~~~~~~~~~~~~~~~~~~~

**AWS CLI example**

    aws s3api select-object-content \
    --bucket "mybucket" \
    --key keyfile1 \
    --expression "SELECT * FROM s3object s" \
    --expression-type 'SQL' \
    --request-progress '{"Enabled": false}' \
    --input-serialization '{"CSV": {"FieldDelimiter": ","}, "CompressionType": "NONE"}' \
    --output-serialization '{"CSV": {"FieldDelimiter": ":", "RecordDelimiter":"\\t", "QuoteFields": "ALWAYS"}}' /dev/stdout
    
    **QuoteFields** -> (string)
    Indicates whether to use quotation marks around output fields.
    **ALWAYS**: Always use quotation marks for output fields.
    **ASNEEDED** (not implemented): Use quotation marks for output fields when needed.
   
    **RecordDelimiter** -> (string)
    A single character is used to separate individual records in the output. Instead of the default value, you can specify an        
    arbitrary delimiter.
    
    **FieldDelimiter** -> (string)
    The value used to separate individual fields in a record. You can specify an arbitrary delimiter.

Scan Range Option
~~~~~~~~~~~~~~~~~

    The scan range option to AWS-CLI enables the client to scan and process only a selected part of the object. 
    This option reduces input/output operations and bandwidth by skipping parts of the object that are not of interest.
    TODO : different data-sources (CSV, JSON, Parquet)

CSV Parsing Behavior
--------------------

     The ``s3-select`` engine contains a CSV parser, which parses s3-objects as follows.   
     - Each row ends with ``row-delimiter``.
     - ``field-separator`` separates adjacent columns, successive instances of ``field separator`` define a NULL column.
     - ``quote-character`` overrides ``field separator``, meaning that ``field separator`` is treated like any character between quotes.
     - ``escape character`` disables interpretation of special characters, except for ``row delimiter``.
    
     Below are examples of CSV parsing rules.

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

JSON
--------------------

A JSON reader has been integrated with the ``s3select-engine``, which allows the client to use SQL statements to scan and extract information from JSON documents. 
It should be noted that the data readers and parsers for CSV, Parquet, and JSON documents are separated from the SQL engine itself, so all of these readers use the same SQL engine.

It's important to note that values in a JSON document can be nested in various ways, such as within objects or arrays.
These objects and arrays can be nested within each other without any limitations.
When using SQL to query a specific value in a JSON document, the client must specify the location of the value
via a path in the SELECT statement.

The SQL engine processes the SELECT statement in a row-based fashion.
It uses the columns specified in the statement to perform its projection calculation, and each row contains values for these columns.
In other words, the SQL engine processes each row one at a time (and aggregates results), using the values in the columns to perform SQL calculations.
However, the generic structure of a JSON document does not have a row-and-column structure like CSV or Parquet.
Instead, it is the SQL statement itself that defines the rows and columns when querying a JSON document.

When querying JSON documents using SQL, the FROM clause in the SELECT statement defines the row boundaries.
A row in a JSON document should be similar to how the row delimiter is used to define rows when querying CSV objects, and how row groups are used to define rows when querying Parquet objects.
The statement "SELECT ... FROM s3object[*].aaa.bb.cc" instructs the reader to search for the path "aaa.bb.cc" and defines the row boundaries based on the occurrence of this path.
A row begins when the reader encounters the path, and it ends when the reader exits the innermost part of the path, which in this case is the object "cc".

NOTE : The semantics of querying JSON document may change and may not be the same as the current methodology described.

TODO : relevant example for object and array values.

A JSON Query Example
--------------------

::

 {
  "firstName": "Joe",
  "lastName": "Jackson",
  "gender": "male",
  "age": "twenty",
  "address": {
  "streetAddress": "101",
  "city": "San Diego",
  "state": "CA"
  },

  "firstName": "Joe_2",
  "lastName": "Jackson_2",
  "gender": "male",
  "age": 21,
  "address": {
  "streetAddress": "101",
  "city": "San Diego",
  "state": "CA"
  },

  "phoneNumbers": [
    { "type": "home1", "number": "734928_1","addr": 11 },
    { "type": "home2", "number": "734928_2","addr": 22 },
    { "type": "home3", "number": "734928_3","addr": 33 },
    { "type": "home4", "number": "734928_4","addr": 44 },
    { "type": "home5", "number": "734928_5","addr": 55 },
    { "type": "home6", "number": "734928_6","addr": 66 },
    { "type": "home7", "number": "734928_7","addr": 77 },
    { "type": "home8", "number": "734928_8","addr": 88 },
    { "type": "home9", "number": "734928_9","addr": 99 },
    { "type": "home10", "number": "734928_10","addr": 100 }
  ],

  "key_after_array": "XXX",

  "description" : {
    "main_desc" : "value_1",
    "second_desc" : "value_2"
  }
 }

  # the from-clause define a single row.
  # _1 points to root object level.
  # _1.age appears twice in Documnet-row, the last value is used for the operation.  
  query = "select _1.firstname,_1.key_after_array,_1.age+4,_1.description.main_desc,_1.description.second_desc from s3object[*];";
  expected_result = Joe_2,XXX,25,value_1,value_2


  # the from-clause points the phonenumbers array (it defines the _1)
  # each element in phoneNumbers array define a row. 
  # in this case each element is an object contains 3 keys/values.
  # the query "can not access" values outside phonenumbers array, the query can access only values appears on _1.phonenumbers path.
  query = "select cast(substring(_1.number,1,6) as int) *10 from s3object[*].phonenumbers where _1.type='home2';";
  expected_result = 7349280  


BOTO3
-----

using BOTO3 is "natural" and easy due to AWS-cli support. 

::

 import pprint

 def run_s3select(bucket,key,query,column_delim=",",row_delim="\n",quot_char='"',esc_char='\\',csv_header_info="NONE"):

    s3 = boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        region_name=region_name,
        aws_secret_access_key=secret_key)

    result = ""
    try:
        r = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        InputSerialization = {"CSV": {"RecordDelimiter" : row_delim, "FieldDelimiter" : column_delim,"QuoteEscapeCharacter": esc_char, "QuoteCharacter": quot_char, "FileHeaderInfo": csv_header_info}, "CompressionType": "NONE"},
        OutputSerialization = {"CSV": {}},
        Expression=query,
        RequestProgress = {"Enabled": progress})

    except ClientError as c:
        result += str(c)
        return result

    for event in r['Payload']:
            if 'Records' in event:
                result = ""
                records = event['Records']['Payload'].decode('utf-8')
                result += records
            if 'Progress' in event:
                print("progress")
                pprint.pprint(event['Progress'],width=1)
            if 'Stats' in event:
                print("Stats")
                pprint.pprint(event['Stats'],width=1)
            if 'End' in event:
                print("End")
                pprint.pprint(event['End'],width=1)

    return result




  run_s3select(
  "my_bucket",
  "my_csv_object",
  "select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3 from s3object where a3>100 and a3<300;")


S3 SELECT Responses
-------------------

Error Response
~~~~~~~~~~~~~~

::

   <?xml version="1.0" encoding="UTF-8"?>
   <Error>
     <Code>NoSuchKey</Code>
     <Message>The resource you requested does not exist</Message>
     <Resource>/mybucket/myfoto.jpg</Resource> 
     <RequestId>4442587FB7D0A2F9</RequestId>
   </Error>

Report Response
~~~~~~~~~~~~~~~
::

   HTTP/1.1 200
   <?xml version="1.0" encoding="UTF-8"?>
   <Payload>
      <Records>
         <Payload>blob</Payload>
      </Records>
      <Stats>
         <Details>
            <BytesProcessed>long</BytesProcessed>
            <BytesReturned>long</BytesReturned>
            <BytesScanned>long</BytesScanned>
         </Details>
      </Stats>
      <Progress>
         <Details>
            <BytesProcessed>long</BytesProcessed>
            <BytesReturned>long</BytesReturned>
            <BytesScanned>long</BytesScanned>
         </Details>
      </Progress>
      <Cont>
      </Cont>
      <End>
      </End>
   </Payload>

Response Description
~~~~~~~~~~~~~~~~~~~~

For CEPH S3 Select, responses can be messages of the following types:

**Records message**: Can contain a single record, partial records, or multiple records. Depending on the size of the result, a response can contain one or more of these messages.

**Error message**: Upon an error being detected, RGW returns 400 Bad Request, and a specific error message sends back to the client, according to its type.

**Continuation message**: Ceph S3 periodically sends this message to keep the TCP connection open.
These messages appear in responses at random. The client must detect the message type and process it accordingly.

**Progress message**: Ceph S3 periodically sends this message if requested. It contains information about the progress of a query that has started but has not yet been completed.  

**Stats message**: Ceph S3 sends this message at the end of the request. It contains statistics about the query.

**End message**: Indicates that the request is complete, and no more messages will be sent. You should not assume that request is complete until the client receives an End message.
