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
 
Basic Workflow
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

        
Basic Functionalities
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

  | Currently only part of `AWS select command <https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html>`_ is implemented, table bellow describes what is currently supported.
  | The following table describes the current implementation for s3-select functionalities:

+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Feature                         | Detailed        | Example                                                               |
+=================================+=================+=======================================================================+
| Arithmetic operators            | ^ * / + - ( )   | select (int(_1)+int(_2))*int(_9) from stdin;                          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 |                 | select ((1+2)*3.14) ^ 2 from stdin;                                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Compare operators               | > < >= <= == != | select _1,_2 from stdin where (int(1)+int(_3))>int(_5);               |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| logical operator                | AND OR          | select count(*) from stdin where int(1)>123 and int(_5)<200;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| casting operator                | int(expression) | select int(_1),int( 1.2 + 3.4) from stdin;                            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 |float(expression)| select float(1.2) from stdin;                                         |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
|                                 | timestamp(...)  | select timestamp("1999:10:10-12:23:44") from stdin;                   |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | sum             | select sum(int(_1)) from stdin;                                       |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | min             | select min( int(_1) * int(_5) ) from stdin;                           |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | max             | select max(float(_1)),min(int(_5)) from stdin;                        |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Aggregation Function            | count           | select count(*) from stdin where (int(1)+int(_3))>int(_5);            |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | extract         | select count(*) from stdin where                                      |
|                                 |                 | extract("year",timestamp(_2)) > 1950                                  |    
|                                 |                 | and extract("year",timestamp(_1)) < 1960;                             |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | dateadd         | select count(0) from stdin where                                      |
|                                 |                 | datediff("year",timestamp(_1),dateadd("day",366,timestamp(_1))) == 1; |  
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | datediff        | select count(0) from stdin where                                      |  
|                                 |                 | datediff("month",timestamp(_1),timestamp(_2))) == 2;                  | 
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| Timestamp Functions             | utcnow          | select count(0) from stdin where                                      |
|                                 |                 | datediff("hours",utcnow(),dateadd("day",1,utcnow())) == 24 ;          |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| String Functions                | substr          | select count(0) from stdin where                                      |
|                                 |                 | int(substr(_1,1,4))>1950 and int(substr(_1,1,4))<1960;                |
+---------------------------------+-----------------+-----------------------------------------------------------------------+
| alias support                   |                 |  select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3                  | 
|                                 |                 |  from stdin where a3>100 and a3<300;                                  |
+---------------------------------+-----------------+-----------------------------------------------------------------------+

.. _null-handle:

NULL
~~~~
| NULL is a legit value in ceph-s3select systems similar to other DB systems, i.e. systems needs to handle the case where a value is NULL.
| The definition of NULL in our context, is missing/unknown, in that sense **NULL can not produce a value on ANY arithmetic operations** ( a + NULL will produce NULL value).
| The Same is with arithmetic comparison, **any comparison to NULL is NULL**, i.e. unknown.
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

S3-select Function Interfaces
-----------------------------

Timestamp Functions
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

    | ``to_string(timestamp, format_pattern)`` : returns a string representation of the input timestamp in the given input string format.

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

    | ``count()`` : return integer according to number of rows matching condition(if such exist).

    | ``sum(expression)`` : return a summary of expression per all rows matching condition(if such exist).

    | ``max(expression)`` : return the maximal result for all expressions matching condition(if such exist).

    | ``min(expression)`` : return the minimal result for all expressions matching condition(if such exist).

String Functions
~~~~~~~~~~~~~~~~

    | ``substr(string,from,to)`` : return a string extract from input string according to from,to inputs.

    | ``char_length`` : return a number of characters in string (``character_length`` does the same).

    | ``trim`` : trim ( [[``leading`` | ``trailing`` | ``both`` remove_chars] ``from``] string )
    | trims leading/trailing(or both) characters from target string, the default is blank character.

    | ``upper\lower`` : converts characters into lowercase/uppercase.

SQL Limit Operator
~~~~~~~~~~~~~~~~~~

    | The SQL LIMIT operator is used to limit the number of rows processed by the query.
    | Upon reaching the limit set by the user, the RGW stops fetching additional chunks.
    | TODO : add examples, for aggregation and non-aggregation queries.

Alias
~~~~~
    | **Alias** programming-construct is an essential part of s3-select language, it enables much better programming especially with objects containing many columns or in the case of complex queries.
    
    | Upon parsing the statement containing alias construct, it replaces alias with reference to correct projection column, on query execution time the reference is evaluated as any other expression.

    | There is a risk that self(or cyclic) reference may occur causing stack-overflow(endless-loop), for that concern upon evaluating an alias, it is validated for cyclic reference.
    
    | Alias also maintains a result cache, meaning that successive uses of a given alias do not evaluate the expression again.  The result is instead returned from the cache.

    | With each new row the cache is invalidated as the results may then differ.

Testing
~~~~~~~
    
    | ``s3select`` contains several testing frameworks which provide a large coverage for its functionalities.

    | (1) Tests comparison against a trusted engine, meaning,  C/C++ compiler is a trusted expression evaluator, 
    | since the syntax for arithmetical and logical expressions are identical (s3select compare to C) 
    | the framework runs equal expressions and validates their results.
    | A dedicated expression generator produces different sets of expressions per each new test session. 

    | (2) Compares results of queries whose syntax is different but which are semantically equivalent.
    | This kind of test validates that different runtime flows produce an identical result 
    | on each run with a different, random dataset.

    | For example, on a dataset which contains a random numbers(1-1000)
    | the following queries will produce identical results.
    | ``select count(*) from s3object where char_length(_3)=3;``
    | ``select count(*) from s3object where cast(_3 as int)>99 and cast(_3 as int)<1000;``

    | (3) Constant dataset, the conventional way of testing. A query is processing a constant dataset, its result is validated against constant results.   

Additional Syntax Support
~~~~~~~~~~~~~~~~~~~~~~~~~

    | S3select syntax supports table-alias ``select s._1 from s3object s where s._2 = ‘4’;``
    | 
    | S3select syntax supports case insensitive ``Select SUM(Cast(_1 as int)) FROM S3Object;``
    | 
    | S3select syntax supports statements without closing semicolon  ``select count(*) from s3object``


Sending Query to RGW
--------------------

   | Any HTTP client can send an ``s3-select`` request to RGW, which must be compliant with `AWS Request syntax <https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html#API_SelectObjectContent_RequestSyntax>`_.



   | When sending an ``s3-select`` request to RGW using AWS CLI, clients must follow `AWS command reference <https://docs.aws.amazon.com/cli/latest/reference/s3api/select-object-content.html>`_.
   | Below is an example:

::

 aws --endpoint-url http://localhost:8000 s3api select-object-content 
  --bucket {BUCKET-NAME}  
  --expression-type 'SQL'     
  --input-serialization 
  '{"CSV": {"FieldDelimiter": "," , "QuoteCharacter": "\"" , "RecordDelimiter" : "\n" , "QuoteEscapeCharacter" : "\\" , "FileHeaderInfo": "USE" }, "CompressionType": "NONE"}' 
  --output-serialization '{"CSV": {}}' 
  --key {OBJECT-NAME} 
  --expression "select count(0) from stdin where int(_1)<10;" output.csv

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

| **AWS CLI example**

   | aws s3api select-object-content \
   | --bucket "mybucket" \
   | --key keyfile1 \
   | --expression "SELECT * FROM s3object s" \
   | --expression-type 'SQL' \
   | --request-progress '{"Enabled": false}' \
   | --input-serialization '{"CSV": {"FieldDelimiter": ","}, "CompressionType": "NONE"}' \
   | --output-serialization '{"CSV": {"FieldDelimiter": ":", "RecordDelimiter":"\\t", "QuoteFields": "ALWAYS"}}' /dev/stdout
   | 
   | **QuoteFields** -> (string)
   | Indicates whether to use quotation marks around output fields.
   | **ALWAYS**: Always use quotation marks for output fields.
   | **ASNEEDED** (not implemented): Use quotation marks for output fields when needed.
   |
   | **RecordDelimiter** -> (string)
   | A single character is used to separate individual records in the output. Instead of the default value, you can specify an        
   | arbitrary delimiter.
   | 
   | **FieldDelimiter** -> (string)
   | The value used to separate individual fields in a record. You can specify an arbitrary delimiter.

Scan Range Option
~~~~~~~~~~~~~~~~~

   | The scan range option to AWS-CLI enables the client to scan and process only a selected part of the object. 
   | This option reduces input/output operations and bandwidth by skipping parts of the object that are not of interest.
   | TODO : different data-sources (CSV, JSON, Parquet)

CSV Parsing Behavior
--------------------

    | The ``s3-select`` engine contains a CSV parser, which parses s3-objects as follows.   
    | - Each row ends with ``row-delimiter``.
    | - ``field-separator`` separates adjacent columns, successive instances of ``field separator`` define a NULL column.
    | - ``quote-character`` overrides ``field separator``, meaning that ``field separator`` is treated like any character between quotes.
    | - ``escape character`` disables interpretation of special characters, except for ``row delimiter``.
    
    | Below are examples of CSV parsing rules.

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
----

         | A JSON reader has been integrated with the ``s3select-engine``, which allows the client to use SQL statements to scan and extract information from JSON documents. 
         | It should be noted that the data readers and parsers for CSV, Parquet, and JSON documents are separated from the SQL engine itself, so all of these readers use the same SQL engine.

         | It's important to note that values in a JSON document can be nested in various ways, such as within objects or arrays.
         | These objects and arrays can be nested within each other without any limitations.
         | When using SQL to query a specific value in a JSON document, the client must specify the location of the value
         | via a path in the SELECT statement.

         | The SQL engine processes the SELECT statement in a row-based fashion.
         | It uses the columns specified in the statement to perform its projection calculation, and each row contains values for these columns.
         | In other words, the SQL engine processes each row one at a time (and aggregates results), using the values in the columns to perform SQL calculations.
         | However, the generic structure of a JSON document does not have a row-and-column structure like CSV or Parquet.
         | Instead, it is the SQL statement itself that defines the rows and columns when querying a JSON document.

         | When querying JSON documents using SQL, the FROM clause in the SELECT statement defines the row boundaries.
         | A row in a JSON document should be similar to how the row delimiter is used to define rows when querying CSV objects, and how row groups are used to define rows when querying Parquet objects.
         | The statement "SELECT ... FROM s3object[*].aaa.bb.cc" instructs the reader to search for the path "aaa.bb.cc" and defines the row boundaries based on the occurrence of this path.
         | A row begins when the reader encounters the path, and it ends when the reader exits the innermost part of the path, which in this case is the object "cc".

         | NOTE : The semantics of querying JSON document may change and may not be the same as the current methodology described.

         | TODO : relevant example for object and array values.

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
  "select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3 from stdin where a3>100 and a3<300;")

