import pytest
import random
import string
import re
import json
from botocore.exceptions import ClientError
from botocore.exceptions import EventStreamError

import uuid
import warnings
import traceback

from . import (
    configfile,
    setup_teardown,
    get_client,
    get_new_bucket_name
    )

import logging
logging.basicConfig(level=logging.INFO)

import collections
collections.Callable = collections.abc.Callable

region_name = ''

# recurssion function for generating arithmetical expression 
def random_expr(depth):
    # depth is the complexity of expression 
    if depth==1 :
        return str(int(random.random() * 100) + 1)+".0"
    return '(' + random_expr(depth-1) + random.choice(['+','-','*','/']) + random_expr(depth-1) + ')'


def generate_s3select_where_clause(bucket_name,obj_name):

    a=random_expr(4)
    b=random_expr(4)
    s=random.choice([ '<','>','=','<=','>=','!=' ])

    try:
        eval( a )
        eval( b )
    except ZeroDivisionError:
        return

    # generate s3select statement using generated randome expression
    # upon count(0)>0 it means true for the where clause expression
    # the python-engine {eval( conditional expression )} should return same boolean result.
    s3select_stmt =  "select count(0) from s3object where " + a + s + b + ";"

    res = remove_xml_tags_from_result( run_s3select(bucket_name,obj_name,s3select_stmt) ).replace(",","")

    if  s == '=':
        s = '=='

    s3select_assert_result(int(res)>0 , eval( a + s + b ))

def generate_s3select_expression_projection(bucket_name,obj_name):

        # generate s3select statement using generated randome expression
        # statement return an arithmetical result for the generated expression.
        # the same expression is evaluated by python-engine, result should be close enough(Epsilon)
        
        e = random_expr( 4 )

        try:
            eval( e )
        except ZeroDivisionError:
            return

        if eval( e ) == 0:
            return

        res = remove_xml_tags_from_result( run_s3select(bucket_name,obj_name,"select " + e + " from s3object;",) ).replace(",","")

        # accuracy level 
        epsilon = float(0.00001) 

        # both results should be close (epsilon)
        assert(  abs(float(res.split("\n")[0]) - eval(e)) < epsilon )

@pytest.mark.s3select
def get_random_string():

    return uuid.uuid4().hex[:6].upper()

@pytest.mark.s3select
def test_generate_where_clause():

    # create small csv file for testing the random expressions
    single_line_csv = create_random_csv_object(1,1)
    bucket_name = get_new_bucket_name()
    obj_name = get_random_string() #"single_line_csv.csv"
    upload_object(bucket_name,obj_name,single_line_csv)
       
    for _ in range(100): 
        generate_s3select_where_clause(bucket_name,obj_name)


@pytest.mark.s3select
def test_generate_projection():

    # create small csv file for testing the random expressions
    single_line_csv = create_random_csv_object(1,1)
    bucket_name = get_new_bucket_name()
    obj_name = get_random_string() #"single_line_csv.csv"
    upload_object(bucket_name,obj_name,single_line_csv)
       
    for _ in range(100): 
        generate_s3select_expression_projection(bucket_name,obj_name)

def s3select_assert_result(a,b):
    if type(a) == str:
        a_strip = a.strip()
        b_strip = b.strip()
        if a=="" and b=="":
            warnings.warn(UserWarning("{}".format("both results are empty, it may indicates a wrong input, please check the test input")))
            ## print the calling function that created the empty result.
            stack = traceback.extract_stack(limit=2)
            formatted_stack = traceback.format_list(stack)[0]
            warnings.warn(UserWarning("{}".format(formatted_stack)))
            return True
        assert a_strip != ""
        assert b_strip != ""
    else:
        if a=="" and b=="":
            warnings.warn(UserWarning("{}".format("both results are empty, it may indicates a wrong input, please check the test input")))
            ## print the calling function that created the empty result.
            stack = traceback.extract_stack(limit=2)
            formatted_stack = traceback.format_list(stack)[0]
            warnings.warn(UserWarning("{}".format(formatted_stack)))
            return True
        assert a != ""
        assert b != ""
    assert True

def create_csv_object_for_datetime(rows,columns):
        result = ""
        for _ in range(rows):
            row = ""
            for _ in range(columns):
                row = row + "{}{:02d}{:02d}T{:02d}{:02d}{:02d}Z,".format(random.randint(0,100)+1900,random.randint(1,12),random.randint(1,28),random.randint(0,23),random.randint(0,59),random.randint(0,59),)
            result += row + "\n"

        return result

def create_random_csv_object(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                row = row + "{}{}".format(random.randint(0,1000),col_delim)
            result += row + record_delim

        return result

def create_random_csv_object_string(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                if random.randint(0,9) == 5:
                    row = row + "{}{}".format(''.join(random.choice(string.ascii_letters) for m in range(10)) + "aeiou",col_delim)
                else:
                    row = row + "{}{}".format(''.join("cbcd" + random.choice(string.ascii_letters) for m in range(10)) + "vwxyzzvwxyz" ,col_delim)
                
            result += row + record_delim

        return result

def create_random_csv_object_trim(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                if random.randint(0,5) == 2:
                    row = row + "{}{}".format(''.join("   aeiou    ") ,col_delim)
                else:
                    row = row + "{}{}".format(''.join("abcd") ,col_delim)


                
            result += row + record_delim

        return result

def create_random_csv_object_escape(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                if random.randint(0,9) == 5:
                    row = row + "{}{}".format(''.join("_ar") ,col_delim)
                else:
                    row = row + "{}{}".format(''.join("aeio_")  ,col_delim)
                
            result += row + record_delim

        return result

def create_random_csv_object_null(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = ""
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            for _ in range(columns):
                if random.randint(0,5) == 2:
                    row = row + "{}{}".format(''.join("") ,col_delim)
                else:
                    row = row + "{}{}".format(''.join("abc") ,col_delim)
                
            result += row + record_delim

        return result

def create_random_json_object(rows,columns,col_delim=",",record_delim="\n",csv_schema=""):
        result = "{\"root\" : ["
        result += record_delim
        if len(csv_schema)>0 :
            result = csv_schema + record_delim

        for _ in range(rows):
            row = ""
            num = 0
            row += "{"
            for _ in range(columns):
                num += 1
                row = row + "\"c" + str(num) + "\"" + ": " "{}{}".format(random.randint(0,1000),col_delim)
            row = row[:-1]
            row += "}"
            row += ","
            result += row + record_delim
        
        result = result[:-2]  
        result += record_delim
        result += "]" + "}"

        return result

def csv_to_json(obj, field_split=",",row_split="\n",csv_schema=""):
    result = "{\"root\" : ["
    result += row_split
    if len(csv_schema)>0 :
        result = csv_schema + row_split
    
    for rec in obj.split(row_split):
        row = ""
        num = 0
        row += "{"
        for col in rec.split(field_split):
            if col == "":
                break
            num += 1
            row = row + "\"c" + str(num) + "\"" + ": " "{}{}".format(col,field_split)
        row = row[:-1]
        row += "}"
        row += ","
        result += row + row_split
        
    result = result[:-5]  
    result += row_split
    result += "]" + "}"

    return result

def upload_object(bucket_name,new_key,obj):

        client = get_client()
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key=new_key, Body=obj)

        # validate uploaded object
        c2 = get_client()
        response = c2.get_object(Bucket=bucket_name, Key=new_key)
        assert response['Body'].read().decode('utf-8') == obj, 's3select error[ downloaded object not equal to uploaded objecy'

def run_s3select(bucket,key,query,column_delim=",",row_delim="\n",quot_char='"',esc_char='\\',csv_header_info="NONE", progress = False):

    s3 = get_client()
    result = ""
    result_status = {}

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

    if progress == False:

        try:
            for event in r['Payload']:
                if 'Records' in event:
                    records = event['Records']['Payload'].decode('utf-8')
                    result += records

        except EventStreamError as c:
            result = str(c)
            return result
        
    else:
            result = []
            max_progress_scanned = 0
            for event in r['Payload']:
                if 'Records' in event:
                    records = event['Records']
                    result.append(records.copy())
                if 'Progress' in event:
                    if(event['Progress']['Details']['BytesScanned'] > max_progress_scanned):
                        max_progress_scanned = event['Progress']['Details']['BytesScanned']
                        result_status['Progress'] = event['Progress']

                if 'Stats' in event:
                    result_status['Stats'] = event['Stats']
                if 'End' in event:
                    result_status['End'] = event['End']


    if progress == False:
        return result
    else:
        return result,result_status

def run_s3select_output(bucket,key,query, quot_field, op_column_delim = ",", op_row_delim = "\n",  column_delim=",", op_quot_char = '"', op_esc_char = '\\', row_delim="\n",quot_char='"',esc_char='\\',csv_header_info="NONE"):

    s3 = get_client()

    r = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        InputSerialization = {"CSV": {"RecordDelimiter" : row_delim, "FieldDelimiter" : column_delim,"QuoteEscapeCharacter": esc_char, "QuoteCharacter": quot_char, "FileHeaderInfo": csv_header_info}, "CompressionType": "NONE"},
        OutputSerialization = {"CSV": {"RecordDelimiter" : op_row_delim, "FieldDelimiter" : op_column_delim, "QuoteCharacter" : op_quot_char, "QuoteEscapeCharacter" : op_esc_char, "QuoteFields" : quot_field}},
        Expression=query,)
    
    result = ""
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            result += records
    
    return result

def run_s3select_json(bucket,key,query, op_row_delim = "\n"):

    s3 = get_client()

    r = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        InputSerialization = {"JSON": {"Type": "DOCUMENT"}},
        OutputSerialization = {"JSON": {}},
        Expression=query,)
    #Record delimiter optional in output serialization
    
    result = ""
    for event in r['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            result += records
    
    return result

def remove_xml_tags_from_result(obj):
    result = ""
    for rec in obj.split("\n"):
        if(rec.find("Payload")>0 or rec.find("Records")>0):
            continue
        result += rec + "\n" # remove by split

    result_strip= result.strip()
    x = bool(re.search("^failure.*$", result_strip))
    if x:
        logging.info(result)
    assert x == False

    return result

def create_list_of_int(column_pos,obj,field_split=",",row_split="\n"):
    
    list_of_int = [] 
    for rec in obj.split(row_split):
        col_num = 1
        if ( len(rec) == 0):
            continue
        for col in rec.split(field_split):
            if (col_num == column_pos):
                list_of_int.append(int(col))
            col_num+=1

    return list_of_int

@pytest.mark.s3select
def test_count_operation():
    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()
    num_of_rows = 1234
    obj_to_load = create_random_csv_object(num_of_rows,10)
    upload_object(bucket_name,csv_obj_name,obj_to_load)
    res = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object;") ).replace(",","")

    s3select_assert_result( num_of_rows, int( res ))

@pytest.mark.s3select
def test_count_json_operation():
    json_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    num_of_rows = 1
    obj_to_load = create_random_json_object(num_of_rows,10)
    upload_object(bucket_name,json_obj_name,obj_to_load)
    res = remove_xml_tags_from_result(run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*];"))
    s3select_assert_result( 1,  int(res))

    res = remove_xml_tags_from_result(run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root;"))
    s3select_assert_result( 1,  int(res))

    obj_to_load = create_random_json_object(3,10)
    upload_object(bucket_name,json_obj_name,obj_to_load)
    res = remove_xml_tags_from_result(run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root;"))
    s3select_assert_result( 3,  int(res))

@pytest.mark.s3select
def test_json_column_sum_min_max():
    csv_obj = create_random_csv_object(10000,10)

    json_obj = csv_to_json(csv_obj);

    json_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,json_obj_name,json_obj)
    
    json_obj_name_2 = get_random_string()
    bucket_name_2 = "testbuck2"
    upload_object(bucket_name_2,json_obj_name_2,json_obj)
    
    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select min(_1.c1) from s3object[*].root;")  ).replace(",","")
    list_int = create_list_of_int( 1 , csv_obj )
    res_target = min( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select min(_1.c4) from s3object[*].root;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = min( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select avg(_1.c6) from s3object[*].root;")  ).replace(",","")
    list_int = create_list_of_int( 6 , csv_obj )
    res_target = float(sum(list_int ))/10000

    s3select_assert_result( float(res_s3select), float(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select max(_1.c4) from s3object[*].root;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = max( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select max(_1.c7) from s3object[*].root;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = max( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select sum(_1.c4) from s3object[*].root;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = sum( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select sum(_1.c7) from s3object[*].root;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = sum( list_int )

    s3select_assert_result(  int(res_s3select) , int(res_target) )

    # the following queries, validates on *random* input an *accurate* relation between condition result,sum operation and count operation.
    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name_2,json_obj_name_2,"select count(0),sum(_1.c1),sum(_1.c2) from s3object[*].root where (_1.c1-_1.c2) = 2;" ) )
    count,sum1,sum2 = res_s3select.split(",")

    s3select_assert_result( int(count)*2 , int(sum1)-int(sum2 ) )

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select count(0),sum(_1.c1),sum(_1.c2) from s3object[*].root where (_1.c1-_1.c2) = 4;" ) ) 
    count,sum1,sum2 = res_s3select.split(",")

    s3select_assert_result( int(count)*4 , int(sum1)-int(sum2) )

@pytest.mark.s3select
def test_json_nullif_expressions():

    json_obj = create_random_json_object(10000,10)

    json_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,json_obj_name,json_obj)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root where nullif(_1.c1,_1.c2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root where _1.c1 = _1.c2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select (nullif(_1.c1,_1.c2) is null) from s3object[*].root ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select (_1.c1 = _1.c2) from s3object[*].root  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root where not nullif(_1.c1,_1.c2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root where _1.c1 != _1.c2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select (nullif(_1.c1,_1.c2) is not null) from s3object[*].root ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select (_1.c1 != _1.c2) from s3object[*].root  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root where  nullif(_1.c1,_1.c2) = _1.c1 ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select_json(bucket_name,json_obj_name,"select count(0) from s3object[*].root where _1.c1 != _1.c2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)


@pytest.mark.s3select
def test_column_sum_min_max():
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()
    
    upload_object(bucket_name,csv_obj_name,csv_obj)
    
    csv_obj_name_2 = get_random_string()
    bucket_name_2 = "testbuck2"
    upload_object(bucket_name_2,csv_obj_name_2,csv_obj)
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_1)) from s3object;")  ).replace(",","")
    list_int = create_list_of_int( 1 , csv_obj )
    res_target = min( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_4)) from s3object;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = min( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select avg(int(_6)) from s3object;")  ).replace(",","")
    list_int = create_list_of_int( 6 , csv_obj )
    res_target = float(sum(list_int ))/10000

    s3select_assert_result( float(res_s3select), float(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select max(int(_4)) from s3object;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = max( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select max(int(_7)) from s3object;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = max( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select sum(int(_4)) from s3object;")  ).replace(",","")
    list_int = create_list_of_int( 4 , csv_obj )
    res_target = sum( list_int )

    s3select_assert_result( int(res_s3select), int(res_target))
    
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select sum(int(_7)) from s3object;")  ).replace(",","")
    list_int = create_list_of_int( 7 , csv_obj )
    res_target = sum( list_int )

    s3select_assert_result(  int(res_s3select) , int(res_target) )

    # the following queries, validates on *random* input an *accurate* relation between condition result,sum operation and count operation.
    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name_2,csv_obj_name_2,"select count(0),sum(int(_1)),sum(int(_2)) from s3object where (int(_1)-int(_2)) = 2;" ) )
    count,sum1,sum2 = res_s3select.split(",")

    s3select_assert_result( int(count)*2 , int(sum1)-int(sum2 ) )

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0),sum(int(_1)),sum(int(_2)) from s3object where (int(_1)-int(_2)) = 4;" ) ) 
    count,sum1,sum2 = res_s3select.split(",")

    s3select_assert_result( int(count)*4 , int(sum1)-int(sum2) )

@pytest.mark.s3select
def test_nullif_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where nullif(_1,_2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where _1 = _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select (nullif(_1,_2) is null) from s3object ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select (_1 = _2) from s3object  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where not nullif(_1,_2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where _1 != _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select (nullif(_1,_2) is not null) from s3object ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select (_1 != _2) from s3object  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where  nullif(_1,_2) = _1 ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where _1 != _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    csv_obj = create_random_csv_object_null(10000,10)

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(*) from s3object where nullif(_1,null) is null;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(*) from s3object where _1 is null;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select (nullif(_1,null) is null) from s3object;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select (_1 is null) from s3object;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

@pytest.mark.s3select
def test_nulliftrue_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where (nullif(_1,_2) is null) = true ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where _1 = _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where not (nullif(_1,_2) is null) = true ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where _1 != _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where (nullif(_1,_2) = _1 = true) ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object where _1 != _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_nullif, res_s3select)

@pytest.mark.s3select
def test_is_not_null_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_null = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(*) from s3object where nullif(_1,_2) is not null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(*) from s3object where _1 != _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_null, res_s3select)

    res_s3select_null = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(*) from s3object where (nullif(_1,_1) and _1 = _2) is not null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(*) from s3object where _1 != _2  ;")  ).replace("\n","")

    s3select_assert_result( res_s3select_null, res_s3select)

@pytest.mark.s3select
def test_lowerupper_expressions():

    csv_obj = create_random_csv_object(1,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select lower("AB12cd$$") from s3object ;')  ).replace("\n","")

    s3select_assert_result( res_s3select, "ab12cd$$")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select upper("ab12CD$$") from s3object ;')  ).replace("\n","")

    s3select_assert_result( res_s3select, "AB12CD$$")

@pytest.mark.s3select
def test_in_expressions():

    # purpose of test: engine is process correctly several projections containing aggregation-functions
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) in(1);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) = 1;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_1) in(1)) from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_1) = 1) from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) in(1,0);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) = 1 or int(_1) = 0;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_1) in(1,0)) from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_1) = 1 or int(_1) = 0) from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where int(_2) in(1,0,2);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where int(_2) = 1 or int(_2) = 0 or int(_2) = 2;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_2) in(1,0,2)) from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_2) = 1 or int(_2) = 0 or int(_2) = 2) from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where int(_2)*2 in(int(_3)*2,int(_4)*3,int(_5)*5);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where int(_2)*2 = int(_3)*2 or int(_2)*2 = int(_4)*3 or int(_2)*2 = int(_5)*5;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_2)*2 in(int(_3)*2,int(_4)*3,int(_5)*5)) from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_2)*2 = int(_3)*2 or int(_2)*2 = int(_4)*3 or int(_2)*2 = int(_5)*5) from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where character_length(_1) = 2 and substring(_1,2,1) in ("3");')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where _1 like "_3";')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (character_length(_1) = 2 and substring(_1,2,1) in ("3")) from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (_1 like "_3") from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

@pytest.mark.s3select
def test_true_false_in_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    ## 1,2 must exist in first/second column (to avoid empty results)
    csv_obj = csv_obj + "1,2,,,,,,,,,,\n"

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where (int(_1) in(1)) = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) = 1;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where (int(_1) in(1,0)) = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) = 1 or int(_1) = 0;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where (int(_2) in(1,0,2)) = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where int(_2) = 1 or int(_2) = 0 or int(_2) = 2;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where (int(_2)*2 in(int(_3)*2,int(_4)*3,int(_5)*5)) = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from s3object where int(_2)*2 = int(_3)*2 or int(_2)*2 = int(_4)*3 or int(_2)*2 = int(_5)*5;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where (character_length(_1) = 2) = true and (substring(_1,2,1) in ("3")) = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where _1 like "_3";')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (int(_1) in (1,2,0)) as a1 from s3object where a1 = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select \"true\"from s3object where (int(_1) in (1,0,2)) ;')).replace("\n","")

    s3select_assert_result( res_s3select_in, res_s3select )  

@pytest.mark.s3select
def test_like_expressions():

    csv_obj = create_random_csv_object_string(1000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _1 like "%aeio%";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,11,4) = "aeio" ;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select  (_1 like "%aeio%") from s3object ;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select (substring(_1,11,4) = "aeio") from s3object ;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _1 like "cbcd%";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,1,4) = "cbcd";')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _1 like "%aeio%" like;')).replace("\n","")

    find_like = res_s3select_like.find("UnsupportedSyntax")

    assert int(find_like) >= 0

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (_1 like "cbcd%") from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select (substring(_1,1,4) = "cbcd") from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _3 like "%y[y-z]";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_3,char_length(_3),1) between "y" and "z" and substring(_3,char_length(_3)-1,1) = "y";')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (_3 like "%y[y-z]") from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select (substring(_3,char_length(_3),1) between "y" and "z" and substring(_3,char_length(_3)-1,1) = "y") from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _2 like "%yz";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_2,char_length(_2),1) = "z" and substring(_2,char_length(_2)-1,1) = "y";')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (_2 like "%yz") from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select (substring(_2,char_length(_2),1) = "z" and substring(_2,char_length(_2)-1,1) = "y") from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _3 like "c%z";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_3,char_length(_3),1) = "z" and substring(_3,1,1) = "c";')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (_3 like "c%z") from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select (substring(_3,char_length(_3),1) = "z" and substring(_3,1,1) = "c") from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _2 like "%xy_";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_2,char_length(_2)-1,1) = "y" and substring(_2,char_length(_2)-2,1) = "x";')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select (_2 like "%xy_") from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select (substring(_2,char_length(_2)-1,1) = "y" and substring(_2,char_length(_2)-2,1) = "x") from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

@pytest.mark.s3select
def test_truefalselike_expressions():

    csv_obj = create_random_csv_object_string(1000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where (_1 like "%aeio%") = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,11,4) = "aeio" ;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where (_1 like "cbcd%") = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,1,4) = "cbcd";')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where (_3 like "%y[y-z]") = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where (substring(_3,char_length(_3),1) between "y" and "z") = true and (substring(_3,char_length(_3)-1,1) = "y") = true;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where (_2 like "%yz") = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where (substring(_2,char_length(_2),1) = "z") = true and (substring(_2,char_length(_2)-1,1) = "y") = true;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where (_3 like "c%z") = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where (substring(_3,char_length(_3),1) = "z") = true and (substring(_3,1,1) = "c") = true;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

    res_s3select_like = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where (_2 like "%xy_") = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where (substring(_2,char_length(_2)-1,1) = "y") = true and (substring(_2,char_length(_2)-2,1) = "x") = true;')).replace("\n","")

    s3select_assert_result( res_s3select_like, res_s3select )

@pytest.mark.s3select
def test_nullif_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where nullif(_1,_2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where _1 = _2  ;")  ).replace("\n","")

    assert res_s3select_nullif == res_s3select

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where not nullif(_1,_2) is null ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where _1 != _2  ;")  ).replace("\n","")

    assert res_s3select_nullif == res_s3select

    res_s3select_nullif = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where  nullif(_1,_2) = _1 ;")  ).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select count(0) from stdin where _1 != _2  ;")  ).replace("\n","")

    assert res_s3select_nullif == res_s3select

@pytest.mark.s3select
def test_lowerupper_expressions():

    csv_obj = create_random_csv_object(1,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select lower("AB12cd$$") from stdin ;')  ).replace("\n","")

    assert res_s3select == "ab12cd$$"

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select upper("ab12CD$$") from stdin ;')  ).replace("\n","")

    assert res_s3select == "AB12CD$$"

@pytest.mark.s3select
def test_in_expressions():

    # purpose of test: engine is process correctly several projections containing aggregation-functions 
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) in(1);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) = 1;')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) in(1,0);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where int(_1) = 1 or int(_1) = 0;')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2) in(1,0,2);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2) = 1 or int(_2) = 0 or int(_2) = 2;')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2)*2 in(int(_3)*2,int(_4)*3,int(_5)*5);')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_2) from stdin where int(_2)*2 = int(_3)*2 or int(_2)*2 = int(_4)*3 or int(_2)*2 = int(_5)*5;')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where character_length(_1) = 2 and substring(_1,2,1) in ("3");')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from stdin where _1 like "_3";')).replace("\n","")

    assert res_s3select_in == res_s3select 

@pytest.mark.s3select
def test_like_expressions():

    csv_obj = create_random_csv_object_string(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _1 like "%aeio%";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_1,11,4) = "aeio" ;')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _1 like "cbcd%";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_1,1,4) = "cbcd";')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _3 like "%y[y-z]";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_3,character_length(_3),1) between "y" and "z" and substring(_3,character_length(_3)-1,1) = "y";')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _2 like "%yz";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_2,character_length(_2),1) = "z" and substring(_2,character_length(_2)-1,1) = "y";')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _3 like "c%z";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_3,character_length(_3),1) = "z" and substring(_3,1,1) = "c";')).replace("\n","")

    assert res_s3select_in == res_s3select 

    res_s3select_in = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from stdin where _2 like "%xy_";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from stdin where substring(_2,character_length(_2)-1,1) = "y" and substring(_2,character_length(_2)-2,1) = "x";')).replace("\n","")

    assert res_s3select_in == res_s3select 


@pytest.mark.s3select
def test_complex_expressions():

    # purpose of test: engine is process correctly several projections containing aggregation-functions 
    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select min(int(_1)),max(int(_2)),min(int(_3))+1 from s3object;")).replace("\n","")

    min_1 = min ( create_list_of_int( 1 , csv_obj ) )
    max_2 = max ( create_list_of_int( 2 , csv_obj ) )
    min_3 = min ( create_list_of_int( 3 , csv_obj ) ) + 1

    __res = "{},{},{}".format(min_1,max_2,min_3)
    
    # assert is according to radom-csv function 
    s3select_assert_result( res_s3select, __res )

    # purpose of test that all where conditions create the same group of values, thus same result
    res_s3select_substring = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from s3object where substring(_2,1,1) = "1" and char_length(_2) = 3;')).replace("\n","")

    res_s3select_between_numbers = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from s3object where int(_2)>=100 and int(_2)<200;')).replace("\n","")

    res_s3select_eq_modolu = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select min(int(_2)),max(int(_2)) from s3object where int(_2)/100 = 1 and character_length(_2) = 3;')).replace("\n","")

    s3select_assert_result( res_s3select_substring, res_s3select_between_numbers)

    s3select_assert_result( res_s3select_between_numbers, res_s3select_eq_modolu)
    
@pytest.mark.s3select
def test_alias():

    # purpose: test is comparing result of exactly the same queries , one with alias the other without.
    # this test is setting alias on 3 projections, the third projection is using other projection alias, also the where clause is using aliases
    # the test validate that where-clause and projections are executing aliases correctly, bare in mind that each alias has its own cache,
    # and that cache need to be invalidate per new row. 

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1) as a1, int(_2) as a2 , (a1+a2) as a3 from s3object where a3>100 and a3<300;")  ).replace(",","")

    res_s3select_no_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1),int(_2),int(_1)+int(_2) from s3object where (int(_1)+int(_2))>100 and (int(_1)+int(_2))<300;")  ).replace(",","")

    s3select_assert_result( res_s3select_alias, res_s3select_no_alias)


@pytest.mark.s3select
def test_alias_cyclic_refernce():

    number_of_rows = 10000
    
    # purpose of test is to validate the s3select-engine is able to detect a cyclic reference to alias.
    csv_obj = create_random_csv_object(number_of_rows,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select int(_1) as a1,int(_2) as a2, a1+a4 as a3, a5+a1 as a4, int(_3)+a3 as a5 from s3object;")  )

    find_res = res_s3select_alias.find("number of calls exceed maximum size, probably a cyclic reference to alias")
    
    assert int(find_res) >= 0 

@pytest.mark.s3select
def test_datetime():

    # purpose of test is to validate date-time functionality is correct,
    # by creating same groups with different functions (nested-calls) ,which later produce the same result 

    csv_obj = create_csv_object_for_datetime(10000,1)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object where extract(year from to_timestamp(_1)) > 1950 and extract(year from to_timestamp(_1)) < 1960;')  )

    res_s3select_substring = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object where int(substring(_1,1,4))>1950 and int(substring(_1,1,4))<1960;')  )

    s3select_assert_result( res_s3select_date_time, res_s3select_substring)

    res_s3select_date_time_to_string = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select cast(to_string(to_timestamp(_1), \'x\') as int) from  s3object;')  )

    res_s3select_date_time_extract = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select extract(timezone_hour from to_timestamp(_1)) from  s3object;')  )

    s3select_assert_result( res_s3select_date_time_to_string, res_s3select_date_time_extract )

    res_s3select_date_time_to_timestamp = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select extract(month from to_timestamp(_1)) from s3object where extract(month from to_timestamp(_1)) = 5;')  )

    res_s3select_substring = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select cast(substring(_1, 5, 2) as int) from s3object where _1 like \'____05%\';')  )

    s3select_assert_result( res_s3select_date_time_to_timestamp, res_s3select_substring)

@pytest.mark.s3select
def test_true_false_datetime():

    # purpose of test is to validate date-time functionality is correct,
    # by creating same groups with different functions (nested-calls) ,which later produce the same result 

    csv_obj = create_csv_object_for_datetime(10000,1)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object where (extract(year from to_timestamp(_1)) > 1950) = true and (extract(year from to_timestamp(_1)) < 1960) = true;')  )

    res_s3select_substring = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object where int(substring(_1,1,4))>1950 and int(substring(_1,1,4))<1960;')  )

    s3select_assert_result( res_s3select_date_time, res_s3select_substring)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object where  (date_diff(month,to_timestamp(_1),date_add(month,2,to_timestamp(_1)) ) = 2) = true;')  )

    res_s3select_count = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object;')  )

    s3select_assert_result( res_s3select_date_time, res_s3select_count)

    res_s3select_date_time = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object where (date_diff(year,to_timestamp(_1),date_add(day, 366 ,to_timestamp(_1))) = 1) = true ;')  )

    s3select_assert_result( res_s3select_date_time, res_s3select_count)

    # validate that utcnow is integrate correctly with other date-time functions 
    res_s3select_date_time_utcnow = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(0) from  s3object where (date_diff(hour,utcnow(),date_add(day,1,utcnow())) = 24) = true ;')  )

    s3select_assert_result( res_s3select_date_time_utcnow, res_s3select_count)

@pytest.mark.s3select
def test_csv_parser():

    # purpuse: test default csv values(, \n " \ ), return value may contain meta-char 
    # NOTE: should note that default meta-char for s3select are also for python, thus for one example double \ is mandatory

    csv_obj = r',first,,,second,third="c31,c32,c33",forth="1,2,3,4",fifth=my_string=\"any_value\" \, my_other_string=\"aaaa\,bbb\" ,' + "\n"
    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    # return value contain comma{,}
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _6 from s3object;")  ).replace("\n","")
    s3select_assert_result( res_s3select_alias, 'third=c31,c32,c33')

    # return value contain comma{,}
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _7 from s3object;")  ).replace("\n","")
    s3select_assert_result( res_s3select_alias, 'forth=1,2,3,4')

    # return value contain comma{,}{"}, escape-rule{\} by-pass quote{"} , the escape{\} is removed.
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _8 from s3object;")  ).replace("\n","")
    s3select_assert_result( res_s3select_alias, 'fifth=my_string="any_value" , my_other_string="aaaa,bbb" ')

    # return NULL as first token
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _1 from s3object;")  ).replace("\n","")
    s3select_assert_result( res_s3select_alias, 'null')

    # return NULL in the middle of line
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _3 from s3object;")  ).replace("\n","")
    s3select_assert_result( res_s3select_alias, 'null')

    # return NULL in the middle of line (successive)
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _4 from s3object;")  ).replace("\n","")
    s3select_assert_result( res_s3select_alias, 'null')

    # return NULL at the end line
    res_s3select_alias = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _9 from s3object;")  ).replace("\n","")
    s3select_assert_result( res_s3select_alias, 'null')

@pytest.mark.s3select
def test_csv_definition():

    number_of_rows = 10000

    #create object with pipe-sign as field separator and tab as row delimiter.
    csv_obj = create_random_csv_object(number_of_rows,10,"|","\t")

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)
   
    # purpose of tests is to parse correctly input with different csv defintions  
    res = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select count(0) from s3object;","|","\t") ).replace(",","")

    s3select_assert_result( number_of_rows, int(res))
    
    # assert is according to radom-csv function 
    # purpose of test is validate that tokens are processed correctly
    res_s3select = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select min(int(_1)),max(int(_2)),min(int(_3))+1 from s3object;","|","\t") ).replace("\n","")

    min_1 = min ( create_list_of_int( 1 , csv_obj , "|","\t") )
    max_2 = max ( create_list_of_int( 2 , csv_obj , "|","\t") )
    min_3 = min ( create_list_of_int( 3 , csv_obj , "|","\t") ) + 1

    __res = "{},{},{}".format(min_1,max_2,min_3)
    s3select_assert_result( res_s3select, __res )


@pytest.mark.s3select
def test_schema_definition():

    number_of_rows = 10000

    # purpose of test is to validate functionality using csv header info
    csv_obj = create_random_csv_object(number_of_rows,10,csv_schema="c1,c2,c3,c4,c5,c6,c7,c8,c9,c10")

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    # ignoring the schema on first line and retrieve using generic column number
    res_ignore = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select _1,_3 from s3object;",csv_header_info="IGNORE") ).replace("\n","")

    # using the scheme on first line, query is using the attach schema
    res_use = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select c1,c3 from s3object;",csv_header_info="USE") ).replace("\n","")
    # result of both queries should be the same
    s3select_assert_result( res_ignore, res_use)

    # using column-name not exist in schema
    res_multiple_defintion = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select c1,c10,int(c11) from s3object;",csv_header_info="USE") ).replace("\n","")

    assert ((res_multiple_defintion.find("alias {c11} or column not exist in schema")) >= 0)

    #find_processing_error = res_multiple_defintion.find("ProcessingTimeError")
    assert ((res_multiple_defintion.find("ProcessingTimeError")) >= 0)

    # alias-name is identical to column-name
    res_multiple_defintion = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select int(c1)+int(c2) as c4,c4 from s3object;",csv_header_info="USE") ).replace("\n","")

    assert ((res_multiple_defintion.find("multiple definition of column {c4} as schema-column and alias"))  >= 0)

@pytest.mark.s3select
def test_when_then_else_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select case when cast(_1 as int)>100 and cast(_1 as int)<200 then "(100-200)" when cast(_1 as int)>200 and cast(_1 as int)<300 then "(200-300)" else "NONE" end from s3object;')  ).replace("\n","")

    count1 = res_s3select.count("(100-200)")  

    count2 = res_s3select.count("(200-300)") 

    count3 = res_s3select.count("NONE")

    res = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where  cast(_1 as int)>100 and cast(_1 as int)<200  ;')  ).replace("\n","")

    res1 = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where  cast(_1 as int)>200 and cast(_1 as int)<300  ;')  ).replace("\n","")
    
    res2 = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where  cast(_1 as int)<=100 or cast(_1 as int)>=300 or cast(_1 as int)=200  ;')  ).replace("\n","")

    s3select_assert_result( str(count1) , res)

    s3select_assert_result( str(count2) , res1)

    s3select_assert_result( str(count3) , res2)

@pytest.mark.s3select
def test_coalesce_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where char_length(_3)>2 and char_length(_4)>2 and cast(substring(_3,1,2) as int) = cast(substring(_4,1,2) as int);')  ).replace("\n","")  

    res_null = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where cast(_3 as int)>99 and cast(_4 as int)>99 and coalesce(nullif(cast(substring(_3,1,2) as int),cast(substring(_4,1,2) as int)),7) = 7;' ) ).replace("\n","") 

    s3select_assert_result( res_s3select, res_null)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select coalesce(nullif(_5,_5),nullif(_1,_1),_2) from s3object;')  ).replace("\n","") 

    res_coalesce = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select coalesce(_2) from s3object;')  ).replace("\n","")   

    s3select_assert_result( res_s3select, res_coalesce)


@pytest.mark.s3select
def test_cast_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where cast(_3 as int)>999;')  ).replace("\n","")  

    res = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where char_length(_3)>3;')  ).replace("\n","") 

    s3select_assert_result( res_s3select, res)

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where cast(_3 as int)>99 and cast(_3 as int)<1000;')  ).replace("\n","")  

    res = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where char_length(_3)=3;')  ).replace("\n","") 

    s3select_assert_result( res_s3select, res)

@pytest.mark.s3select
def test_version():

    return
    number_of_rows = 1

    # purpose of test is to validate functionality using csv header info
    csv_obj = create_random_csv_object(number_of_rows,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_version = remove_xml_tags_from_result( run_s3select(bucket_name,csv_obj_name,"select version() from s3object;") ).replace("\n","")

    s3select_assert_result( res_version, "41.a," )

@pytest.mark.s3select
def test_trim_expressions():

    csv_obj = create_random_csv_object_trim(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(_1) = "aeiou";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1 from 4 for 5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(both from _1) = "aeiou";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(trailing from _1) = "   aeiou";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(leading from _1) = "aeiou    ";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(trim(leading from _1)) = "aeiou";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

@pytest.mark.s3select
def test_truefalse_trim_expressions():

    csv_obj = create_random_csv_object_trim(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(_1) = "aeiou" = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1 from 4 for 5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(both from _1) = "aeiou" = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(trailing from _1) = "   aeiou" = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(leading from _1) = "aeiou    " = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

    res_s3select_trim = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where trim(trim(leading from _1)) = "aeiou" = true;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,4,5) = "aeiou";')).replace("\n","")

    s3select_assert_result( res_s3select_trim, res_s3select )

@pytest.mark.s3select
def test_escape_expressions():

    csv_obj = create_random_csv_object_escape(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_escape = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _1 like "%_ar" escape "%";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,char_length(_1),1) = "r" and substring(_1,char_length(_1)-1,1) = "a" and substring(_1,char_length(_1)-2,1) = "_";')).replace("\n","")

    s3select_assert_result( res_s3select_escape, res_s3select )

    res_s3select_escape = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where _1 like "%aeio$_" escape "$";')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where substring(_1,1,5) = "aeio_";')).replace("\n","")

    s3select_assert_result( res_s3select_escape, res_s3select )

@pytest.mark.s3select
def test_case_value_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_case = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select case cast(_1 as int) when cast(_2 as int) then "case_1_1" else "case_2_2" end from s3object;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select case when cast(_1 as int) = cast(_2 as int) then "case_1_1" else "case_2_2" end from s3object;')).replace("\n","")

    s3select_assert_result( res_s3select_case, res_s3select )

@pytest.mark.s3select
def test_bool_cast_expressions():

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_cast = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select count(*) from s3object where cast(int(_1) as bool) = true ;')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name, 'select count(*) from s3object where cast(_1 as int) != 0 ;')).replace("\n","")

    s3select_assert_result( res_s3select_cast, res_s3select )

@pytest.mark.s3select
def test_progress_expressions():

    csv_obj = create_random_csv_object(1000000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    obj_size = len(csv_obj.encode('utf-8'))

    result_status = {}
    result_size = 0

    res_s3select_response,result_status = run_s3select(bucket_name,csv_obj_name,"select sum(int(_1)) from s3object;",progress = True)

    for rec in res_s3select_response:
        result_size += len(rec['Payload'])

    records_payload_size = result_size
   
    # To do: Validate bytes processed after supporting compressed data
    s3select_assert_result(obj_size, result_status['Progress']['Details']['BytesScanned'])
    s3select_assert_result(records_payload_size, result_status['Progress']['Details']['BytesReturned'])

    # stats response payload validation
    s3select_assert_result(obj_size, result_status['Stats']['Details']['BytesScanned'])
    s3select_assert_result(records_payload_size, result_status['Stats']['Details']['BytesReturned'])

    # end response
    s3select_assert_result({}, result_status['End'])

@pytest.mark.s3select
def test_output_serial_expressions():
    return # TODO fix test

    csv_obj = create_random_csv_object(10000,10)

    csv_obj_name = get_random_string()
    bucket_name = get_new_bucket_name()

    upload_object(bucket_name,csv_obj_name,csv_obj)

    res_s3select_1 = remove_xml_tags_from_result(  run_s3select_output(bucket_name,csv_obj_name,"select _1, _2 from s3object where nullif(_1,_2) is null ;", "ALWAYS")  ).replace("\n",",").replace(",","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,"select _1, _2 from s3object where _1 = _2 ;")  ).replace("\n",",")

    res_s3select_list = res_s3select.split(',')

    res_s3select_list.pop()

    res_s3select_final = (''.join('"' + item + '"' for item in res_s3select_list))

    s3select_assert_result( '""'+res_s3select_1+'""', res_s3select_final)


    res_s3select_in = remove_xml_tags_from_result(  run_s3select_output(bucket_name,csv_obj_name,'select int(_1) from s3object where (int(_1) in(int(_2)));', "ASNEEDED", '$', '#')).replace("\n","#") ## TODO why \n appears in output?

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) = int(_2);')).replace("\n","#")
    
    res_s3select_list = res_s3select.split('#')

    res_s3select_list.pop()

    res_s3select_final = (''.join(item + '#' for item in res_s3select_list))


    s3select_assert_result(res_s3select_in , res_s3select_final )


    res_s3select_quot = remove_xml_tags_from_result(  run_s3select_output(bucket_name,csv_obj_name,'select int(_1) from s3object where (int(_1) in(int(_2)));', "ALWAYS", '$', '#')).replace("\n","")

    res_s3select = remove_xml_tags_from_result(  run_s3select(bucket_name,csv_obj_name,'select int(_1) from s3object where int(_1) = int(_2);')).replace("\n","#")
    res_s3select_list = res_s3select.split('#')

    res_s3select_list.pop()

    res_s3select_final = (''.join('"' + item + '"' + '#' for item in res_s3select_list))

    s3select_assert_result( '""#'+res_s3select_quot+'""#', res_s3select_final )
