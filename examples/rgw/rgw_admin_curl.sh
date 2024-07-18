#!/usr/bin/env bash

show_help()
{
    echo "Usage: `basename $0` -a <access-key> -s <secret-key>" \
                 "-e <rgw-endpoint> -r <http-request>" \
                 "-p <admin-resource> -q \"<http-query-string>\"" 
    echo "   -a       Access key of rgw user"
    echo "   -s       Secret key of rgw user"
    echo "   -e       RGW endpoint in <ipaddr:port> format"
    echo "   -r       HTTP request type GET/PUT/DELETE"
    echo "   -p       RGW admin resource e.g user, bucket etc"
    echo "   -q       HTTP query string"
    echo "   -j       (Optional) Print output in pretty JSON format"
    echo "   Examples :"
    echo "     - To create rgw user"
    echo "       # `basename $0` -a ABCD1234EFGH5678IJ90" \
                   "-s klmnopqrstuvwxyz12345ABCD987654321efghij" \
                   "-e 10.0.0.1:8080 -r PUT -p user" \
                   "-q \"uid=admin&display-name=Administrator\""
    echo "     - To get rgw user info"
    echo "       # `basename $0` -a ABCD1234EFGH5678IJ90" \
                   "-s klmnopqrstuvwxyz12345ABCD987654321efghij" \
                   "-e 10.0.0.1:8080 -r GET -p user -q \"uid=admin\""
    echo "     - To list buckets"
    echo "       (List all buckets)"
    echo "       # `basename $0` -a ABCD1234EFGH5678IJ90" \
                   "-s klmnopqrstuvwxyz12345ABCD987654321efghij" \
                   "-e 10.0.0.1:8080 -r GET -p bucket"
    echo "       (For specific rgw user)"
    echo "       # `basename $0` -a ABCD1234EFGH5678IJ90" \
                   "-s klmnopqrstuvwxyz12345ABCD987654321efghij" \
                   "-e 10.0.0.1:8080 -r GET -p bucket -q \"uid=admin\""
    echo "     - To delete bucket"
    echo "       # `basename $0` -a ABCD1234EFGH5678IJ90" \
                   "-s klmnopqrstuvwxyz12345ABCD987654321efghij" \
                   "-e 10.0.0.1:8080 -r DELETE -p bucket -q \"bucket=foo\""
    echo "     - To delete rgw user"
    echo "       # `basename $0` -a ABCD1234EFGH5678IJ90" \
                   "-s klmnopqrstuvwxyz12345ABCD987654321efghij" \
                   "-e 10.0.0.1:8080 -r DELETE -p user -q \"uid=admin\""
    exit 1
}

access_key=""
secret_key=""
rgw_endpoint=""
http_request=""
admin_resource=""
http_query=""
use_jq=false

while getopts "a:s:e:r:p:q:j" opt; do
    case "$opt" in
    a)
        access_key=${OPTARG}
        ;;
    s)  secret_key=${OPTARG}
        ;;
    e)  rgw_endpoint=${OPTARG}
        ;;
    r)  http_request=${OPTARG}
        ;;
    p)  admin_resource=${OPTARG}
        ;;
    q)  http_query=${OPTARG}
        ;;
    j)  use_jq=true
        ;;
    *)
        show_help
        exit 1
        ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${access_key}" ] || [ -z "${secret_key}" ] || \
   [ -z "${rgw_endpoint}" ] || [ -z "${http_request}" ] || \
   [ -z "${admin_resource}" ] || [ -z "${http_query}" ]; then
    if [ "${http_request}" = "GET" ] && [ "${admin_resource}" = "bucket" ] && \
       [ -z "${http_query}" ]; then
      :
    else 
      show_help
    fi
fi

resource="/admin/${admin_resource}"
contentType="application/x-compressed-tar"
dateTime=`date -R -u`

headerToSign="${http_request}

${contentType}
${dateTime}
${resource}"

signature=`echo -en "$headerToSign" | \
           openssl sha1 -hmac ${secret_key} -binary | base64`

if "$use_jq";
then 
    curl -X ${http_request} -H "Content-Type: ${contentType}" -H "Date: ${dateTime}" \
         -H "Authorization: AWS ${access_key}:${signature}" -H "Host: ${rgw_endpoint}" \
         "http://${rgw_endpoint}${resource}?${http_query}" 2> /dev/null|jq "."
else
    curl -X ${http_request} -H "Content-Type: ${contentType}" -H "Date: ${dateTime}" \
         -H "Authorization: AWS ${access_key}:${signature}" -H "Host: ${rgw_endpoint}" \
         "http://${rgw_endpoint}${resource}?${http_query}"
fi
echo ""
