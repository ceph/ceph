===============================
Rados Gateway S3 API Compliance
===============================

.. warning::
	This document is a draft, it might not be accurate

----------------------
Naming code reference
----------------------

Here comes a BNF definition on how to name a feature in the code for referencing purpose : ::

    name ::= request_type "_" ( header | operation ) ( "_" header_option )?
    
    request_type ::= "req" | "res"
    
    header ::= string
    
    operation ::= method resource
    
    method ::= "GET" | "PUT" | "POST" | "DELETE" | "OPTIONS" | "HEAD"
    
    resource ::= string
    
    header_option ::= string

----------------------
Common Request Headers
----------------------

S3 Documentation reference : http://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonRequestHeaders.html

+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Header               | Supported? | Code Links                                                                                              | Tests links |
+======================+============+=========================================================================================================+=============+
| Authorization        | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1962 |             |
|                      |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L2051 |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Content-Length       | Yes        |                                                                                                         |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Content-Type         | Yes        |                                                                                                         |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Content-MD5          | Yes        | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1249      |             |
|                      |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1306      |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Date                 | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_auth_s3.cc#L164  |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Expect               | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest.cc#L1227    |             |
|                      |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L802  |             |
|                      |            | https://github.com/ceph/ceph/blob/76040d90f7eb9f9921a3b8dcd0f821ac2cd9c492/src/rgw/rgw_main.cc#L372     |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Host                 | ?          |                                                                                                         |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| x-amz-date           | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_auth_s3.cc#L169  |             |
|                      |            | should take precedence over DATE as mentionned here ->                                                  |             |
|                      |            | http://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonRequestHeaders.html                            |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| x-amz-security-token | No         |                                                                                                         |             |
+----------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+

-----------------------
Common Response Headers
-----------------------

S3 Documentation reference : http://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html

+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Header              | Supported? | Code Links                                                                                              | Tests links |
+=====================+============+=========================================================================================================+=============+
| Content-Length      | Yes        |                                                                                                         |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Connection          | ?          |                                                                                                         |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Date                | ?          |                                                                                                         |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| ETag                | Yes        | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1312      |             |
|                     |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1436      |             |
|                     |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L2222      |             |
|                     |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L118  |             |
|                     |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L268  |             |
|                     |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L516  |             |
|                     |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1336 |             |
|                     |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1486 |             |
|                     |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1548 |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Server              | No         |                                                                                                         |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| x-amz-delete-marker | No         |                                                                                                         |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| x-amz-id-2          | No         |                                                                                                         |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| x-amz-request-id    | Yes        | https://github.com/ceph/ceph/commit/b711e3124f8f73c17ebd19b38807a1b77f201e44                            |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| x-amz-version-id    | No         |                                                                                                         |             |
+---------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+

-------------------------
Operations on the Service
-------------------------

S3 Documentation reference : http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceOps.html

+------+-----------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Type | Operation | Supported? | Code links                                                                                              | Tests links |
+======+===========+============+=========================================================================================================+=============+
| GET  | Service   | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L2094 |             |
|      |           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1676 |             |
|      |           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L185  |             |
+------+-----------+------------+---------------------------------------------------------------------------------------------------------+-------------+

---------------------
Operations on Buckets
---------------------

S3 Documentation reference : http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketOps.html

+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| Type   | Operation              | Supported? | Code links                                                                                                 | Tests links |
+========+========================+============+============================================================================================================+=============+
| DELETE | Bucket                 | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1728    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/e91042171939b6bf82a56a1015c5cae792d228ad/src/rgw/rgw_rest_bucket.cc#L250 |             |
|        |                        |            | https://github.com/ceph/ceph/blob/e91042171939b6bf82a56a1015c5cae792d228ad/src/rgw/rgw_rest_bucket.cc#L212 |             |
|        |                        |            | https://github.com/ceph/ceph/blob/25948319c4d256c4aeb0137eb88947e54d14cc79/src/rgw/rgw_bucket.cc#L856      |             |
|        |                        |            | https://github.com/ceph/ceph/blob/25948319c4d256c4aeb0137eb88947e54d14cc79/src/rgw/rgw_bucket.cc#L513      |             |
|        |                        |            | https://github.com/ceph/ceph/blob/25948319c4d256c4aeb0137eb88947e54d14cc79/src/rgw/rgw_bucket.cc#L286      |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L461     |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket cors            | ?          | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1731    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1916         |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket lifecycle       | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket policy          | ?          |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket tagging         | ?          |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket website         | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket                 | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1676    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L185     |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket acl             | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1697    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1728         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1344    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket cors            | ?          | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1698    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1845         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/76040d90f7eb9f9921a3b8dcd0f821ac2cd9c492/src/rgw/rgw_main.cc#L345        |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket lifecycle       | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket location        | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket policy          | ?          | https://github.com/ceph/ceph/blob/e91042171939b6bf82a56a1015c5cae792d228ad/src/rgw/rgw_rest_bucket.cc#L232 |             |
|        |                        |            | https://github.com/ceph/ceph/blob/e91042171939b6bf82a56a1015c5cae792d228ad/src/rgw/rgw_rest_bucket.cc#L58  |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket logging         | ?          | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1695    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L287     |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket notification    | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket tagging         | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket Object versions | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket requestPayment  | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket versionning     | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket website         | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | List Multipart uploads | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1701    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest.cc#L877        |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L2355         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L2363         |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| HEAD   | Bucket                 | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1713    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1689    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L826          |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L834          |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket                 | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1725    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L382     |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L437     |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L901          |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L945          |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket acl             | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1721    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1354    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1373    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1739         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1753         |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket cors            | ?          | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1723    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1398    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1858         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1866         |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket lifecycle       | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket policy          | ?          |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket logging         | ?          |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket notification    | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket tagging         | ?          |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket requestPayment  | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket versionning     | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket website         | N0         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+

---------------------
Operations on Objects
---------------------

S3 Documentation reference : http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectOps.html

+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| Type    | Operation                 | Supported? | Code links                                                                                              | Tests links |
+=========+===========================+============+=========================================================================================================+=============+
| DELETE  | Object                    | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1796 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1516      |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1524      |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| DELETE  | Multiple objects          | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1739 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1616 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1626 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1641 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1667 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1516      |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1524      |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| GET     | Object                    | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1767 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L71   |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L397       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L424       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L497       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L562       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L626       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L641       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L706       |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| GET     | Object acl                | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| GET     | Object torrent            | No         |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| HEAD    | Object                    | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1777 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L71   |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L397       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L424       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L497       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L562       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L626       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L641       |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L706       |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| OPTIONS | Object                    | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1814 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1418 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1951      |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1968      |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1993      |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| POST    | Object                    | Yes        | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1742 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L631  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L694  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L700  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L707  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L759  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L771  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L781  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L795  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L929  |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1037 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1059 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/8a2eb18494005aa968b71f18121da8ebab48e950/src/rgw/rgw_rest_s3.cc#L1134 |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1344      |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1360      |             |
|         |                           |            | https://github.com/ceph/ceph/blob/b139a7cd34b4e203ab164ada7a8fa590b50d8b13/src/rgw/rgw_op.cc#L1365      |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| POST    | Object restore            | ?          |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Object                    | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Object acl                | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Object copy               | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Initate multipart upload  | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Upload Part               | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Upload Part copy          | ?          |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Complete multipart upload | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | Abort multipart upload    | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
| PUT     | List parts                | Yes        |                                                                                                         |             |
+---------+---------------------------+------------+---------------------------------------------------------------------------------------------------------+-------------+
