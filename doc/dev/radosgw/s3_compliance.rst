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
|                      |            | should take precedence over DATE as mentioned here ->                                                   |             |
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
| DELETE | Bucket                 | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L3477         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L2239    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket cors            | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L5723         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3526    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket lifecycle       | Yes        | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3414    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L5651         |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket policy          | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L7779         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L7761         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4405    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket tagging         | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L1247         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L597     |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| DELETE | Bucket website         | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L2807         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L2029    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket acl             | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L5337         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3303    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4317    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket cors            | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L5674         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3426    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4319    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket lifecycle       | Yes        | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3365    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3385    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket location        | Yes        | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L1802    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4300    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket policy          | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L7738         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L7719         |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket logging         | Yes        | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L1791    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4297    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket notification    | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket tagging         | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L1200         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L512     |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4329    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket Object versions | Yes        | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L1424    |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L1454    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket requestPayment  | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket versioning      | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L2644         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L1825    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | Bucket website         | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L2750         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L2039    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| GET    | List Multipart uploads | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L6421         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4323    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| HEAD   | Bucket                 | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L2848         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L2065    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket                 | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L3191         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L2215    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket acl             | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L5421         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3356    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket cors            | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L5692         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L3517    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket lifecycle       | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket policy          | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L7680         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L4377    |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket logging         | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket notification    | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket tagging         | Yes        | https://github.com/ceph/ceph/blob/45e8438b9950158f26e629e2ffa37e19e7abf592/src/rgw/rgw_op.cc#L1216         |             |
|        |                        |            | https://github.com/ceph/ceph/blob/b1be4bdf2cc968afb13448023d81a46e2eaa691f/src/rgw/rgw_rest_s3.cc#L588     |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket requestPayment  | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket versioning      | No         |                                                                                                            |             |
+--------+------------------------+------------+------------------------------------------------------------------------------------------------------------+-------------+
| PUT    | Bucket website         | No         |                                                                                                            |             |
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
