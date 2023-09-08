===========================
External IAM Authorization
===========================

With External IAM Authorization, a RadosGateway can ask an external service to Allow/Deny/Pass on an incoming request.

Configuration
=============

The following configurable options are available::

    [client.radosgw.gateway]
    rgw_extern_iam_enabled = true
    rgw_extern_iam_addr = {HTTP URL of the External IAM service}
    rgw_extern_iam_unix_sock_path = {optional if the service is listening on unix socket}
    rgw_extern_iam_authorization_header = {HTTP Authorization header value}
    rgw_extern_iam_verify_ssl = {verify ssl certificate while connecting to the service}

API
===

The request contains Environment info, ARN, Action, subuser, and user data.
The ``env`` value might be different based on the request.

Example request::

    POST {rgw_extern_iam_addr} HTTP/1.1
    Host: {rgw_extern_iam_addr}
    Authorization: {rgw_extern_iam_authorization_header}
    Content-Type: application/json

    {
        "env": {
            "sts:authentication": "false",
            "aws:username": "testid",
            "aws:SourceIp": "127.0.0.1",
            "aws:PrincipalType": "User",
            "aws:EpochTime": "2023-09-08T17:13:52.178500486Z",
            "aws:CurrentTime": "1694193232",
        },
        "arn": "arn:aws:s3:::*",
        "action": "s3:ListAllMyBuckets",
        "subuser": "subid",
        "user": "testid",
    }

The expected response should be either ``Allow``, ``Deny``, or ``Pass`` with the key ``Effect``.
The response parser is case-sensitive.

Example Response::

    {"Effect": "Allow"}

Response Explanation
====================

* Responding with ``Deny`` will drop the request.
* Responding with ``Allow`` will allow the request regardless of ACLs if there were no other policies (like bucket policy, ...) denying the request.
* Responding with ``Pass`` will let the request be verified by ACLs if other policies (like bucket policy, ...) also reply with ``Pass`` (which means no policies were matching the request).
