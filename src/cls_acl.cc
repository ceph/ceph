


#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <openssl/md5.h>
#include <openssl/sha.h>

#include "include/types.h"
#include "objclass/objclass.h"


CLS_VER(1,0)
CLS_NAME(acl)

cls_handle_t h_class;
cls_method_handle_t h_get;
cls_method_handle_t h_set;

int get_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   MD5_CTX c;

   cls_log("acl test method");
   cls_log("indata=%.*s data_len=%d", datalen, indata, datalen);

   cls_getxattr(ctx, "acls", outdata, outdatalen);

   return 0;
}

int set_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   MD5_CTX c;

   cls_log("acl test method");
   cls_log("indata=%.*s data_len=%d", datalen, indata, datalen);

   cls_setxattr(ctx, "acls", indata, datalen);

   return 0;
}

void __cls_init()
{
   cls_log("Loaded acl class!");

   cls_register("acl", &h_class);
   cls_register_method(h_class, "get", CLS_METHOD_RD, get_method, &h_get);
   cls_register_method(h_class, "set", CLS_METHOD_WR, set_method, &h_set);

   return;
}

