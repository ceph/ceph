


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
cls_method_handle_t h_test;

int test_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   int i;
   MD5_CTX c;
   unsigned char *md;

   cls_log("acl test method");
   cls_log("indata=%.*s data_len=%d", datalen, indata, datalen);

#if 0
   *outdata = (char *)md;
   *outdatalen = MD5_DIGEST_LENGTH;
#endif
   cls_getxattr(ctx, "test", outdata, outdatalen);

   return 0;
}

void class_init()
{
   cls_log("Loaded acl class!");

   cls_register("acl", &h_class);
   cls_register_method(h_class, "test", CLS_METHOD_RD, test_method, &h_test);

   return;
}

