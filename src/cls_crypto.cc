#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include <openssl/md5.h>
#include <openssl/sha.h>

#include "include/types.h"
#include "objclass/objclass.h"


CLS_VER(1,0)
CLS_NAME(crypto)

cls_handle_t h_class;

cls_method_handle_t h_md5;
cls_method_handle_t h_sha1;

int md5_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   MD5_CTX c;
   unsigned char *md;

   cls_log("md5 method");
   cls_log("indata=%.*s data_len=%d", datalen, indata, datalen);

   md = (unsigned char *)cls_alloc(MD5_DIGEST_LENGTH);
   if (!md)
     return -ENOMEM;

   MD5_Init(&c);
   MD5_Update(&c, indata, (unsigned long)datalen);
   MD5_Final(md,&c);

   *outdata = (char *)md;
   *outdatalen = MD5_DIGEST_LENGTH;

   return 0;
}

int sha1_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   SHA_CTX c;
   unsigned char *md;

   cls_log("sha1 method");
   cls_log("indata=%.*s data_len=%d", datalen, indata, datalen);

   md = (unsigned char *)cls_alloc(SHA_DIGEST_LENGTH);
   if (!md)
     return -ENOMEM;

   SHA1_Init(&c);
   SHA1_Update(&c, indata, (unsigned long)datalen);
   SHA1_Final(md,&c);

   *outdata = (char *)md;
   *outdatalen = SHA_DIGEST_LENGTH;

   return 0;
}

void __cls_init()
{
   cls_log("Loaded crypto class!");

   cls_register("crypto", &h_class);
   cls_register_method(h_class, "md5", CLS_METHOD_RD, md5_method, &h_md5);
   cls_register_method(h_class, "sha1", CLS_METHOD_RD, sha1_method, &h_sha1);

   return;
}

