


#include <iostream>
#include <string.h>
#include <stdlib.h>

#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(test)

cls_handle_t h_class;

cls_method_handle_t h_foo;

int foo_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   int i, r;

   cls_log("hello world");
   cls_log("indata=%s", indata);

   *outdata = (char *)cls_alloc(128);
   for (i=0; i<strlen(indata) + 1; i++) {
     if (indata[i] == '0') {
       (*outdata)[i] = '*';
     } else {
       (*outdata)[i] = indata[i];
     }
   }
   *outdatalen = strlen(*outdata) + 1;
   cls_log("outdata=%s", *outdata);

   r = cls_call(ctx, "foo", "foo", *outdata, *outdatalen, outdata, outdatalen);

   return r;
}

static cls_deps_t depend[] = {{"foo", "1.0"}, {"bar", "1.0"}, {NULL, NULL}};

extern "C" cls_deps_t *class_deps()
{
   return depend;
};

void class_init()
{
   cls_log("Loaded class test!");

   cls_register("test", &h_class);
   cls_register_method(h_class, "foo", foo_method, &h_foo);

   return;
}

