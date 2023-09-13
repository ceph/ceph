


#include <iostream>
#include <string.h>
#include <stdlib.h>

#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(foo)

cls_handle_t h_class;

cls_method_handle_t h_foo;

int foo_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   int i;

   cls_log("hello world, this is foo");
   cls_log("indata=%s", indata);

   *outdata = (char *)malloc(128);
   for (i=0; i<strlen(indata) + 1; i++) {
     if (indata[i] == '1') {
       (*outdata)[i] = 'I';
     } else {
       (*outdata)[i] = indata[i];
     }
   }
   *outdatalen = strlen(*outdata) + 1;
   cls_log("outdata=%s", *outdata);

   return 0;
}

void class_init()
{
   cls_log("Loaded foo class!");

   cls_register("foo", &h_class);
   cls_register_method(h_class, "foo", foo_method, &h_foo);

   return;
}

