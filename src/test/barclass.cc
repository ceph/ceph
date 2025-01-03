


#include <iostream>
#include <string.h>
#include <stdlib.h>

#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(bar)

cls_handle_t h_class;

cls_method_handle_t h_foo;

int foo_method(cls_method_context_t ctx, char *indata, int datalen,
				 char **outdata, int *outdatalen)
{
   int i;

   cls_log("hello world, this is bar");
   cls_log("indata=%s", indata);

   *outdata = (char *)malloc(128);
   for (i=0; i<strlen(indata) + 1; i++) {
     if (indata[i] == '0') {
       (*outdata)[i] = '*';
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
   cls_log("Loaded bar class!");

   cls_register("bar", &h_class);
   cls_register_method(h_class, "bar", foo_method, &h_foo);

   return;
}

