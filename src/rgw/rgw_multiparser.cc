#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_multi.h"

using namespace std;
                                  
int main(int argc, char **argv) {
  RGWMultiXMLParser parser;

  if (!parser.init())
    exit(1);

  char buf[1024];

  for (;;) {
    int done;
    int len;

    len = fread(buf, 1, sizeof(buf), stdin);
    if (ferror(stdin)) {
      fprintf(stderr, "Read error\n");
      exit(-1);
    }
    done = feof(stdin);

    parser.parse(buf, len, done);

    if (done)
      break;
  }

  exit(0);
}

