#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "s3acl.h"

using namespace std;
                                  
int main(int argc, char **argv) {
  S3XMLParser parser;

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

  S3AccessControlPolicy *policy = (S3AccessControlPolicy *)parser.find_first("AccessControlPolicy");

  if (policy) {
    string id="79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be";
    cout << hex << policy->get_perm(id, S3_PERM_ALL) << dec << endl;
    policy->to_xml(cout);
  }

  cout << parser.get_xml() << endl;

  bufferlist bl;
  policy->encode(bl);

  S3AccessControlPolicy newpol;
  bufferlist::iterator iter = bl.begin();
  newpol.decode(iter);

  newpol.to_xml(cout);

  exit(0);
}

