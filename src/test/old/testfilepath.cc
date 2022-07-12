
#include "include/filepath.h"
#include <iostream>
using namespace std;

int print(const string &s) {
  filepath fp = s;
  cout << "s = " << s << "   filepath = " << fp << endl;
  cout << "  depth " << fp.depth() << endl;
  for (int i=0; i<fp.depth(); i++) {
    cout << "\t" << i << " " << fp[i] << endl;
  }
}

int main() {
  filepath p;
  print("/home/sage");
  print("a/b/c");
  print("/a/b/c");
  print("/a/b/c/");
  print("/a/b/../d");
}
