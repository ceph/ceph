

#include "include/mds.h"
#include "include/dcache.h"

#include <iostream>
using namespace std;


int main(char **argv, int argc) {
  cout << "hi there" << endl;

  CInode* root = new CInode();
  root->dentries = new CDir(root);
  
  CInode* bla = new CInode();
  string sblah("blah");
  root->dentries->add_child(new CDentry(sblah,bla));

  //CDentry* rd = new CDentry(new string, root);
  root->dump();

  return 0;
}
