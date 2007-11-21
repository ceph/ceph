
#include <iostream>
using namespace std;

#include "include/bufferlist.h"


int main()
{

  bufferptr p1 = new buffer("123456",6);
  bufferptr p2 = p1;

  cout << "it is '" << p1.c_str() << "'" << endl;

  bufferptr p3 = new buffer("abcdef",6);
  
  cout << "p3 is " << p3 << endl;

  bufferlist bl;
  bl.push_back(p2);
  bl.push_back(p1);
  bl.push_back(p3);

  cout << "bl is " << bl << endl;

  cout << "len is " << bl.length() << endl;

  bufferlist took;
  bl.splice(10,4,&took);

  cout << "took out " << took << "leftover is " << bl << endl;
  //cout << "len is " << bl.length() << endl;

  bufferlist bl2;
  bl2.substr_of(bl, 3, 5);
  cout << "bl2 is " << bl2 << endl;
  

}
