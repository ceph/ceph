
#include <iostream>
using namespace std;

#include "include/bufferlist.h"


int main()
{

  bufferptr p1 = new buffer("hello",6);
  bufferptr p2 = p1;

  cout << "it is '" << p1.c_str() << "'" << endl;

  bufferptr p3 = new buffer("there",6);
  
  cout << "p3 is " << p3 << endl;

  bufferlist bl;
  bl.push_back(p2);
  bl.push_back(p1);
  bl.push_back(p3);

  cout << "bl is " << bl << endl;

  cout << "len is " << bl.length() << endl;

  bl.splice(3,6);

  cout << "bl is now " << bl << endl;
  cout << "len is " << bl.length() << endl;

  bufferlist bl2;
  bl2.substr_of(bl, 3, 5);
  cout << "bl2 is " << bl2 << endl;
  

}
