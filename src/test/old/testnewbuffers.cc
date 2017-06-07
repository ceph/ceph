
#include <list>
#include <iostream>
using namespace std;


#include "include/newbuffer.h"
//#include "include/bufferlist.h"

#include "common/Thread.h"


  class Th : public Thread {
  public:
	bufferlist bl;
	explicit Th(bufferlist& o) : bl(o) { }
	
	void *entry() {
	  //cout << "start" << endl;
	  // thrash it a bit.
	  for (int n=0; n<10000; n++) {
		bufferlist bl2;
		unsigned off = rand() % (bl.length() -1);
		unsigned len = 1 + rand() % (bl.length() - off - 1);
		bl2.substr_of(bl, off, len);
		bufferlist bl3;
		bl3.append(bl);
		bl3.append(bl2);
		//cout << bl3 << endl;
		bl2.clear();
		bl3.clear();
	  }
	  //cout << "end" << endl;
	}
  };

int main()
{

  bufferptr p1 = buffer::copy("123456",7);
  //bufferptr p1 = new buffer("123456",7);
  bufferptr p2 = p1;

  cout << "p1 is '" << p1.c_str() << "'" << " " << p1 << endl;
  cout << "p2 is '" << p2.c_str() << "'" << " " << p2 << endl;

  bufferptr p3 = buffer::copy("abcdef",7);
  //bufferptr p3 = new buffer("abcdef",7);
  
  cout << "p3 is " << p3.c_str() << " " << p3 << endl;

  bufferlist bl;
  bl.push_back(p2);
  bl.push_back(p1);
  bl.push_back(p3);

  cout << "bl is " << bl << endl;

  bufferlist took;
  bl.splice(10,4,&took);

  cout << "took out " << took << ", leftover is " << bl << endl;
  //cout << "len is " << bl.length() << endl;

  bufferlist bl2;
  bl2.substr_of(bl, 3, 5);
  cout << "bl2 is " << bl2 << endl;


  cout << "bl before " << bl << endl;

  list<Th*> ls;
  for (int t=0; t<40; t++) {
	Th *t = new Th(bl);
	cout << "create" << endl;
	t->create();
	ls.push_back(t);
  }

  bl.clear();

  while (!ls.empty()) {
	cout << "join" << endl;
	ls.front()->join();
	delete ls.front();
	ls.pop_front();
  }

  cout << "bl after " << bl << endl;

}
