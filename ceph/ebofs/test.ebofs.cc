
#include <iostream>
#include "ebofs/Ebofs.h"

bool stop = false;


int nt = 0;
class Tester : public Thread {
  Ebofs &fs;
  int t;
  
  char b[1024*1024];

public:
  Tester(Ebofs &e) : fs(e), t(nt) { nt++; }
  void *entry() {

	while (!stop) {
	  object_t oid = rand() % 1000;
	  coll_t cid = rand() % 50;
	  off_t off = rand() % 10000;//0;//rand() % 1000000;
	  off_t len = 1+rand() % 100000;
	  char *a = "one";
	  if (rand() % 2) a = "two";
	  int l = 3;//rand() % 10;

	  switch (rand() % 8) {
	  case 0:
		{
		  cout << t << " read " << oid << " at " << off << " len " << len << endl;
		  bufferlist bl;
		  fs.read(oid, len, off, bl);
		  int l = MIN(len,bl.length());
		  if (l) {
			cout << t << " got " << l << endl;
			bl.copy(0, l, b);
			char *p = b;
			while (l--) {
			  assert(*p == 0 ||
					 *p == (char)(off ^ oid));
			  off++;
			  p++;
			}
		  }
		}
		break;

	  case 1:
		{
		  cout << t << " write " << oid <<" at " << off << " len " << len << endl;
		  for (int j=0;j<len;j++) 
			b[j] = (char)(oid^(off+j));
		  bufferlist w;
		  w.append(b,len);
		  fs.write(oid, len, off, w);
		}
		break;

	  case 2:
		cout << t << " remove " << oid << endl;
		fs.remove(oid);
		break;

	  case 3:
		cout << t << " collection_add " << oid << " to " << cid << endl;
		fs.collection_add(cid, oid);
		break;

	  case 4:
		cout << t << " collection_remove " << oid << " from " << cid << endl;
		fs.collection_remove(cid, oid);
		break;

	  case 5:
		cout << t << " setattr " << oid << " " << a << " len " << l << endl;
		fs.setattr(oid, a, (void*)a, l);
		break;
		
	  case 6:
		cout << t << " rmattr " << oid << " " << a << endl;
		fs.rmattr(oid,a);
		break;

	  case 7:
		{
		  char v[4];
		  cout << t << " getattr " << oid << " " << a << endl;
		  if (fs.getattr(oid,a,(void*)v,3) == 0) {
			v[3] = 0;
			assert(strcmp(v,a) == 0);
		  }
		}
		break;
	  }


	}
	cout << t << " done" << endl;
	return 0;
  }
};

int main(int argc, char **argv)
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args
  if (args.size() != 3) return -1;
  char *filename = args[0];
  int seconds = atoi(args[1]);
  int threads = atoi(args[2]);

  cout << "dev " << filename << " .. " << threads << " threads .. " << seconds << " seconds" << endl;

  Ebofs fs(filename);
  if (fs.mount() < 0) return -1;

  list<Tester*> ls;
  for (int i=0; i<threads; i++) {
	Tester *t = new Tester(fs);
	t->create();
	ls.push_back(t);
  }

  utime_t now = g_clock.now();
  utime_t dur(seconds,0);
  utime_t end = now + dur;
  cout << "stop at " << end << endl;
  while (now < end) {
	sleep(1);
	now = g_clock.now();
	cout << now << endl;
  }

  cout << "stopping" << endl;
  stop = true;
  
  while (!ls.empty()) {
	Tester *t = ls.front();
	ls.pop_front();
	t->join();
	delete t;
  }

  fs.umount();
  return 0;
}

