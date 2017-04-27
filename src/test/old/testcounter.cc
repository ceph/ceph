
#include "common/DecayCounter.h"

#include <list>
using namespace std;

struct RealCounter {
public:
  list<int> hits;
  
  void hit(int ms) {
	hits.push_back(ms);
  }
  
  int get(double hl, int now) {
	trim(now-hl);
	return hits.size();
  }

  void trim(int to) {
	while (!hits.empty() &&
		   hits.front() < to) 
	  hits.pop_front();
  }
 

};

int main(int argc, char **argv)
{
  int target;
  double hl = atof(argv[1]);
  cerr << "halflife " << hl << endl;

  DecayCounter dc(hl);
  RealCounter rc;

  utime_t now = ceph_clock_now();

  for (int ms=0; ms < 300*1000; ms++) {
	if (ms % 30000 == 0) {
	  target = 1 + (rand() % 10) * 10;
	  if (ms > 200000) target = 0;
	}

	if (target &&
		(rand() % (1000/target) == 0)) {
	  dc.hit();
	  rc.hit(ms);
	}

	if (ms % 500 == 0) dc.get(now);
	if (ms % 100 == 0) {
	  //dc.get(now);
	  DecayCounter o = dc;
	  cout << ms << "\t"
		   << target*hl << "\t"
		   << rc.get(hl*1000, ms) << "\t"
		   << o.get(now) << "\t" 
		   << dc.val << "\t"
		//		   << dc.delta << "\t"
		   << o.get_last_vel() << "\t"
		   << o.get_last() + o.get_last_vel() << "\t"
		   << endl;
	}

	now += .001;
  }

}
