

#include "../crush/Bucket.h"
using namespace crush;

#include <iostream>
#include <vector>
using namespace std;


ostream& operator<<(ostream &out, const vector<int> &v)
{
  out << "[";
  for (int i=0; i<v.size(); i++) {
    if (i) out << " ";
    out << v[i];
  }
  out << "]";
  return out;
}


int main() 
{
  Hash h(73);

  vector<int> disks;
  for (int i=0; i<20; i++)
    disks.push_back(i);


  /*
  UniformBucket ub(1, 1, 0, 10, disks);
  ub.make_primes(h);
  cout << "primes are " << ub.primes << endl;
  */

  MixedBucket mb(2, 1);
  for (int i=0;i<20;i++)
    mb.add_item(i, 10);

  /*
  MixedBucket b(3, 1);
  b.add_item(1, ub.get_weight());
  b.add_item(2, mb.get_weight());
  */
  MixedBucket b= mb;

  vector<int> ocount(disks.size());
  int numrep = 3;

  vector<int> v(numrep);
  for (int x=1; x<1000000; x++) {
    //cout << H(x) << "\t" << h(x) << endl;
    for (int i=0; i<numrep; i++) {
      int d = b.choose_r(x, i, h);
      v[i] = d;
      ocount[d]++;
    }
    //cout << v << "\t" << endl;//ocount << endl;
  }

  for (int i=0; i<ocount.size(); i++) {
    cout << "disk " << i << " has " << ocount[i] << endl;
  }

}
