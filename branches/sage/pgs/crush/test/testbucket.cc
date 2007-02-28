

#include "../Bucket.h"
using namespace crush;

#include <iostream>
#include <vector>
using namespace std;


ostream& operator<<(ostream& out, vector<int>& v)
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

  int ndisks = 0;
  int numrep = 3;

  StrawBucket mb(1);
  /*for (int i=0;i<10;i++)
	mb.add_item(ndisks++, 10);
  */
  mb.add_item(ndisks++, 1);
  mb.add_item(ndisks++, 1);
  mb.add_item(ndisks++, 10);
  mb.add_item(ndisks++, 10);
  mb.add_item(ndisks++, 100);
  mb.add_item(ndisks++, 1000);

  vector<int> ocount(ndisks);

  vector<int> v(numrep);
  int nplace = 0;
  for (int x=1; x<1000000; x++) {
	//cout << H(x) << "\t" << h(x) << endl;
	for (int i=0; i<numrep; i++) {
	  int d = mb.choose_r(x, i, h);
	  v[i] = d;
	  ocount[d]++;
	  nplace++;
	}
	//cout << v << "\t" << endl;//ocount << endl;
  }

  for (int i=0; i<ocount.size(); i++) {
	float f = ocount[i] / (float)nplace;
	cout << "disk " << i << " has " << ocount[i] << "  " << f << endl;
  }

}
