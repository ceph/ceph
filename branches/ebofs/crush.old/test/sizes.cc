
#include "include/types.h"
#include "include/Distribution.h"
#include "osd/OSDMap.h"


Distribution file_size_distn; //kb


list<int> object_queue;
int max_object_size = 1024*1024*100;  //kb

off_t no;

int get_object()  //kb
{
  if (object_queue.empty()) {
	int max = file_size_distn.sample();
	no++;
	int filesize = max/2 + (rand() % 100) * max/200 + 1;
	//cout << "file " << filesize << endl;
	while (filesize > max_object_size) {
	  object_queue.push_back(max_object_size);
	  filesize -= max_object_size;
	}
	object_queue.push_back(filesize);
  }
  int s = object_queue.front();
  object_queue.pop_front();
  //cout << "object " << s << endl;
  return s;
}

void getdist(vector<off_t>& v, float& avg, float& var) 
{
  avg = 0.0;
  for (int i=0; i<v.size(); i++)
	avg += v[i];
  avg /= v.size();
  
  var = 0.0;
  for (int i=0; i<v.size(); i++)
	var += (v[i] - avg) * (v[i] - avg);
  var /= v.size();
}


void testpgs(int n, // numpg
			 off_t pggb,
			 float& avg,
			 float& var,
			 off_t& numo
			 )
{
  off_t dist = (off_t)n * 1024LL*1024LL * (off_t)pggb;  //kb
  vector<off_t> pgs(n);
  off_t did = 0;
  
  no = 0;
  while (did < dist) {
	off_t s = get_object();
	pgs[rand()%n] += s;
	did += s;
  }
  while (!object_queue.empty())
	pgs[rand()%n] += get_object();

  numo = no;
  //cout << did/n << endl; 

  //for (int i=0; i<n; i++) cout << pgs[i] << endl;

  getdist(pgs, avg, var);
  //cout << "avg " << avg << "  var " << var << "  dev " << sqrt(var) << endl;
 
}



int main()
{
  /*

// File Size 
//cate   count_mean             size_mean       
1b      -0.5     0.65434375     0        
1k      0.5      19.0758125     0.00875          
512K    1.5      35.6566        2.85875
1M      2.5      27.7271875     25.0084375       
2M      3.5      16.63503125    20.8046875       
4M      4.5      106.82384375   296.053125       
8M      5.5      81.493375      335.77625        
16M     6.5      14.13553125    185.9775         
32M     7.5      2.176          52.921875
256M    8.5      0.655938       47.8066
512M    9.5      0.1480625      57.83375 
2G      10.5     0.020125       19.2888 
  */
  file_size_distn.add(1, 19.0758125+0.65434375);
  file_size_distn.add(512, 35.6566);
  file_size_distn.add(1024, 27.7271875);
  file_size_distn.add(2*1024, 16.63503125);
  file_size_distn.add(4*1024, 106.82384375);
  file_size_distn.add(8*1024, 81.493375);
  file_size_distn.add(16*1024, 14.13553125);
  file_size_distn.add(32*1024, 2.176);
  file_size_distn.add(256*1024, 0.655938);
  file_size_distn.add(512*1024, 0.1480625);
  file_size_distn.add(1*1024*1024, 0.020125); // actually 2, but 32bit
  file_size_distn.normalize();

  
  for (int pggb = 1; pggb < 16; pggb++) {
	cout << pggb;
	for (int max = 1; max <= 1024; max *= 2) {
	  float avg, var, var2, var3;
	  off_t no;
	  max_object_size = max*1024;
	  testpgs(100, pggb, avg, var, no);
	  testpgs(100, pggb, avg, var2, no);
	  testpgs(100, pggb, avg, var3, no);
	  float dev = sqrt((var+var2+var3)/3.0);
	  cout << "\t" << no << "\t" << max << "\t" << dev;
	}
	cout << endl;
  }




}
