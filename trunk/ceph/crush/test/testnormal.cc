
#include <vector>
#include <iostream>
using namespace std;


void getdist(vector<int>& v, float& avg, float& var) 
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

int main() 
{
  int n = 50;
  vector<int> a(n);
  vector<int> b(n);

  for (int i=0; i<n*n; i++)
	a[rand()%n]++;

  float aavg, avar;
  getdist(a, aavg, avar);

  for (int i=0; i<7*n*n; i++)
	b[rand()%n]++;

  float bavg, bvar;
  getdist(b, bavg, bvar);

  cout << "a avg " << aavg << " var " << avar << endl;
  cout << "b avg " << bavg << " var " << bvar << endl;


  vector<int> c(n);
  for (int i=0; i<n; i++)
	c[i] = a[i] * b[i];

  float cavg, cvar;
  getdist(c, cavg, cvar);

  cout << "c avg " << cavg << " var " << cvar << endl;
	
}
