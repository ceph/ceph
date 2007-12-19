

#include "../crush/BinaryTree.h"
using namespace crush;

#include <iostream>
#include <vector>
using namespace std;

int main() 
{
  BinaryTree t;

  vector<int> nodes;

  for (int i=0; i<30; i++) {
    cout << "adding " << i << endl;
    int n = t.add_node(1);
    nodes.push_back(n);
    //cout << t << endl;
  }
  cout << t << endl;

  for (int k=0; k<10000; k++) {
    if (rand() % 2) {
      cout << "adding" << endl;
      nodes.push_back( t.add_node(1) );
    } else {
      if (!nodes.empty()) {
        //for (int i=0; i<nodes.size(); i++) {
        int p = rand() % nodes.size();
        int n = nodes[p];
        assert (t.exists(n));
        cout << "removing " << n << endl;
        t.remove_node(n);
        
        for (int j=p; j<nodes.size(); j++)
          nodes[j] = nodes[j+1];
        nodes.pop_back();
      }
    }
    cout << t << endl;
  }


}
