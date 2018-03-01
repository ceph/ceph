#include <vector>
#include <iostream>

#include "include/random.h"

#include "../crush/BinaryTree.h"

using namespace crush;
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
    if (ceph::util::generate_random_number(1)) {
      cout << "adding" << endl;
      nodes.push_back( t.add_node(1) );
    } else {
      if (!nodes.empty()) {
        //for (int i=0; i<nodes.size(); i++) {
        int p = ceph::util::generate_random_number(nodes.size() - 1);
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
