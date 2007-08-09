
#include "include/types.h"
#include "include/frag.h"

int main(int argc, char **argv)
{
  fragtree_t tree;
  tree.split(frag_t(),2);
  tree.split(frag_t(0,2),1);
  tree.split(frag_t(1,2),1);
  tree.split(frag_t(2,2),1);
  tree.split(frag_t(1,3),1);

  cout << "tree is " << tree << endl;
  frag_t fg(2,4);
  cout << "fg is " << fg << endl;
  tree.force_to_leaf(fg);
  
}
