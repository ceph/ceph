#ifndef __MDCLUSTER_H
#define __MDCLUSTER_H

#include <string>
#include <vector>
using namespace std;

class CDir;
class MDS;

class MDCluster {
 protected:
  vector<MDS*> mds;

 public:
  MDCluster();
  ~MDCluster();
  
  int get_size() { return mds.size(); }
  int add_mds(MDS *m);
  int hash_dentry( CDir *dir, string& dn );  

};

#endif
