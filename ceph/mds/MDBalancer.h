#ifndef __MDBALANCER_H
#define __MDBALANCER_H

class MDS;
class CInode;
class MExportDir;
class MExportDirAck;

class MDBalancer {
 protected:
  MDS *mds;
  
 public:
  MDBalancer(MDS *m) {
	mds = m;
  }
  
  void export_dir(CInode *in,
				  int mds);
  void export_dir_ack(MExportDirAck *m);

  void import_dir(MExportDir *m);
				  
};

#endif
