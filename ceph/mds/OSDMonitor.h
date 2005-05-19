
class MDS;

class OSDMonitor {
  MDS *mds;



 public:
  OSDMonitor(MDS *mds) {
	this->mds = mds;
  }

  void proc_message(Message *m);
  void handle_ping(MPing *m);

};
