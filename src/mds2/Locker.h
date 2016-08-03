#ifndef CEPH_MDS_LOCKER_H
#define CEPH_MDS_LOCKER_H

class MDSRank;
class MClientCaps;
class MClientCapRelease;
class MClientLease;

class Locker {
  MDSRank *mds;
public:
  Locker(MDSRank *_mds) : mds(_mds) {}
  void dispatch(Message *m);
  void handle_client_caps(MClientCaps *m);
  void handle_client_cap_release(MClientCapRelease *m);
  void handle_client_lease(MClientLease *m);
};
#endif
