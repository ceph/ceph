
#ifndef SCRUB_HEADER_H_
#define SCRUB_HEADER_H_

class CInode;

/**
 * Externally input parameters for a scrub, associated with the root
 * of where we are doing a recursive scrub
 */
class ScrubHeader {
public:
  CInode *origin;
  std::string tag;

  bool force;
  bool recursive;
  bool repair;
  Formatter *formatter;
};
typedef ceph::shared_ptr<ScrubHeader> ScrubHeaderRef;
typedef ceph::shared_ptr<const ScrubHeader> ScrubHeaderRefConst;

#endif // SCRUB_HEADER_H_

