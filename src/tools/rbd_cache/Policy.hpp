#ifndef RBD_CACHE_POLICY_HPP
#define RBD_CACHE_POLICY_HPP

enum CACHESTATUS {
  NONE = 0,
  PROMOTING,
  PROMOTED,
  EVICTING,
  EVICTED,
};


class Policy {
public:
  Policy(){}
  virtual ~Policy(){};
  virtual CACHESTATUS lookup_object(std::string) = 0;
  virtual int evict_object(std::string&) = 0;
  virtual void update_status(std::string, CACHESTATUS) = 0;
  virtual CACHESTATUS get_status(std::string) = 0;
};
#endif
