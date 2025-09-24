/*
 * plugin for Ceph - scalable distributed file system
 *
 * (C) Copyright IBM Corporation 2022
 * Author: Martin Ohmacht <mohmacht@us.ibm.com>
 *
 */

#include "ceph_ver.h"
#include "extblkdev/ExtBlkDevInterface.h"
#include "common/blkdev.h"
#include "include/stringify.h"
#include "include/compat.h"
#include "common/debug.h"

//#define dout_subsys ceph_subsys_context
#define dout_context cct
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "fcm(" << this << ") "

#include <blkid/blkid.h>
#include <cstring>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <linux/nvme_ioctl.h>
#include <set>
#include <sstream>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/capability.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>


//! class managing one logical volume or partition, which may use one or more underlying FCM devices
class ExtBlkDevFcm : public ceph::ExtBlkDevInterface
{
  //! ceph context used for logging messages
  CephContext *cct;

  //! name of the top level logical device
  std::string logdevname;

  //! class managing one underlying FCM devices
  class fcm_dev{
    int fd = -1; //! file descriptor for underlying device, used for issueing log queries
    std::string fcm_devname; //! name of the underlying fcm device
    uint64_t log[4]; //! utilization queried from device log page

  public:
    fcm_dev(const std::string& name):fcm_devname(name){}
    ~fcm_dev(){
      if(fd>=0)
	VOID_TEMP_FAILURE_RETRY(::close(fd));
    }
    int query_log()
    {
      // fetch only 4 numbers from the log

      // number of bytes to transfer
      unsigned log_size=sizeof(log);
      // number of double word (32b) blocks
      uint32_t num_dw = (log_size >> 2) - 1;

      struct nvme_passthru_cmd cmd = {
        .opcode     = 2, // get admin log page
        .nsid       = 0xffffffff, // nsid == all
        .addr       = (__u64)(uintptr_t)log,
        .data_len   = log_size,
        .cdw10      = 202 | ((num_dw & 0xffff)<<16), // log 202 contains phys util number
        .cdw11      = num_dw>>16,
        .cdw12      = 280, // avail numbers start at offset 280
        .cdw13      = 0,
        .cdw14      = 0,
        .timeout_ms = 0,
      };

      int rc=ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);
      if(rc<0)
	return -errno;
      return 0;
    }
    int open(CephContext *cct){
      // open file descriptor of underlying hardware device for state queries
      std::string path = std::string("/dev/") + fcm_devname;
      fd = ::open(path.c_str(), O_RDWR);
      if(fd<0){
	dout(1) << __func__ << " Can not open FCM device at path " << path << dendl;
	return -4000-errno;
      }
      return 0;
    }
    uint64_t get_device_physical_size() const {return log[0];}
    uint64_t get_device_physical_util() const {return log[1];}
    uint64_t get_device_physical_avail() const {
      int64_t avail=get_device_physical_size()-get_device_physical_util();
      return avail<0 ? 0 : avail;}
    uint64_t get_device_logical_size() const {return log[2];}
    uint64_t get_device_logical_util() const {return log[3];}
    uint64_t get_device_logical_avail() const {
      int64_t avail=get_device_logical_size()-get_device_logical_util();
      return avail<0 ? 0 : avail;}
  };

  std::vector<fcm_dev> fcm_devices;
  int64_t lsize=0;  // total number of logical bytes of logical volume
  int64_t lavail=0;  // total number of logical available bytes of logical volume
  uint64_t psize=0; // total number of physical bytes of logical volume
  uint64_t pavail=0; // total number of physical available bytes of logical volume
  struct timespec last_access = {0};

  uint64_t get_partition_physical_size() const {return psize;}
  uint64_t get_partition_logical_size() const {return lsize;}
  uint64_t get_partition_physical_avail() const {return pavail;}
  uint64_t get_partition_logical_avail() const {return lavail;}

  uint64_t get_device_physical_size() const {
    uint64_t sum=0;
    for (auto& d : fcm_devices) {
      sum += d.get_device_physical_size();
    }
    return sum;
  }
  uint64_t get_device_physical_util() const {
    uint64_t sum=0;
    for (auto& d : fcm_devices) {
      sum += d.get_device_physical_util();
    }
    return sum;
  }
  uint64_t get_device_logical_size() const {
    uint64_t sum=0;
    for (auto& d : fcm_devices) {
      sum += d.get_device_logical_size();
    }
    return sum;
  }
  uint64_t get_device_logical_util() const {
    uint64_t sum=0;
    for (auto& d : fcm_devices) {
      sum += d.get_device_logical_util();
    }
    return sum;
  }


  //! set the CAP_SYS_ADMIN in the effective capapbility set to value v
  static int set_cap(cap_flag_value_t v)
  {
    int rc=0;
    cap_t caps=cap_get_proc();
    if (caps == NULL){
      return -600;
    }
    do{
      // does the cap already have the target value?
      cap_flag_value_t cv;
      if(cap_get_flag(caps, CAP_SYS_ADMIN, CAP_EFFECTIVE, &cv) == -1){
	rc=-605;
	break;
      }
      // if so, indicate a return code ==1 to caller
      if(cv==v){
	rc=1;
	break;
      }
      cap_value_t cap_list[1];
      cap_list[0] = CAP_SYS_ADMIN;
      if (cap_set_flag(caps, CAP_EFFECTIVE, 1, cap_list, v) == -1){
	rc=-601;
	break;
      }
      if (cap_set_proc(caps) == -1){
	rc=-602;
	break;
      }
    }while(0);
    if (cap_free(caps) == -1){
      return -604;
    }
    return rc;
  }


  int get_fcm_utilization()
  {
    struct timespec tnow;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &tnow);
    // have we retrieved log in past 15 seconds?
    if(tnow.tv_sec < last_access.tv_sec + 15){
      // just use cached numbers and indicate in return code
      return 1;
    }

    // set SYS_ADMIN capability in effective set, needed for NVME ioctl
    int was_set=set_cap(CAP_SET);
    if(was_set<0){
      return was_set;
    }

    uint64_t sum_ps=0;
    uint64_t sum_pa=0;
    uint64_t sum_ls=0;
    uint64_t sum_la=0;

    // accumulate status across devices
    for (auto& dev : fcm_devices) {
      int rc=dev.query_log();
      if(rc<0)
	return rc;
      uint64_t ps=dev.get_device_physical_size(); // physical size
      uint64_t pa=dev.get_device_physical_avail(); // physical available space
      uint64_t ls=dev.get_device_logical_size();  // logical size
      uint64_t la=dev.get_device_logical_avail();  // logical available space
      if(ps==0 || ls==0){
	return -500;
      }
      sum_ps+=ps;
      sum_pa+=pa;
      sum_ls+=ls;
      sum_la+=la;
    }

    // restore effective capability set if we changed it
    if(!was_set){
      int rc=set_cap(CAP_CLEAR);
      if(rc<0)
	return rc;
    }

    // apportion space to OSD fractions
    double frac=(double)get_partition_logical_size()/sum_ls;
    psize = sum_ps*frac;
    pavail = sum_pa*frac;
    lavail = sum_la*frac;
    last_access = tnow;
    return 0;
  }

  static int fcm_get_int_property(const std::string& devpath, const std::string& attr)
  {
    // fetch integer device attribute from sysfs
    std::string path = devpath + "/device/"+attr;
    int fd=::open(path.c_str(), O_RDONLY);
    if(fd<0)
      return -errno;
    int rc=-1;
    char buf[1024];
    int r = ::read(fd, buf, sizeof(buf) - 1);
    if (r > 0) {
      buf[r] = 0;
      rc = strtol(buf,0,0);
    }else{
      rc=-errno;
    }
    TEMP_FAILURE_RETRY(::close(fd));
    return rc;
  }

  int get_lsize()
  {
    // retrieve size of logical device assigned to this OSD
    // this is used later on to apportion physical space accordingly

    std::string path = std::string("/dev/") + logdevname;
    int fd = ::open(path.c_str(), O_RDONLY);
    if(fd<0){
      dout(1) << __func__ << " Can not open logical device " << logdevname << dendl;
      return -2000-errno;
    }
    BlkDev bdfd(fd);
    int rc=bdfd.get_size(&lsize);
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    if(rc<0){
      dout(1) << __func__ << " Can not get device size of " << logdevname << dendl;
      return -3000-errno;
    }
    return 0;
  }

public:
  explicit ExtBlkDevFcm(CephContext *cct) : cct(cct) {}
  ~ExtBlkDevFcm(){}
  int init(const std::string& logdevname_a){
    logdevname=logdevname_a;

    // determine device name for underlying hardware
    std::set<std::string> raw_devices;
    get_raw_devices(logdevname, &raw_devices);
    for (auto& d : raw_devices) {
      std::string devpath = "/sys/block/" + d + "/device/";
      uint32_t vendor;
      uint32_t device;
      if (scan_device_and_vendor(devpath, vendor, device) == 0) {
        dout(10) << __func__ << "found device=" << device
                 << " vendor=" << vendor << " at " << devpath << dendl;
        // get vendor and device id of underlying hardware, compare with FCM ids
        if (vendor == 0x1014 && device == 0x0634) {
          fcm_devices.push_back(fcm_dev(d));
          dout(1) << __func__ << " Found FCM vendor/device id on " << d << dendl;
        }
      }
    }
    if(fcm_devices.empty()){
      return -1000;
    }

    // get size of logical volume/partition
    int rc=get_lsize();
    if(rc<0)
      return rc;

    // open file handles for FCM devices
    for (auto& d : fcm_devices) {
      int rc=d.open(cct);
      if(rc<0)
	return rc;
    }

    // do initial query for utilization to ensure query mechanism works
    rc=get_fcm_utilization();
    if(rc<0){
      dout(1) << __func__ << " Can not access physical utilization log of FCM device" << dendl;
      return rc;
    }
    return 0;
  }
  /*
    Scans provided directory for entries ?/device/device and ?/device/vendor.

    For example for "nvme0n1" device the required files are in:
    /sys/block/nvme0n1/device/nvme0/device/device and
    /sys/block/nvme0n1/device/nvme0/device/vendor
  */
  int scan_device_and_vendor( const std::string& e,
                              uint32_t& vendor,
                              uint32_t& device) {
    DIR *dir;
    struct dirent *entry;
    dir = opendir(e.c_str());
    if (dir == nullptr) {
      return -1;
    }
    int result = -1;
    while ((entry = readdir(dir)) != nullptr) {
      int vendor_i;
      int device_i = fcm_get_int_property(e + entry->d_name, "device");
      if (device_i >= 0) {
        vendor_i = fcm_get_int_property(e + entry->d_name, "vendor");
      }
      if (vendor_i >= 0 && device_i >= 0) {
        device = device_i;
        vendor = vendor_i;
        result = 0;
        break;
      }
    }
    closedir(dir);
    return result;
  }

  virtual const std::string& get_devname() const {return logdevname;}
  int get_state(ExtBlkDevState& state)
  {
    int rc=get_fcm_utilization();
    if(rc<0)
      return rc;
    if(rc==0){
      dout(1) << __func__ << " FCM volume " << get_devname() << " physical utilization:" << dendl;
      dout(1) << __func__ << " FCM device logical size: " << get_device_logical_size() << dendl;
      dout(1) << __func__ << " FCM device logical util: " << get_device_logical_util() << dendl;
      dout(1) << __func__ << " FCM device physical size: " << get_device_physical_size() << dendl;
      dout(1) << __func__ << " FCM device physical util: " << get_device_physical_util() << dendl;
      dout(1) << __func__ << " FCM partition logical size: " << get_partition_logical_size() << dendl;
      dout(1) << __func__ << " FCM partition logical avail: " << get_partition_logical_avail() << dendl;
      dout(1) << __func__ << " FCM partition physical size: " << get_partition_physical_size() << dendl;
      dout(1) << __func__ << " FCM partition physical avail: " << get_partition_physical_avail() << dendl;
    }
    state.set_logical_total(get_partition_logical_size());
    state.set_logical_avail(get_partition_logical_avail());
    state.set_physical_total(get_partition_physical_size());
    state.set_physical_avail(get_partition_physical_avail());
    return 0;
  }
  int collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm)
  {
    int rc=get_fcm_utilization();
    if(rc<0)
      return rc;
    (*pm)[prefix + "fcm"] = "true";
    (*pm)[prefix + "fcm_partition_physical_size"] = stringify(get_partition_physical_size());
    (*pm)[prefix + "fcm_partition_logical_size"] = stringify(get_partition_logical_size());
    (*pm)[prefix + "fcm_partition_physical_avail"] = stringify(get_partition_physical_avail());
    (*pm)[prefix + "fcm_partition_logical_avail"] = stringify(get_partition_logical_avail());
    (*pm)[prefix + "fcm_device_physical_size"] = stringify(get_device_physical_size());
    (*pm)[prefix + "fcm_device_logical_size"] = stringify(get_device_logical_size());
    return 0;
  }
};

class ExtBlkDevPluginFcm : public ceph::ExtBlkDevPlugin {
public:
  explicit ExtBlkDevPluginFcm(CephContext *cct) : ExtBlkDevPlugin(cct) {}
  int get_required_cap_set(cap_t caps)
  {
    cap_value_t adm[1];
    adm[0]=CAP_SYS_ADMIN;
    // set SYS_ADMIN capability in permitted set
    return cap_set_flag(caps, CAP_PERMITTED, 1, adm, CAP_SET);
  }
  int factory(const std::string& logdevname,
	      ceph::ExtBlkDevInterfaceRef& ext_blk_dev)
  {
    auto fcm = new ExtBlkDevFcm(cct);
    int r = fcm->init(logdevname);
    if (r != 0) {
      delete fcm;
      return r;
    }
    ext_blk_dev.reset(fcm);
    return 0;
  }
};

const char *__ceph_plugin_version() { return CEPH_GIT_NICE_VER; }

int __ceph_plugin_init(CephContext *cct,
		       const std::string& type,
		       const std::string& name)
{
  auto plg=new ExtBlkDevPluginFcm(cct);
  if(plg==0) return -ENOMEM;
  int rc=cct->get_plugin_registry()->add(type, name, plg);
  if(rc!=0){
    delete plg;
  }
  return rc;
}
