// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * (C) Copyright IBM Corporation 2022
 * Author: Martin Ohmacht <mohmacht@us.ibm.com>
 *
 * Based on the file ceph/src/erasure-code/ErasureCodeInterface.h
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#ifndef CEPH_EXT_BLK_DEV_INTERFACE_H
#define CEPH_EXT_BLK_DEV_INTERFACE_H

/*! @file ExtBlkDevInterface.h
    @brief Interface provided by extended block device plugins

    Block devices with verdor specific capabilities rely on plugins implementing
    **ExtBlkDevInterface** to provide access to their capabilities.
    
    Methods returning an **int** return **0** on success and a
    negative value on error.
 */ 

#include <string>
#include <map>
#include <ostream>
#include <memory>

namespace ceph {
  //! argument structure passed to extblkdev plugin when trying to detect extended capability set
  struct ExtBlkDevProfile{
    //! name of device to be examined. It may be an logical volume that needs detection of underlying devices
    std::string devname;
    inline bool operator!=(const ExtBlkDevProfile& profile) {
      return devname!=profile.devname;
    }
  };
  inline std::ostream& operator<<(std::ostream& out, const ExtBlkDevProfile& profile) {
    out << "{" << profile.devname  << "}";
    return out;
  }

  class ExtBlkDevInterface {
  public:
    virtual ~ExtBlkDevInterface() {}

    /**
     * Initialize the instance according to the content of
     * **profile**. The **ss** stream is set with debug messages or
     * error messages, the content of which depend on the
     * implementation.
     *
     * Return 0 on success or a negative errno on error. When
     * returning on error, the implementation is expected to
     * provide a human readable explanation in **ss**.
     *
     * @param [in] profile argument set used to detect device
     * @param [out] ss contains informative messages when an error occurs
     * @return 0 on success or a negative errno on error.
     */
    virtual int init(ExtBlkDevProfile &profile, std::ostream *ss) = 0;

    /**
     * Return the profile that was used to initialize the instance
     * with the **init** method.
     *
     * @return the profile in use by the instance
     */
    virtual const ExtBlkDevProfile &get_profile() const = 0;

    /**
     * Return the name of the underlying device detected by **init** method
     *
     * @return the name of the underlying device
     */
    virtual const std::string& get_devname() const = 0;

    /**
     * Provide status of underlying physical storage after compression.
     * The values denote effectively total and available capacity for 
     * incompressible data.
     * The **ss** stream is set with debug messages or
     * error messages, the content of which depend on the
     * implementation.
     *
     * Return 0 on success or a negative errno on error. When
     * returning on error, the implementation is expected to
     * provide a human readable explanation in **ss**.
     *
     * @param [out] total total capacity for incompressible data
     * @param [out] avail available capacity for incompressible data
     * @param [out] ss contains informative messages when an error occurs
     * @return 0 on success or a negative errno on error.
     */
    virtual int get_thin_utilization(uint64_t *total, uint64_t *avail, std::ostream *ss) = 0;

    /**
     * Populate property map with meta data of device.
     *
     * @param [in] prefix prefix to be prepended to all map values by this method
     * @param [in,out] pm property map of the device, to be extended by attributes detected by this plugin
     * @return 0 on success or a negative errno on error.
     */
    virtual int collect_metadata(const std::string& prefix, std::map<std::string,std::string> *pm) = 0;
  };

  typedef std::shared_ptr<ExtBlkDevInterface> ExtBlkDevInterfaceRef;

  class ExtBlkDevPlugin {
  public:
    void *library;

    ExtBlkDevPlugin() :
      library(0) {}
    virtual ~ExtBlkDevPlugin() {}

    /**
     * Factory method, creating ExtBlkDev instances
     *
     * @param [in] profile argument set specifying device to check for support by this plugin
     * @param [out] ext_blk_dev object created on successful device support detection
     * @param [out] ss contains informative messages when an error occurs
     * @return 0 on success or a negative errno on error.
     */
    virtual int factory(ExtBlkDevProfile &profile,
                        ExtBlkDevInterfaceRef *ext_blk_dev,
			std::ostream *ss) = 0;
  };

}

extern "C" {
    /**
     * version function, providing ceph version the plugin was built for
     *
     * @return ceph version string
     */
  const char *__ext_blk_dev_version();
    /**
     * plugin initialization function
     *
     * @param [in] plugin_name name of plugin
     * @return plugin factory object
     */
  ceph::ExtBlkDevPlugin* __ext_blk_dev_init(char *plugin_name);
}

#endif
