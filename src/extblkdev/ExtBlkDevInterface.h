// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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
#include <set>
#include <ostream>
#include <memory>
#ifdef __linux__
#include <sys/capability.h>
#else
typedef void *cap_t;
#endif

#include "common/PluginRegistry.h"
#include "osd/osd_types.h"

namespace ceph {

  class ExtBlkDevInterface {
  public:
    virtual ~ExtBlkDevInterface() {}

    /**
     * Initialize the instance if device logdevname is supported
     *
     * Return 0 on success or a negative errno on error
     *
     * @param [in] logdevname name of device to check for support by this plugin
     * @param [in] device name of a physical device to check this plugin support for
     * @return 0 on success or a negative errno on error.
     */
    virtual int init(const std::string& logdevname,
                     const std::string& device) = 0;

    /**
     * Return the name of the underlying device detected by **init** method
     *
     * @return the name of the underlying device
     */
    virtual const std::string& get_devname() const = 0;

    /**
     * Provide total/available stats of underlying physical storage
     * after compression
     *
     * Return 0 on success or a negative errno on error.
     *
     * @param [out] statfs updated total/available statfs members
     * @return 0 on success or a negative errno on error.
     */
    virtual int get_statfs(store_statfs_t& statfs) = 0;

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

  class ExtBlkDevPlugin : public Plugin {
  public:

    explicit ExtBlkDevPlugin(CephContext *cct) : Plugin(cct) {}
    virtual ~ExtBlkDevPlugin() {}

    /**
     * Indicate plugin-required capabilities in permitted set
     * If a plugin requires a capability to be active in the 
     * permitted set when invoked, it must indicate so by setting
     * the required flags in the cap_t structure passed into this method.
     * The cap_t structure is empty when passed into the method, and only the
     * method's modifications to the permitted set are used by ceph.
     * The plugin must elevate the capabilities into the effective
     * set at a later point when needed during the invocation of its
     * other methods, and is responsible to restore the effective set
     * before returning from the method
     *
     * @param [out] caps capability set indicating the necessary capabilities
     */
    virtual int get_required_cap_set(cap_t caps) = 0;

    /**
     * Factory method, creating ExtBlkDev instances
     *
     * @param [in] logdevname name of logic device, may be composed of physical devices
     * @param [in] device name of a physical device
     * @param [out] ext_blk_dev object created on successful device support detection
     * @return 0 on success or a negative errno on error.
     */
    virtual int factory(const std::string& logdevname,
                        const std::string& device,
                        ExtBlkDevInterfaceRef& ext_blk_dev) = 0;
  };

}

#endif
