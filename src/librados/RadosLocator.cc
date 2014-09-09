/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

// -----------------------------------------------------------------------------
#include "librados/IoCtxImpl.h"
#include "include/rados/librados.hpp"
#include "RadosLocator.h"
// -----------------------------------------------------------------------------

/**
 * @file RadosLocator.cc
 *
 * @brief Class providing a function to retrieve a vector of locations
 *
 * The 'locate' function fills a vector storing the IP, OSD- & PG-Seeds for
 * a given object in a given IO context (pool).
 */

// -----------------------------------------------------------------------------
void
librados::Location::resolve()
{
  std::string tcmd="dig -x "; tcmd += IpString; tcmd += " +short 2>/dev/null";
  FILE* fd = popen(tcmd.c_str(),"r");
  if (fd) {
    char response[256];
    response[0]=0;
    size_t nread = fread(response,1, sizeof(response),fd);
    if (nread>1)
      response[nread-2]=0;
    if (strlen(response))
      HostName = response;
    fclose(fd);
  }
}

// -----------------------------------------------------------------------------
std::string
librados::Location::dump(bool showhost) const
{
  char p[1024];
  if (showhost) {
    snprintf(p, sizeof (p),
	     "osd-id=%04ld pg-seed=%04ld ip=%s hostname=%s",
	     OsdID,
	     PgSeed,
	     IpString.c_str(),
	     HostName.length() ? HostName.c_str() : "<unresolved>");
  } else {
    snprintf(p, sizeof (p),
	     "osd-id=%04ld pg-seed=%04ld ip=%s",
	     OsdID,
	     PgSeed,
	     IpString.c_str());
  }
  return std::string(p);
}

// -----------------------------------------------------------------------------
bool
librados::RadosLocator::locate(const std::string &oname,
                               location_vector_t &locations
                               )
{
  librados::IoCtxImpl *ctx = io.io_ctx_impl;
  object_t oid(oname.c_str());

  // retrieve OSD map
  const OSDMap* osdmap = ctx->objecter->get_osdmap_read();
  // check if given pool is defined in the OSD map
  int64_t pool = ctx->poolid;
  if (!osdmap->have_pg_pool(pool)) {
    if (osdmap)
      ctx->objecter->put_osdmap_read();
    return false;
  }

  object_locator_t loc(pool);
  pg_t raw_pgid = osdmap->object_locator_to_pg(oid, loc);
  pg_t pgid = osdmap->raw_pg_to_pg(raw_pgid);

  vector<int> acting;
  osdmap->pg_to_acting_osds(pgid, acting);

  // fill and translate all given locations into a location vector
  for (size_t i = 0; i < acting.size(); i++) {
    std::stringstream hostaddr;
    entity_addr_t addr = osdmap->get_addr(acting[i]);
    hostaddr << addr.ss_addr();
    std::string host = hostaddr.str().c_str();
    host.erase(host.rfind(":"));

    location_t new_location;
    new_location.IpString = host;
    new_location.OsdID = acting[i];
    new_location.PgSeed = pgid.ps();
    locations.push_back(new_location);
  }

  ctx->objecter->put_osdmap_read();
  return true;
}


bool
librados::Rados::locate(IoCtx &ioctx, const std::string &oid, location_vector_t &locations)
{
  RadosLocator locator(ioctx);
  return locator.locate(oid,locations);
}

std::string
librados::Rados::dump_location(location_t &location, bool reversedns)
{
  if (reversedns)
    location.resolve();
  return location.dump(reversedns);
}
