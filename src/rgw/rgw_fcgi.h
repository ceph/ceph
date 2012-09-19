#ifndef CEPH_RGW_FCGI_H
#define CEPH_RGW_FCGI_H

#include "rgw_client_io.h"


struct FCGX_Request;


class RGWFCGX : public RGWClientIO
{
  FCGX_Request *fcgx;
protected:
  int write_data(const char *buf, int len);
  int read_data(char *buf, int len);

public:
  RGWFCGX(FCGX_Request *_fcgx) : fcgx(_fcgx) {}
  void flush();
  const char **envp();
};


#endif
