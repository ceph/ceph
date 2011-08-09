
#include "MetaRequest.h"
#include "Dentry.h"

MetaRequest::~MetaRequest()
{
  if (dentry)
    dentry->put();
  if (old_dentry)
    old_dentry->put();
}
