
#include "include/types.h"
#include "MetaRequest.h"
#include "Dentry.h"
#include "messages/MClientReply.h"

MetaRequest::~MetaRequest()
{
  if (dentry)
    dentry->put();
  if (old_dentry)
    old_dentry->put();
  if (reply)
    reply->put();
}
