
#include "MOSDOp.h"
#include "osd/OSD.h"

#include "config.h"

/* need to update read/write flags for class method operations */
void MOSDOp::update_flags(OSD *osd)
{
  flags = osd->get_op_flags(this);
}
