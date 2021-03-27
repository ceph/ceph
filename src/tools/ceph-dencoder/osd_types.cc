#include "acconfig.h"
#include <cstdint>
using namespace std;
#include "include/ceph_features.h"

#define TYPE(t)
#define TYPE_STRAYDATA(t)
#define TYPE_NONDETERMINISTIC(t)
#define TYPE_FEATUREFUL(t)
#define TYPE_FEATUREFUL_STRAYDATA(t)
#define TYPE_FEATUREFUL_NONDETERMINISTIC(t)
#define TYPE_FEATUREFUL_NOCOPY(t)
#define TYPE_NOCOPY(t)
#define MESSAGE(t)
#include "osd_types.h"
#undef TYPE
#undef TYPE_STRAYDATA
#undef TYPE_NONDETERMINISTIC
#undef TYPE_NOCOPY
#undef TYPE_FEATUREFUL
#undef TYPE_FEATUREFUL_STRAYDATA
#undef TYPE_FEATUREFUL_NONDETERMINISTIC
#undef TYPE_FEATUREFUL_NOCOPY
#undef MESSAGE

#include "denc_registry.h"

// cannot initialize dencoders when initializing static variables, as some of
// the types are allocated using mempool, and the mempools are initialized as
// static variables.
DENC_API void register_dencoders()
{
#include "osd_types.h"
}
