// This assertion is isolated from framework headers: their template instantiations
// can mask compiler defects in constexpr string construction.
#include "datum_key_plan_case.h"

static_assert(::datum_test::key_plan());
static_assert(::datum_test::key_query_ownership());
