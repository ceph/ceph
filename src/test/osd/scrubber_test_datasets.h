// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/// \file data-sets used by the scrubber unit tests

#include "./scrubber_generators.h"

namespace ScrubDatasets {
/*
 * Two objects with some clones. No inconsistencies.
 */
extern ScrubGenerator::RealObjsConf minimal_snaps_configuration;

// and a part of this configuration, one that we will corrupt in a test:
extern hobject_t hobj_ms1_snp30;

// a manipulation set used in TestTScrubberBe_data_2:
extern ScrubGenerator::CorruptFuncList crpt_funcs_set1;

}  // namespace ScrubDatasets
