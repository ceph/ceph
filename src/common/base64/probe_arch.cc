/*
 * Copyright (C) 2020 Intel Corporation.
 * All rights reserved.
 *
 * Author: Changcheng Liu<changcheng.liu@intel.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "probe_arch.h"
#include "intel_simd.h"

int ceph_probe_arch(void) {
  static int arch_probed = 0;
  if (arch_probed) {
    return 1;
  }

  #if defined(__i386__) || defined(__x86_64__)
  spec_arch_intel_probe();
  #endif

  arch_probed = 1;
  return 1;
}
