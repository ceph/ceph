import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { ErasureCodeProfile } from '../models/erasure-code-profile';

@Injectable({
  providedIn: 'root'
})
export class ErasureCodeProfileService {
  apiPath = 'api/erasure_code_profile';

  formTooltips = {
    // Copied from /doc/rados/operations/erasure-code.*.rst
    k: $localize`Each object is split in data-chunks parts, each stored on a different OSD.`,

    m: $localize`Compute coding chunks for each object and store them on different OSDs.
      The number of coding chunks is also the number of OSDs that can be down without losing data.`,

    plugins: {
      jerasure: {
        description: $localize`The jerasure plugin is the most generic and flexible plugin,
          it is also the default for Ceph erasure coded pools.`,
        technique: $localize`The more flexible technique is reed_sol_van : it is enough to set k
          and m. The cauchy_good technique can be faster but you need to chose the packetsize
          carefully. All of reed_sol_r6_op, liberation, blaum_roth, liber8tion are RAID6 equivalents
          in the sense that they can only be configured with m=2.`,
        packetSize: $localize`The encoding will be done on packets of bytes size at a time.
          Choosing the right packet size is difficult.
          The jerasure documentation contains extensive information on this topic.`
      },
      lrc: {
        description: $localize`With the jerasure plugin, when an erasure coded object is stored on
          multiple OSDs, recovering from the loss of one OSD requires reading from all the others.
          For instance if jerasure is configured with k=8 and m=4, losing one OSD requires reading
          from the eleven others to repair.

          The lrc erasure code plugin creates local parity chunks to be able to recover using
          less OSDs. For instance if lrc is configured with k=8, m=4 and l=4, it will create
          an additional parity chunk for every four OSDs. When a single OSD is lost, it can be
          recovered with only four OSDs instead of eleven.`,
        l: $localize`Group the coding and data chunks into sets of size locality. For instance,
          for k=4 and m=2, when locality=3 two groups of three are created. Each set can
          be recovered without reading chunks from another set.`,
        crushLocality: $localize`The type of the crush bucket in which each set of chunks defined
          by l will be stored. For instance, if it is set to rack, each group of l chunks will be
          placed in a different rack. It is used to create a CRUSH rule step such as step choose
          rack. If it is not set, no such grouping is done.`
      },
      isa: {
        description: $localize`The isa plugin encapsulates the ISA library. It only runs on Intel processors.`,
        technique: $localize`The ISA plugin comes in two Reed Solomon forms.
          If reed_sol_van is set, it is Vandermonde, if cauchy is set, it is Cauchy.`
      },
      shec: {
        description: $localize`The shec plugin encapsulates the multiple SHEC library.
          It allows ceph to recover data more efficiently than Reed Solomon codes.`,
        c: $localize`The number of parity chunks each of which includes each data chunk in its
          calculation range. The number is used as a durability estimator. For instance, if c=2,
          2 OSDs can be down without losing data.`
      },
      clay: {
        description: $localize`CLAY (short for coupled-layer) codes are erasure codes designed to
          bring about significant savings in terms of network bandwidth and disk IO when a failed
          node/OSD/rack is being repaired.`,
        d: $localize`Number of OSDs requested to send data during recovery of a single chunk.
          d needs to be chosen such that k+1 <= d <= k+m-1. The larger the d, the better
          the savings.`,
        scalar_mds: $localize`scalar_mds specifies the plugin that is used as a building block
          in the layered construction. It can be one of jerasure, isa, shec.`,
        technique: $localize`technique specifies the technique that will be picked
          within the 'scalar_mds' plugin specified. Supported techniques
          are 'reed_sol_van', 'reed_sol_r6_op', 'cauchy_orig',
          'cauchy_good', 'liber8tion' for jerasure, 'reed_sol_van',
          'cauchy' for isa and 'single', 'multiple' for shec.`
      }
    },

    crushRoot: $localize`The name of the crush bucket used for the first step of the CRUSH rule.
      For instance step take default.`,

    crushFailureDomain: $localize`Ensure that no two chunks are in a bucket with the same failure
      domain. For instance, if the failure domain is host no two chunks will be stored on the same
      host. It is used to create a CRUSH rule step such as step chooseleaf host.`,

    crushNumFailureDomains: $localize` Number of failure domains to map. Results in a CRUSH MSR rule being created.
    Must be specified if crush-osds-per-failure-domain is specified.`,

    crushOsdsPerFailureDomain: $localize`Maximum number of OSDs to place in each failure domain --
     defaults to 1. Using a value greater than one will cause a CRUSH MSR rule to be created.
      Must be specified if crush-num-failure-domains is specified.`,

    crushDeviceClass: $localize`The device class on which to place data.`,

    directory: $localize`Set the directory name from which the erasure code plugin is loaded.`
  };

  constructor(private http: HttpClient) {}

  list(): Observable<ErasureCodeProfile[]> {
    return this.http.get<ErasureCodeProfile[]>(this.apiPath);
  }

  create(ecp: ErasureCodeProfile) {
    return this.http.post(this.apiPath, ecp, { observe: 'response' });
  }

  delete(name: string) {
    return this.http.delete(`${this.apiPath}/${name}`, { observe: 'response' });
  }

  getInfo() {
    return this.http.get(`ui-${this.apiPath}/info`);
  }
}
