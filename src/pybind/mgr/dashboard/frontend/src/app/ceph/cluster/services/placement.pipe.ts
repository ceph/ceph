import { Pipe, PipeTransform } from '@angular/core';

import _ from 'lodash';

@Pipe({
  name: 'placement'
})
export class PlacementPipe implements PipeTransform {
  /**
   * Convert the placement configuration into human readable form.
   * The output is equal to the column 'PLACEMENT' in 'ceph orch ls'.
   * @param serviceSpec The service specification to process.
   * @return The placement configuration as human readable string.
   */
  transform(serviceSpec: object | undefined): string {
    if (_.isUndefined(serviceSpec)) {
      return $localize`no spec`;
    }
    if (_.get(serviceSpec, 'unmanaged', false)) {
      return $localize`unmanaged`;
    }
    const kv: Array<any> = [];
    const hosts: Array<string> = _.get(serviceSpec, 'placement.hosts');
    const count: number = _.get(serviceSpec, 'placement.count');
    const label: string = _.get(serviceSpec, 'placement.label');
    const hostPattern: string = _.get(serviceSpec, 'placement.host_pattern');
    if (_.isArray(hosts)) {
      kv.push(...hosts);
    }
    if (_.isNumber(count)) {
      kv.push($localize`count:${count}`);
    }
    if (_.isString(label)) {
      kv.push($localize`label:${label}`);
    }
    if (_.isString(hostPattern)) {
      kv.push(hostPattern);
    }
    return kv.join(';');
  }
}
