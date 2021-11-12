import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { CrushRuleConfig } from '../models/crush-rule';

@Injectable({
  providedIn: 'root'
})
export class CrushRuleService {
  apiPath = 'api/crush_rule';

  formTooltips = {
    // Copied from /doc/rados/operations/crush-map.rst
    root: $localize`The name of the node under which data should be placed.`,
    failure_domain: $localize`The type of CRUSH nodes across which we should separate replicas.`,
    device_class: $localize`The device class data should be placed on.`
  };

  constructor(private http: HttpClient) {}

  create(rule: CrushRuleConfig) {
    return this.http.post(this.apiPath, rule, { observe: 'response' });
  }

  delete(name: string) {
    return this.http.delete(`${this.apiPath}/${name}`, { observe: 'response' });
  }

  getInfo() {
    return this.http.get(`ui-${this.apiPath}/info`);
  }
}
