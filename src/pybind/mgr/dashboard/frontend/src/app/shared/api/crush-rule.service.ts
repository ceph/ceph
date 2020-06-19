import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { CrushRuleConfig } from '../models/crush-rule';

@Injectable({
  providedIn: 'root'
})
export class CrushRuleService {
  apiPath = 'api/crush_rule';

  formTooltips = {
    // Copied from /doc/rados/operations/crush-map.rst
    root: this.i18n(`The name of the node under which data should be placed.`),
    failure_domain: this.i18n(`The type of CRUSH nodes across which we should separate replicas.`),
    device_class: this.i18n(`The device class data should be placed on.`)
  };

  constructor(private http: HttpClient, private i18n: I18n) {}

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
