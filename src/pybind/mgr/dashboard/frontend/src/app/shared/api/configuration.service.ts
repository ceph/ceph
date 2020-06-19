import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ConfigFormCreateRequestModel } from '../../ceph/cluster/configuration/configuration-form/configuration-form-create-request.model';

@Injectable({
  providedIn: 'root'
})
export class ConfigurationService {
  constructor(private http: HttpClient) {}

  private findValue(config: any, section: string) {
    if (!config.value) {
      return undefined;
    }
    return config.value.find((v: any) => v.section === section);
  }

  getValue(config: any, section: string) {
    let val = this.findValue(config, section);
    if (!val) {
      const indexOfDot = section.indexOf('.');
      if (indexOfDot !== -1) {
        val = this.findValue(config, section.substring(0, indexOfDot));
      }
    }
    if (!val) {
      val = this.findValue(config, 'global');
    }
    if (val) {
      return val.value;
    }
    return config.default;
  }

  getConfigData() {
    return this.http.get('api/cluster_conf/');
  }

  get(configOption: string) {
    return this.http.get(`api/cluster_conf/${configOption}`);
  }

  filter(configOptionNames: Array<string>) {
    return this.http.get(`api/cluster_conf/filter?names=${configOptionNames.join(',')}`);
  }

  create(configOption: ConfigFormCreateRequestModel) {
    return this.http.post('api/cluster_conf/', configOption);
  }

  delete(configOption: string, section: string) {
    return this.http.delete(`api/cluster_conf/${configOption}?section=${section}`);
  }

  bulkCreate(configOptions: object) {
    return this.http.put('api/cluster_conf/', configOptions);
  }
}
