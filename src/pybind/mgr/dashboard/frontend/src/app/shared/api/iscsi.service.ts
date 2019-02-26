import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode } from '../decorators/cd-encode';
import { ApiModule } from './api.module';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class IscsiService {
  constructor(private http: HttpClient) {}

  targetAdvancedSettings = {
    cmdsn_depth: {
      help: ''
    },
    dataout_timeout: {
      help: ''
    },
    first_burst_length: {
      help: ''
    },
    immediate_data: {
      help: ''
    },
    initial_r2t: {
      help: ''
    },
    max_burst_length: {
      help: ''
    },
    max_outstanding_r2t: {
      help: ''
    },
    max_recv_data_segment_length: {
      help: ''
    },
    max_xmit_data_segment_length: {
      help: ''
    },
    nopin_response_timeout: {
      help: ''
    },
    nopin_timeout: {
      help: ''
    }
  };

  imageAdvancedSettings = {
    hw_max_sectors: {
      help: ''
    },
    max_data_area_mb: {
      help: ''
    },
    osd_op_timeout: {
      help: ''
    },
    qfull_timeout: {
      help: ''
    }
  };

  listTargets() {
    return this.http.get(`api/iscsi/target`);
  }

  getTarget(target_iqn) {
    return this.http.get(`api/iscsi/target/${target_iqn}`);
  }

  updateTarget(target_iqn, target) {
    return this.http.put(`api/iscsi/target/${target_iqn}`, target, { observe: 'response' });
  }

  status() {
    return this.http.get(`ui-api/iscsi/status`);
  }

  settings() {
    return this.http.get(`ui-api/iscsi/settings`);
  }

  portals() {
    return this.http.get(`ui-api/iscsi/portals`);
  }

  createTarget(target) {
    return this.http.post(`api/iscsi/target`, target, { observe: 'response' });
  }

  deleteTarget(target_iqn) {
    return this.http.delete(`api/iscsi/target/${target_iqn}`, { observe: 'response' });
  }

  getDiscovery() {
    return this.http.get(`api/iscsi/discoveryauth`);
  }

  updateDiscovery(auth) {
    return this.http.put(`api/iscsi/discoveryauth`, auth);
  }
}
