import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class IscsiService {
  constructor(private http: HttpClient, private i18n: I18n) {}

  targetAdvancedSettings = [
    {
      property: 'tpg_default_cmdsn_depth',
      help: this.i18n(
        'Default CmdSN (Command Sequence Number) depth. Limits the amount ' +
          'of requests that an iSCSI initiator can have outstanding at ' +
          'any moment'
      )
    },
    {
      property: 'tpg_default_erl',
      help: this.i18n('Default error recovery level')
    },
    {
      property: 'tpg_login_timeout',
      help: this.i18n('Login timeout value in seconds')
    },
    {
      property: 'tpg_netif_timeout',
      help: this.i18n('NIC failure timeout in seconds')
    },
    {
      property: 'tpg_prod_mode_write_protect',
      help: this.i18n('If set to 1, prevent writes to LUNs')
    },
    {
      property: 'tpg_t10_pi',
      help: ''
    }
  ];

  imageAdvancedSettings = [
    {
      property: 'backstore_block_size',
      help: this.i18n('Block size of the underlying device')
    },
    {
      property: 'backstore_emulate_3pc',
      help: this.i18n('If set to 1, enable Third Party Copy')
    },
    {
      property: 'backstore_emulate_caw',
      help: this.i18n('If set to 1, enable Compare and Write')
    },
    {
      property: 'backstore_emulate_dpo',
      help: this.i18n('If set to 1, turn on Disable Page Out')
    },
    {
      property: 'backstore_emulate_fua_read',
      help: this.i18n('If set to 1, enable Force Unit Access read')
    },
    {
      property: 'backstore_emulate_fua_write',
      help: this.i18n('If set to 1, enable Force Unit Access write')
    },
    {
      property: 'backstore_emulate_model_alias',
      help: this.i18n('If set to 1, use the back-end device name for the model alias')
    },
    {
      property: 'backstore_emulate_rest_reord',
      help: this.i18n('If set to 0, the Queue Algorithm Modifier is Restricted Reordering')
    },
    {
      property: 'backstore_emulate_tas',
      help: this.i18n('If set to 1, enable Task Aborted Status')
    },
    {
      property: 'backstore_emulate_tpu',
      help: this.i18n('If set to 1, enable Thin Provisioning Unmap')
    },
    {
      property: 'backstore_emulate_tpws',
      help: this.i18n('If set to 1, enable Thin Provisioning Write Same')
    },
    {
      property: 'backstore_emulate_ua_intlck_ctrl',
      help: this.i18n('If set to 1, enable Unit Attention Interlock')
    },
    {
      property: 'backstore_emulate_write_cache',
      help: this.i18n('If set to 1, turn on Write Cache Enable')
    },
    {
      property: 'backstore_enforce_pr_isids',
      help: this.i18n('If set to 1, enforce persistent reservation ISIDs')
    },
    {
      property: 'backstore_fabric_max_sectors',
      help: this.i18n('Maximum number of sectors the fabric can transfer at once')
    },
    {
      property: 'backstore_hw_block_size',
      help: this.i18n('Hardware block size in bytes')
    },
    {
      property: 'backstore_hw_max_sectors',
      help: this.i18n('Maximum number of sectors the hardware can transfer at once')
    },
    {
      property: 'backstore_hw_pi_prot_type',
      help: this.i18n('If non-zero, DIF protection is enabled on the underlying hardware')
    },
    {
      property: 'backstore_hw_queue_depth',
      help: this.i18n('Hardware queue depth')
    },
    {
      property: 'backstore_is_nonrot',
      help: this.i18n('If set to 1, the backstore is a non rotational device')
    },
    {
      property: 'backstore_max_unmap_block_desc_count',
      help: this.i18n('Maximum number of block descriptors for UNMAP')
    },
    {
      property: 'backstore_max_unmap_lba_count',
      help: this.i18n('Maximum number of LBA for UNMAP')
    },
    {
      property: 'backstore_max_write_same_len',
      help: this.i18n('Maximum length for WRITE_SAME')
    },
    {
      property: 'backstore_optimal_sectors',
      help: this.i18n('Optimal request size in sectors')
    },
    {
      property: 'backstore_pi_prot_format',
      help: this.i18n('DIF protection format')
    },
    {
      property: 'backstore_pi_prot_type',
      help: this.i18n('DIF protection type')
    },
    {
      property: 'backstore_queue_depth',
      help: this.i18n('Queue depth')
    },
    {
      property: 'backstore_unmap_granularity',
      help: this.i18n('UNMAP granularity')
    },
    {
      property: 'backstore_unmap_granularity_alignment',
      help: this.i18n('UNMAP granularity alignment')
    }
  ];

  listTargets() {
    return this.http.get(`api/iscsi/target`);
  }

  status() {
    return this.http.get(`api/iscsi/status`);
  }

  createTarget(targetIqn) {
    return this.http.post(`api/iscsi/target`, { target_iqn: targetIqn });
  }

  deleteTarget(targetIqn) {
    return this.http.delete(`api/iscsi/target/${targetIqn}`);
  }

  createGateway(gatewayName, ipAddress) {
    return this.http.post(`api/iscsi/gateway`, {
      gateway_name: gatewayName,
      ip_address: ipAddress
    });
  }
}
