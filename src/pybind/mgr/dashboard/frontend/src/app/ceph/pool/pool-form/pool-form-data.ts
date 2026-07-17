import { Validators } from '@angular/forms';

import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { Pool, PoolType } from '../pool';

export class PoolFormData {
  poolTypes: string[];
  erasureInfo = false;
  crushInfo = false;
  applications: any;

  readonly APP_LABELS: Record<string, string> = {
    cephfs: $localize`File system (CephFS)`,
    rbd: $localize`Block (RBD)`,
    rgw: $localize`Object (RGW)`
  };

  constructor() {
    this.poolTypes = [PoolType.ERASURE, PoolType.REPLICATED];
    this.applications = {
      selected: [],
      default: ['cephfs', 'rbd', 'rgw'],
      available: [], // Filled during runtime
      validators: [Validators.pattern('[A-Za-z0-9_]+'), Validators.maxLength(128)],
      messages: new SelectMessages({
        empty: $localize`No applications added`,
        selectionLimit: {
          text: $localize`Applications limit reached`,
          tooltip: $localize`A pool can only have up to four applications definitions.`
        },
        customValidations: {
          pattern: $localize`Allowed characters '_a-zA-Z0-9'`,
          maxlength: $localize`Maximum length is 128 characters`
        },
        filter: $localize`Filter or add applications`,
        add: $localize`Add application`
      })
    };
  }

  pgs = 1;
  pool: Pool; // Only available during edit mode
}
