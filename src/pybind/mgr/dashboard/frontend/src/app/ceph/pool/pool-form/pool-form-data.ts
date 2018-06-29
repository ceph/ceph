import { Validators } from '@angular/forms';

import { SelectBadgesMessages } from '../../../shared/components/select-badges/select-badges-messages.model';
import { SelectBadgesOption } from '../../../shared/components/select-badges/select-badges-option.model';
import { Pool } from '../pool';

export class PoolFormData {
  poolTypes = ['erasure', 'replicated'];
  applications = {
    selected: [],
    available: [
      new SelectBadgesOption(false, 'cephfs', ''),
      new SelectBadgesOption(false, 'rbd', ''),
      new SelectBadgesOption(false, 'rgw', '')
    ],
    validators: [Validators.pattern('[A-Za-z0-9_]+'), Validators.maxLength(128)],
    messages: new SelectBadgesMessages({
      empty: 'No applications added',
      selectionLimit: {
        text: 'Applications limit reached',
        tooltip: 'A pool can only have up to four applications definitions.'
      },
      customValidations: {
        pattern: `Allowed characters '_a-zA-Z0-9'`,
        maxlength: 'Maximum length is 128 characters'
      },
      filter: 'Filter or add applications',
      add: 'Add application'
    })
  };
  pgs = 1;
  pool: Pool; // Only available during edit mode
}
