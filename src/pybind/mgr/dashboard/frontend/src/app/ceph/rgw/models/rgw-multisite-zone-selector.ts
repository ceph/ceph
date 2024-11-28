import { Validators } from '@angular/forms';
import { SelectMessages } from '~/app/shared/components/select/select-messages.model';
import { SelectOption } from '~/app/shared/components/select/select-option.model';

interface Zone {
  selected: string[];
  available: SelectOption[];
  validators: any[];
  messages: SelectMessages;
}

export class ZoneData {
  data: Zone;
  customBadges: boolean;

  constructor(customBadges: boolean = false, filterMsg: string) {
    this.customBadges = customBadges;
    this.data = {
      selected: [],
      available: [],
      validators: [Validators.pattern('[A-Za-z0-9_-]+|\\*'), Validators.maxLength(50)],
      messages: new SelectMessages({
        empty: $localize`No zones added`,
        customValidations: {
          pattern: $localize`Allowed characters '-_a-zA-Z0-9|*'`,
          maxlength: $localize`Maximum length is 50 characters`
        },
        filter: $localize`${filterMsg}`,
        add: $localize`Add zone`
      })
    };
  }
}
