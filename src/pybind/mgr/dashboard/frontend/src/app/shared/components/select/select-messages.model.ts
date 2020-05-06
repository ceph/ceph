import * as _ from 'lodash';

export class SelectMessages {
  empty: string;
  selectionLimit: any;
  customValidations = {};
  filter: string;
  add: string;
  noOptions: string;

  constructor(messages: {}) {
    this.empty = $localize`No items selected.`;
    this.selectionLimit = {
      tooltip: $localize`Deselect item to select again`,
      text: $localize`Selection limit reached`
    };
    this.filter = $localize`Filter tags`;
    this.add = $localize`Add badge`; // followed by " '{{filter.value}}'"
    this.noOptions = $localize`There are no items available.`;

    _.merge(this, messages);
  }
}
