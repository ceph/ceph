import * as _ from 'lodash';

export class SelectBadgesMessages {
  empty = 'There are no items.';
  selectionLimit = {
    tooltip: 'Deselect item to select again',
    text: 'Selection limit reached'
  };
  customValidations = {};
  filter = 'Filter tags';
  add = 'Add badge'; // followed by " '{{filter.value}}'"

  constructor(messages: {}) {
    _.merge(this, messages);
  }
}
