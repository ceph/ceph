import { I18n } from '@ngx-translate/i18n-polyfill';

import * as _ from 'lodash';

export class SelectMessages {
  i18n: I18n;

  empty: string;
  selectionLimit: any;
  customValidations = {};
  filter: string;
  add: string;
  noOptions: string;

  constructor(messages: {}, i18n: I18n) {
    this.i18n = i18n;

    this.empty = this.i18n('No items selected.');
    this.selectionLimit = {
      tooltip: this.i18n('Deselect item to select again'),
      text: this.i18n('Selection limit reached')
    };
    this.filter = this.i18n('Filter tags');
    this.add = this.i18n('Add badge'); // followed by " '{{filter.value}}'"
    this.noOptions = this.i18n('There are no items available.');

    _.merge(this, messages);
  }
}
