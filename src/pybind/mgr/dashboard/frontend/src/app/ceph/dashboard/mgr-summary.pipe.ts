import { Pipe, PipeTransform } from '@angular/core';

import * as _ from 'lodash';

@Pipe({
  name: 'mgrSummary'
})
export class MgrSummaryPipe implements PipeTransform {
  transform(value: any): any {
    if (!value) {
      return '';
    }

    let activeCount = $localize`n/a`;
    const activeTitleText = _.isUndefined(value.active_name)
      ? ''
      : `${$localize`active daemon`}: ${value.active_name}`;
    // There is always one standbyreplay to replace active daemon, if active one is down
    if (activeTitleText.length > 0) {
      activeCount = '1';
    }
    const standbyHoverText = value.standbys.map((s: any): string => s.name).join(', ');
    const standbyTitleText = !standbyHoverText
      ? ''
      : `${$localize`standby daemons`}: ${standbyHoverText}`;
    const standbyCount = value.standbys.length;
    const mgrSummary = [
      {
        content: `${activeCount} ${$localize`active`}`,
        class: 'popover-info',
        titleText: activeTitleText
      }
    ];

    mgrSummary.push({
      content: '',
      class: 'card-text-line-break',
      titleText: ''
    });
    mgrSummary.push({
      content: `${standbyCount} ${$localize`standby`}`,
      class: 'popover-info',
      titleText: standbyTitleText
    });

    return mgrSummary;
  }
}
