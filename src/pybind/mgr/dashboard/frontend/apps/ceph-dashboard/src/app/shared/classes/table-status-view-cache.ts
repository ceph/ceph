import { ViewCacheStatus } from '../enum/view-cache-status.enum';
import { TableStatus } from './table-status';

export class TableStatusViewCache extends TableStatus {
  constructor(status: ViewCacheStatus = ViewCacheStatus.ValueOk, statusFor: string = '') {
    super();

    switch (status) {
      case ViewCacheStatus.ValueOk:
        this.type = 'ghost';
        this.msg = '';
        break;
      case ViewCacheStatus.ValueNone:
        this.type = 'primary';
        this.msg =
          (statusFor ? $localize`Retrieving data for ${statusFor}.` : $localize`Retrieving data.`) +
          ' ' +
          $localize`Please wait...`;
        break;
      case ViewCacheStatus.ValueStale:
        this.type = 'secondary';
        this.msg = statusFor
          ? $localize`Displaying previously cached data for ${statusFor}.`
          : $localize`Displaying previously cached data.`;
        break;
      case ViewCacheStatus.ValueException:
        this.type = 'danger';
        this.msg =
          (statusFor
            ? $localize`Could not load data for ${statusFor}.`
            : $localize`Could not load data.`) +
          ' ' +
          $localize`Please check the cluster health.`;
        break;
    }
  }
}
