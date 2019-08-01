import * as _ from 'lodash';

import { IndividualConfig } from 'ngx-toastr';
import { ToastComponent } from '../components/toast/toast.component';
import { NotificationType } from '../enum/notification-type.enum';

export interface CdIndividualConfig extends IndividualConfig {
  application: string;
  type: NotificationType;
  timestamp: string;
  isPermanent: boolean;
  errorCode: number;
}

export class CdNotificationConfig {
  constructor(
    public type: NotificationType = NotificationType.info,
    public title?: string,
    public message?: string, // Use this for additional information only
    public options?: any | CdIndividualConfig,
    public application: string = 'Ceph',
    public errorCode?: number,
    public isPermanent: boolean = false
  ) {
    const minimalCdConf = this.getMinimalCdConfig();
    this.options = this.options ? _.merge(this.options, minimalCdConf) : minimalCdConf;
    if (this.isPermanent) {
      this.options = _.merge(this.options, this.getPermanentConfig());
    }
  }

  getPermanentConfig(): Partial<CdIndividualConfig> {
    return {
      disableTimeOut: true,
      positionClass: 'toast-top-full-width',
      tapToDismiss: false
    };
  }

  getMinimalCdConfig(): Partial<CdIndividualConfig> {
    return {
      application: this.application,
      toastComponent: ToastComponent,
      type: this.type,
      timestamp: new Date().toJSON(),
      isPermanent: this.isPermanent,
      errorCode: this.errorCode
    };
  }
}
