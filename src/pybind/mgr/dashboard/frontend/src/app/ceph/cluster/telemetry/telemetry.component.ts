import { Component, OnInit } from '@angular/core';
import { ValidatorFn, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BlockUI, NgBlockUI } from 'ng-block-ui';
import { forkJoin as observableForkJoin } from 'rxjs';

import { MgrModuleService } from '../../../shared/api/mgr-module.service';
import { TelemetryService } from '../../../shared/api/telemetry.service';
import { NotificationType } from '../../../shared/enum/notification-type.enum';
import { CdFormBuilder } from '../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { CdValidators } from '../../../shared/forms/cd-validators';
import { NotificationService } from '../../../shared/services/notification.service';
import { TelemetryNotificationService } from '../../../shared/services/telemetry-notification.service';
import { TextToDownloadService } from '../../../shared/services/text-to-download.service';

@Component({
  selector: 'cd-telemetry',
  templateUrl: './telemetry.component.html',
  styleUrls: ['./telemetry.component.scss']
})
export class TelemetryComponent implements OnInit {
  @BlockUI()
  blockUI: NgBlockUI;

  error = false;
  configForm: CdFormGroup;
  licenseAgrmt = false;
  loading = false;
  moduleEnabled: boolean;
  options: Object = {};
  newConfig: Object = {};
  configResp: object = {};
  previewForm: CdFormGroup;
  requiredFields = [
    'channel_basic',
    'channel_crash',
    'channel_device',
    'channel_ident',
    'interval',
    'proxy',
    'contact',
    'description',
    'organization'
  ];
  contactInfofields = ['contact', 'description', 'organization'];
  report: object = undefined;
  reportId: number = undefined;
  sendToUrl = '';
  sendToDeviceUrl = '';
  step = 1;
  showContactInfo: boolean;

  constructor(
    private formBuilder: CdFormBuilder,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private router: Router,
    private telemetryService: TelemetryService,
    private i18n: I18n,
    private textToDownloadService: TextToDownloadService,
    private telemetryNotificationService: TelemetryNotificationService
  ) {}

  ngOnInit() {
    this.loading = true;
    const observables = [
      this.mgrModuleService.getOptions('telemetry'),
      this.mgrModuleService.getConfig('telemetry')
    ];
    observableForkJoin(observables).subscribe(
      (resp: object) => {
        const configResp = resp[1];
        this.moduleEnabled = configResp['enabled'];
        this.sendToUrl = configResp['url'];
        this.sendToDeviceUrl = configResp['device_url'];
        this.showContactInfo = configResp['channel_ident'];
        this.options = _.pick(resp[0], this.requiredFields);
        this.configResp = _.pick(configResp, this.requiredFields);
        this.createConfigForm();
        this.configForm.setValue(this.configResp);
        this.loading = false;
      },
      (_error) => {
        this.error = true;
      }
    );
  }

  private createConfigForm() {
    const controlsConfig = {};
    _.forEach(Object.values(this.options), (option) => {
      controlsConfig[option.name] = [option.default_value, this.getValidators(option)];
    });
    this.configForm = this.formBuilder.group(controlsConfig);
  }

  private createPreviewForm() {
    const controls = {
      report: JSON.stringify(this.report, null, 2),
      reportId: this.reportId,
      licenseAgrmt: [this.licenseAgrmt, Validators.requiredTrue]
    };
    this.previewForm = this.formBuilder.group(controls);
  }

  private getValidators(option: any): ValidatorFn[] {
    const result = [];
    switch (option.type) {
      case 'int':
        result.push(CdValidators.number());
        result.push(Validators.required);
        if (_.isNumber(option.min)) {
          result.push(Validators.min(option.min));
        }
        if (_.isNumber(option.max)) {
          result.push(Validators.max(option.max));
        }
        break;
      case 'str':
        if (_.isNumber(option.min)) {
          result.push(Validators.minLength(option.min));
        }
        if (_.isNumber(option.max)) {
          result.push(Validators.maxLength(option.max));
        }
        break;
    }
    return result;
  }

  private updateReportFromConfig(updatedConfig: Object = {}) {
    // update channels
    const availableChannels: string[] = this.report['report']['channels_available'];
    const updatedChannels = [];
    for (const channel of availableChannels) {
      const key = `channel_${channel}`;
      if (updatedConfig[key]) {
        updatedChannels.push(channel);
      }
    }
    this.report['report']['channels'] = updatedChannels;
    // update contactInfo
    for (const contactInfofield of this.contactInfofields) {
      this.report['report'][contactInfofield] = updatedConfig[contactInfofield];
    }
  }

  private getReport() {
    this.loading = true;
    this.telemetryService.getReport().subscribe(
      (resp: object) => {
        this.report = resp;
        this.reportId = resp['report']['report_id'];
        this.updateReportFromConfig(this.newConfig);
        this.createPreviewForm();
        this.loading = false;
        this.step++;
      },
      (_error) => {
        this.error = true;
      }
    );
  }

  toggleIdent() {
    this.showContactInfo = !this.showContactInfo;
  }

  buildReport() {
    this.newConfig = {};
    for (const option of Object.values(this.options)) {
      const control = this.configForm.get(option.name);
      // Append the option only if they are valid
      if (control.valid) {
        this.newConfig[option.name] = control.value;
      } else {
        this.configForm.setErrors({ cdSubmitButton: true });
        return;
      }
    }
    // reset contact info field  if ident channel is off
    if (!this.newConfig['channel_ident']) {
      for (const contactInfofield of this.contactInfofields) {
        this.newConfig[contactInfofield] = '';
      }
    }
    this.getReport();
  }

  download(report: object, fileName: string) {
    this.textToDownloadService.download(JSON.stringify(report, null, 2), fileName);
  }

  disableModule(message: string = null, followUpFunc: Function = null) {
    this.telemetryService.enable(false).subscribe(() => {
      this.telemetryNotificationService.setVisibility(true);
      if (message) {
        this.notificationService.show(NotificationType.success, message);
      }
      if (followUpFunc) {
        followUpFunc();
      } else {
        this.router.navigate(['']);
      }
    });
  }

  next() {
    this.buildReport();
  }

  back() {
    this.step--;
  }

  getChangedConfig() {
    const updatedConfig = {};
    _.forEach(this.requiredFields, (configField) => {
      if (!_.isEqual(this.configResp[configField], this.newConfig[configField])) {
        updatedConfig[configField] = this.newConfig[configField];
      }
    });
    return updatedConfig;
  }

  onSubmit() {
    const updatedConfig = this.getChangedConfig();
    const observables = [
      this.telemetryService.enable(),
      this.mgrModuleService.updateConfig('telemetry', updatedConfig)
    ];

    observableForkJoin(observables).subscribe(
      () => {
        this.telemetryNotificationService.setVisibility(false);
        this.notificationService.show(
          NotificationType.success,
          this.i18n('The Telemetry module has been configured and activated successfully.')
        );
      },
      () => {
        this.telemetryNotificationService.setVisibility(false);
        this.notificationService.show(
          NotificationType.error,
          this.i18n(
            'An Error occurred while updating the Telemetry module configuration.\
             Please Try again'
          )
        );
        // Reset the 'Update' button.
        this.previewForm.setErrors({ cdSubmitButton: true });
      },
      () => {
        this.newConfig = {};
        this.router.navigate(['']);
      }
    );
  }
}
