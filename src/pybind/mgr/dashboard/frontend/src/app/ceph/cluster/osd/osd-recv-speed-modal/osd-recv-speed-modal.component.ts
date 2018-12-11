import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin, of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { ConfigurationService } from '../../../../shared/api/configuration.service';
import { OsdService } from '../../../../shared/api/osd.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { NotificationService } from '../../../../shared/services/notification.service';

@Component({
  selector: 'cd-osd-recv-speed-modal',
  templateUrl: './osd-recv-speed-modal.component.html',
  styleUrls: ['./osd-recv-speed-modal.component.scss']
})
export class OsdRecvSpeedModalComponent implements OnInit {
  osdRecvSpeedForm: CdFormGroup;
  priorities = [];
  priorityAttrs = {};

  constructor(
    public bsModalRef: BsModalRef,
    private configService: ConfigurationService,
    private notificationService: NotificationService,
    private i18n: I18n,
    private osdService: OsdService
  ) {
    this.priorities = this.osdService.osdRecvSpeedModalPriorities.KNOWN_PRIORITIES;
    this.osdRecvSpeedForm = new CdFormGroup({
      priority: new FormControl(null, { validators: [Validators.required] }),
      customizePriority: new FormControl(false)
    });
    this.priorityAttrs = {
      osd_max_backfills: {
        text: this.i18n('Max Backfills'),
        desc: ''
      },
      osd_recovery_max_active: {
        text: this.i18n('Recovery Max Active'),
        desc: ''
      },
      osd_recovery_max_single_start: {
        text: this.i18n('Recovery Max Single Start'),
        desc: ''
      },
      osd_recovery_sleep: {
        text: this.i18n('Recovery Sleep'),
        desc: ''
      }
    };

    Object.keys(this.priorityAttrs).forEach((configOptionName) => {
      this.osdRecvSpeedForm.addControl(
        configOptionName,
        new FormControl(null, { validators: [Validators.required] })
      );
    });
  }

  ngOnInit() {
    const observables = [];
    Object.keys(this.priorityAttrs).forEach((configOptionName) => {
      observables.push(this.configService.get(configOptionName));
    });

    observableForkJoin(observables)
      .pipe(
        mergeMap((configOptions) => {
          const result = { values: {}, configOptions: [] };
          configOptions.forEach((configOption) => {
            result.configOptions.push(configOption);

            if (configOption && 'value' in configOption) {
              configOption.value.forEach((value) => {
                if (value['section'] === 'osd') {
                  result.values[configOption.name] = Number(value.value);
                }
              });
            }
          });
          return of(result);
        })
      )
      .subscribe((resp) => {
        this.getStoredPriority(resp.values, (priority) => {
          this.setPriority(priority);
        });
        this.setDescription(resp.configOptions);
      });
  }

  getStoredPriority(configOptionValues: any, callbackFn: Function) {
    const priority = _.find(this.priorities, (p) => {
      return _.isEqual(p.values, configOptionValues);
    });

    this.osdRecvSpeedForm.controls.customizePriority.setValue(false);

    if (priority) {
      return callbackFn(priority);
    }

    if (Object.entries(configOptionValues).length === 4) {
      this.osdRecvSpeedForm.controls.customizePriority.setValue(true);
      return callbackFn(
        Object({ name: 'custom', text: this.i18n('Custom'), values: configOptionValues })
      );
    }

    return callbackFn(this.priorities[0]);
  }

  setDescription(configOptions: Array<any>) {
    configOptions.forEach((configOption) => {
      if (configOption.desc !== '') {
        this.priorityAttrs[configOption.name].desc = configOption.desc;
      }
    });
  }

  setPriority(priority: any) {
    const customPriority = _.find(this.priorities, (p) => {
      return p.name === 'custom';
    });

    if (priority.name === 'custom') {
      if (!customPriority) {
        this.priorities.push(priority);
      }
    } else {
      if (customPriority) {
        this.priorities.splice(this.priorities.indexOf(customPriority), 1);
      }
    }

    this.osdRecvSpeedForm.controls.priority.setValue(priority.name);
    Object.entries(priority.values).forEach(([name, value]) => {
      this.osdRecvSpeedForm.controls[name].setValue(value);
    });
  }

  onCustomizePriorityChange() {
    if (this.osdRecvSpeedForm.getValue('customizePriority')) {
      const values = {};
      Object.keys(this.priorityAttrs).forEach((configOptionName) => {
        values[configOptionName] = this.osdRecvSpeedForm.getValue(configOptionName);
      });
      const customPriority = {
        name: 'custom',
        text: this.i18n('Custom'),
        values: values
      };
      this.setPriority(customPriority);
    } else {
      Object.keys(this.priorityAttrs).forEach((configOptionName) => {
        this.osdRecvSpeedForm.get(configOptionName).reset();
      });
      this.setPriority(this.priorities[0]);
    }
  }

  onPriorityChange(selectedPriorityName) {
    const selectedPriority =
      _.find(this.priorities, (p) => {
        return p.name === selectedPriorityName;
      }) || this.priorities[0];
    this.setPriority(selectedPriority);
  }

  submitAction() {
    const options = {};
    Object.keys(this.priorityAttrs).forEach((configOptionName) => {
      options[configOptionName] = {
        section: 'osd',
        value: this.osdRecvSpeedForm.getValue(configOptionName)
      };
    });

    this.configService.bulkCreate({ options: options }).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          this.i18n('OSD recovery speed priority "{{value}}" was set successfully.', {
            value: this.osdRecvSpeedForm.getValue('priority')
          }),
          this.i18n('OSD recovery speed priority')
        );
        this.bsModalRef.hide();
      },
      () => {
        this.bsModalRef.hide();
      }
    );
  }
}
