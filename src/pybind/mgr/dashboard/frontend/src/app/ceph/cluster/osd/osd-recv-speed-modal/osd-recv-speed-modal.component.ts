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
import { ConfigOptionTypes } from '../../configuration/configuration-form/configuration-form.types';

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
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
      },
      osd_recovery_max_active: {
        text: this.i18n('Recovery Max Active'),
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
      },
      osd_recovery_max_single_start: {
        text: this.i18n('Recovery Max Single Start'),
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
      },
      osd_recovery_sleep: {
        text: this.i18n('Recovery Sleep'),
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
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
          return of(this.getCurrentValues(configOptions));
        })
      )
      .subscribe((resp) => {
        this.detectPriority(resp.values, (priority) => {
          this.setPriority(priority);
        });
        this.setDescription(resp.configOptions);
        this.setValidators(resp.configOptions);
      });
  }

  detectPriority(configOptionValues: any, callbackFn: Function) {
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

  getCurrentValues(configOptions: any) {
    const currentValues = { values: {}, configOptions: [] };
    configOptions.forEach((configOption) => {
      currentValues.configOptions.push(configOption);

      if ('value' in configOption) {
        configOption.value.forEach((value) => {
          if (value.section === 'osd') {
            currentValues.values[configOption.name] = Number(value.value);
          }
        });
      } else if ('default' in configOption && configOption.default !== null) {
        currentValues.values[configOption.name] = Number(configOption.default);
      }
    });
    return currentValues;
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

  setValidators(configOptions: Array<any>) {
    configOptions.forEach((configOption) => {
      const typeValidators = ConfigOptionTypes.getTypeValidators(configOption);
      if (typeValidators) {
        typeValidators.validators.push(Validators.required);

        if ('max' in typeValidators && typeValidators.max !== '') {
          this.priorityAttrs[configOption.name].maxValue = typeValidators.max;
        }

        if ('min' in typeValidators && typeValidators.min !== '') {
          this.priorityAttrs[configOption.name].minValue = typeValidators.min;
        }

        this.priorityAttrs[configOption.name].patternHelpText = typeValidators.patternHelpText;
        this.osdRecvSpeedForm.controls[configOption.name].setValidators(typeValidators.validators);
      } else {
        this.osdRecvSpeedForm.controls[configOption.name].setValidators(Validators.required);
      }
    });
  }

  onCustomizePriorityChange() {
    const values = {};
    Object.keys(this.priorityAttrs).forEach((configOptionName) => {
      values[configOptionName] = this.osdRecvSpeedForm.getValue(configOptionName);
    });

    if (this.osdRecvSpeedForm.getValue('customizePriority')) {
      const customPriority = {
        name: 'custom',
        text: this.i18n('Custom'),
        values: values
      };
      this.setPriority(customPriority);
    } else {
      this.detectPriority(values, (priority) => {
        this.setPriority(priority);
      });
    }
  }

  onPriorityChange(selectedPriorityName) {
    const selectedPriority =
      _.find(this.priorities, (p) => {
        return p.name === selectedPriorityName;
      }) || this.priorities[0];
    // Uncheck the 'Customize priority values' checkbox.
    this.osdRecvSpeedForm.get('customizePriority').setValue(false);
    // Set the priority profile values.
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
          this.i18n('Updated OSD recovery speed priority "{{value}}"', {
            value: this.osdRecvSpeedForm.getValue('priority')
          })
        );
        this.bsModalRef.hide();
      },
      () => {
        this.bsModalRef.hide();
      }
    );
  }
}
