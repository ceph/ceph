import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { ConfigurationService } from '~/app/shared/api/configuration.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { ConfigOptionTypes } from '~/app/shared/components/config-option/config-option.types';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-osd-recv-speed-modal',
  templateUrl: './osd-recv-speed-modal.component.html',
  styleUrls: ['./osd-recv-speed-modal.component.scss']
})
export class OsdRecvSpeedModalComponent implements OnInit {
  osdRecvSpeedForm: CdFormGroup;
  permissions: Permissions;

  priorities: any[] = [];
  priorityAttrs = {};

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private authStorageService: AuthStorageService,
    private configService: ConfigurationService,
    private notificationService: NotificationService,
    private osdService: OsdService
  ) {
    this.permissions = this.authStorageService.getPermissions();
    this.priorities = this.osdService.osdRecvSpeedModalPriorities.KNOWN_PRIORITIES;
    this.osdRecvSpeedForm = new CdFormGroup({
      priority: new UntypedFormControl(null, { validators: [Validators.required] }),
      customizePriority: new UntypedFormControl(false)
    });
    this.priorityAttrs = {
      osd_max_backfills: {
        text: $localize`Max Backfills`,
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
      },
      osd_recovery_max_active: {
        text: $localize`Recovery Max Active`,
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
      },
      osd_recovery_max_single_start: {
        text: $localize`Recovery Max Single Start`,
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
      },
      osd_recovery_sleep: {
        text: $localize`Recovery Sleep`,
        desc: '',
        patternHelpText: '',
        maxValue: undefined,
        minValue: undefined
      }
    };

    Object.keys(this.priorityAttrs).forEach((configOptionName) => {
      this.osdRecvSpeedForm.addControl(
        configOptionName,
        new UntypedFormControl(null, { validators: [Validators.required] })
      );
    });
  }

  ngOnInit() {
    this.configService.filter(Object.keys(this.priorityAttrs)).subscribe((data: any) => {
      const config_option_values = this.getCurrentValues(data);
      this.detectPriority(config_option_values.values, (priority: any) => {
        this.setPriority(priority);
      });
      this.setDescription(config_option_values.configOptions);
      this.setValidators(config_option_values.configOptions);
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
        Object({ name: 'custom', text: $localize`Custom`, values: configOptionValues })
      );
    }

    return callbackFn(this.priorities[0]);
  }

  getCurrentValues(configOptions: any) {
    const currentValues: Record<string, any> = { values: {}, configOptions: [] };
    configOptions.forEach((configOption: any) => {
      currentValues.configOptions.push(configOption);

      if ('value' in configOption) {
        configOption.value.forEach((value: any) => {
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
        text: $localize`Custom`,
        values: values
      };
      this.setPriority(customPriority);
    } else {
      this.detectPriority(values, (priority: any) => {
        this.setPriority(priority);
      });
    }
  }

  onPriorityChange(selectedPriorityName: string) {
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
          $localize`Updated OSD recovery speed priority '${this.osdRecvSpeedForm.getValue(
            'priority'
          )}'`
        );
        this.activeModal.close();
      },
      () => {
        this.activeModal.close();
      }
    );
  }
}
