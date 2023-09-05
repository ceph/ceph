import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import _ from 'lodash';
import { ReplaySubject } from 'rxjs';

import { Icons } from '~/app/shared/enum/icons.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import {
  RbdConfigurationEntry,
  RbdConfigurationSourceField,
  RbdConfigurationType
} from '~/app/shared/models/configuration';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { RbdConfigurationService } from '~/app/shared/services/rbd-configuration.service';

@Component({
  selector: 'cd-rbd-configuration-form',
  templateUrl: './rbd-configuration-form.component.html',
  styleUrls: ['./rbd-configuration-form.component.scss']
})
export class RbdConfigurationFormComponent implements OnInit {
  @Input()
  form: CdFormGroup;
  @Input()
  initializeData = new ReplaySubject<{
    initialData: RbdConfigurationEntry[];
    sourceType: RbdConfigurationSourceField;
  }>(1);
  @Output()
  changes = new EventEmitter<any>();

  icons = Icons;

  ngDataReady = new EventEmitter<any>();
  initialData: RbdConfigurationEntry[];
  configurationType = RbdConfigurationType;
  sectionVisibility: { [key: string]: boolean } = {};

  constructor(
    public formatterService: FormatterService,
    public rbdConfigurationService: RbdConfigurationService
  ) {}

  ngOnInit() {
    const configFormGroup = this.createConfigurationFormGroup();
    this.form.addControl('configuration', configFormGroup);

    // Listen to changes and emit the values to the parent component
    configFormGroup.valueChanges.subscribe(() => {
      this.changes.emit(this.getDirtyValues.bind(this));
    });

    if (this.initializeData) {
      this.initializeData.subscribe((data: Record<string, any>) => {
        this.initialData = data.initialData;
        const dataType = data.sourceType;
        this.rbdConfigurationService.getWritableOptionFields().forEach((option) => {
          const optionData = data.initialData
            .filter((entry: Record<string, any>) => entry.name === option.name)
            .pop();
          if (optionData && optionData['source'] === dataType) {
            this.form.get(`configuration.${option.name}`).setValue(optionData['value']);
          }
        });
        this.ngDataReady.emit();
      });
    }

    this.rbdConfigurationService
      .getWritableSections()
      .forEach((section) => (this.sectionVisibility[section.class] = false));
  }

  getDirtyValues(includeLocalValues = false, localFieldType?: RbdConfigurationSourceField) {
    if (includeLocalValues && !localFieldType) {
      const msg =
        'ProgrammingError: If local values shall be included, a proper localFieldType argument has to be provided, too';
      throw new Error(msg);
    }
    const result = {};

    this.rbdConfigurationService.getWritableOptionFields().forEach((config) => {
      const control: any = this.form.get('configuration').get(config.name);
      const dirty = control.dirty;

      if (this.initialData && this.initialData[config.name] === control.value) {
        return; // Skip controls with initial data loaded
      }

      if (dirty || (includeLocalValues && control['source'] === localFieldType)) {
        if (control.value === null) {
          result[config.name] = control.value;
        } else if (config.type === RbdConfigurationType.bps) {
          result[config.name] = this.formatterService.toBytes(control.value);
        } else if (config.type === RbdConfigurationType.milliseconds) {
          result[config.name] = this.formatterService.toMilliseconds(control.value);
        } else if (config.type === RbdConfigurationType.iops) {
          result[config.name] = this.formatterService.toIops(control.value);
        } else {
          result[config.name] = control.value;
        }
      }
    });

    return result;
  }

  /**
   * Dynamically create form controls.
   */
  private createConfigurationFormGroup() {
    const configFormGroup = new CdFormGroup({});

    this.rbdConfigurationService.getWritableOptionFields().forEach((c) => {
      let control: UntypedFormControl;
      if (
        c.type === RbdConfigurationType.milliseconds ||
        c.type === RbdConfigurationType.iops ||
        c.type === RbdConfigurationType.bps
      ) {
        let initialValue = 0;
        _.forEach(this.initialData, (configList) => {
          if (configList['name'] === c.name) {
            initialValue = configList['value'];
          }
        });
        control = new UntypedFormControl(initialValue, Validators.min(0));
      } else {
        throw new Error(
          `Type ${c.type} is unknown, you may need to add it to RbdConfiguration class`
        );
      }
      configFormGroup.addControl(c.name, control);
    });

    return configFormGroup;
  }

  /**
   * Reset the value. The inherited value will be used instead.
   */
  reset(optionName: string) {
    const formControl: any = this.form.get('configuration').get(optionName);
    if (formControl.disabled) {
      formControl.setValue(formControl['previousValue'] || 0);
      formControl.enable();
      if (!formControl['previousValue']) {
        formControl.markAsPristine();
      }
    } else {
      formControl['previousValue'] = formControl.value;
      formControl.setValue(null);
      formControl.markAsDirty();
      formControl.disable();
    }
  }

  isDisabled(optionName: string) {
    return this.form.get('configuration').get(optionName).disabled;
  }

  toggleSectionVisibility(className: string) {
    this.sectionVisibility[className] = !this.sectionVisibility[className];
  }
}
