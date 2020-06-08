import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import * as _ from 'lodash';
import { BsModalRef, ModalModule } from 'ngx-bootstrap/modal';
import { ToastrModule } from 'ngx-toastr';
import { of as observableOf } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { ConfigurationService } from '../../../../shared/api/configuration.service';
import { SharedModule } from '../../../../shared/shared.module';
import { OsdRecvSpeedModalComponent } from './osd-recv-speed-modal.component';

describe('OsdRecvSpeedModalComponent', () => {
  let component: OsdRecvSpeedModalComponent;
  let fixture: ComponentFixture<OsdRecvSpeedModalComponent>;
  let configurationService: ConfigurationService;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ModalModule.forRoot(),
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      ToastrModule.forRoot()
    ],
    declarations: [OsdRecvSpeedModalComponent],
    providers: [BsModalRef, i18nProviders]
  });

  let configOptions: any[] = [];

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdRecvSpeedModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    configurationService = TestBed.inject(ConfigurationService);
    configOptions = [
      {
        name: 'osd_max_backfills',
        desc: '',
        type: 'uint',
        default: 1
      },
      {
        name: 'osd_recovery_max_active',
        desc: '',
        type: 'uint',
        default: 3
      },
      {
        name: 'osd_recovery_max_single_start',
        desc: '',
        type: 'uint',
        default: 1
      },
      {
        name: 'osd_recovery_sleep',
        desc: 'Time in seconds to sleep before next recovery or backfill op',
        type: 'float',
        default: 0
      }
    ];
    spyOn(configurationService, 'filter').and.returnValue(observableOf(configOptions));
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('ngOnInit', () => {
    let setPriority: jasmine.Spy;
    let setValidators: jasmine.Spy;

    beforeEach(() => {
      setPriority = spyOn(component, 'setPriority').and.callThrough();
      setValidators = spyOn(component, 'setValidators').and.callThrough();
      component.ngOnInit();
    });

    it('should call setValidators', () => {
      expect(setValidators).toHaveBeenCalled();
    });

    it('should get and set priority correctly', () => {
      const defaultPriority = _.find(component.priorities, (p) => {
        return _.isEqual(p.name, 'default');
      });
      expect(setPriority).toHaveBeenCalledWith(defaultPriority);
    });

    it('should set descriptions correctly', () => {
      expect(component.priorityAttrs['osd_max_backfills'].desc).toBe('');
      expect(component.priorityAttrs['osd_recovery_max_active'].desc).toBe('');
      expect(component.priorityAttrs['osd_recovery_max_single_start'].desc).toBe('');
      expect(component.priorityAttrs['osd_recovery_sleep'].desc).toBe(
        'Time in seconds to sleep before next recovery or backfill op'
      );
    });
  });

  describe('setPriority', () => {
    it('should prepare the form for a custom priority', () => {
      const customPriority = {
        name: 'custom',
        text: 'Custom',
        values: {
          osd_max_backfills: 1,
          osd_recovery_max_active: 4,
          osd_recovery_max_single_start: 1,
          osd_recovery_sleep: 1
        }
      };

      component.setPriority(customPriority);

      const customInPriorities = _.find(component.priorities, (p) => {
        return p.name === 'custom';
      });

      expect(customInPriorities).not.toBeNull();
      expect(component.osdRecvSpeedForm.getValue('priority')).toBe('custom');
      expect(component.osdRecvSpeedForm.getValue('osd_max_backfills')).toBe(1);
      expect(component.osdRecvSpeedForm.getValue('osd_recovery_max_active')).toBe(4);
      expect(component.osdRecvSpeedForm.getValue('osd_recovery_max_single_start')).toBe(1);
      expect(component.osdRecvSpeedForm.getValue('osd_recovery_sleep')).toBe(1);
    });

    it('should prepare the form for a none custom priority', () => {
      const lowPriority = {
        name: 'low',
        text: 'Low',
        values: {
          osd_max_backfills: 1,
          osd_recovery_max_active: 1,
          osd_recovery_max_single_start: 1,
          osd_recovery_sleep: 0.5
        }
      };

      component.setPriority(lowPriority);

      const customInPriorities = _.find(component.priorities, (p) => {
        return p.name === 'custom';
      });

      expect(customInPriorities).toBeUndefined();
      expect(component.osdRecvSpeedForm.getValue('priority')).toBe('low');
      expect(component.osdRecvSpeedForm.getValue('osd_max_backfills')).toBe(1);
      expect(component.osdRecvSpeedForm.getValue('osd_recovery_max_active')).toBe(1);
      expect(component.osdRecvSpeedForm.getValue('osd_recovery_max_single_start')).toBe(1);
      expect(component.osdRecvSpeedForm.getValue('osd_recovery_sleep')).toBe(0.5);
    });
  });

  describe('detectPriority', () => {
    const configOptionsLow = {
      osd_max_backfills: 1,
      osd_recovery_max_active: 1,
      osd_recovery_max_single_start: 1,
      osd_recovery_sleep: 0.5
    };

    const configOptionsDefault = {
      osd_max_backfills: 1,
      osd_recovery_max_active: 3,
      osd_recovery_max_single_start: 1,
      osd_recovery_sleep: 0
    };

    const configOptionsHigh = {
      osd_max_backfills: 4,
      osd_recovery_max_active: 4,
      osd_recovery_max_single_start: 4,
      osd_recovery_sleep: 0
    };

    const configOptionsCustom = {
      osd_max_backfills: 1,
      osd_recovery_max_active: 2,
      osd_recovery_max_single_start: 1,
      osd_recovery_sleep: 0
    };

    const configOptionsIncomplete = {
      osd_max_backfills: 1,
      osd_recovery_max_single_start: 1,
      osd_recovery_sleep: 0
    };

    it('should return priority "low" if the config option values have been set accordingly', () => {
      component.detectPriority(configOptionsLow, (priority: Record<string, any>) => {
        expect(priority.name).toBe('low');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });

    it('should return priority "default" if the config option values have been set accordingly', () => {
      component.detectPriority(configOptionsDefault, (priority: Record<string, any>) => {
        expect(priority.name).toBe('default');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });

    it('should return priority "high" if the config option values have been set accordingly', () => {
      component.detectPriority(configOptionsHigh, (priority: Record<string, any>) => {
        expect(priority.name).toBe('high');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });

    it('should return priority "custom" if the config option values do not match any priority', () => {
      component.detectPriority(configOptionsCustom, (priority: Record<string, any>) => {
        expect(priority.name).toBe('custom');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeTruthy();
    });

    it('should return no priority if the config option values are incomplete', () => {
      component.detectPriority(configOptionsIncomplete, (priority: Record<string, any>) => {
        expect(priority.name).toBeNull();
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });
  });

  describe('getCurrentValues', () => {
    it('should return default values if no value has been set by the user', () => {
      const currentValues = component.getCurrentValues(configOptions);
      configOptions.forEach((configOption) => {
        const configOptionValue = currentValues.values[configOption.name];
        expect(configOptionValue).toBe(configOption.default);
      });
    });

    it('should return the values set by the user if they exist', () => {
      configOptions.forEach((configOption) => {
        configOption['value'] = [{ section: 'osd', value: 7 }];
      });

      const currentValues = component.getCurrentValues(configOptions);
      Object.values(currentValues.values).forEach((configValue) => {
        expect(configValue).toBe(7);
      });
    });

    it('should return the default value if one is missing', () => {
      for (let i = 1; i < configOptions.length; i++) {
        configOptions[i]['value'] = [{ section: 'osd', value: 7 }];
      }

      const currentValues = component.getCurrentValues(configOptions);
      Object.entries(currentValues.values).forEach(([configName, configValue]) => {
        if (configName === 'osd_max_backfills') {
          expect(configValue).toBe(1);
        } else {
          expect(configValue).toBe(7);
        }
      });
    });

    it('should return nothing if neither value nor default value is given', () => {
      configOptions[0].default = null;
      const currentValues = component.getCurrentValues(configOptions);
      expect(currentValues.values).not.toContain('osd_max_backfills');
    });
  });

  describe('setDescription', () => {
    it('should set the description if one is given', () => {
      component.setDescription(configOptions);
      Object.keys(component.priorityAttrs).forEach((configOptionName) => {
        if (configOptionName === 'osd_recovery_sleep') {
          expect(component.priorityAttrs[configOptionName].desc).toBe(
            'Time in seconds to sleep before next recovery or backfill op'
          );
        } else {
          expect(component.priorityAttrs[configOptionName].desc).toBe('');
        }
      });
    });
  });

  describe('setValidators', () => {
    it('should set needed validators for config option', () => {
      component.setValidators(configOptions);
      configOptions.forEach((configOption) => {
        const control = component.osdRecvSpeedForm.controls[configOption.name];

        if (configOption.type === 'float') {
          expect(component.priorityAttrs[configOption.name].patternHelpText).toBe(
            'The entered value needs to be a number or decimal.'
          );
        } else {
          expect(component.priorityAttrs[configOption.name].minValue).toBe(0);
          expect(component.priorityAttrs[configOption.name].patternHelpText).toBe(
            'The entered value needs to be an unsigned number.'
          );

          control.setValue(-1);
          expect(control.hasError('min')).toBeTruthy();
        }

        control.setValue(null);
        expect(control.hasError('required')).toBeTruthy();
        control.setValue('E');
        expect(control.hasError('pattern')).toBeTruthy();
        control.setValue(3);
        expect(control.hasError('required')).toBeFalsy();
        expect(control.hasError('min')).toBeFalsy();
        expect(control.hasError('pattern')).toBeFalsy();
      });
    });
  });
});
