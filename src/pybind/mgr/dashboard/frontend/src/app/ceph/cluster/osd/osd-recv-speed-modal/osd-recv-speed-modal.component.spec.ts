import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import * as _ from 'lodash';
import { ToastModule } from 'ng2-toastr';
import { BsModalRef, ModalModule } from 'ngx-bootstrap/modal';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { SharedModule } from '../../../../shared/shared.module';
import { OsdRecvSpeedModalComponent } from './osd-recv-speed-modal.component';

describe('OsdRecvSpeedModalComponent', () => {
  let component: OsdRecvSpeedModalComponent;
  let fixture: ComponentFixture<OsdRecvSpeedModalComponent>;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      ModalModule.forRoot(),
      ReactiveFormsModule,
      SharedModule,
      ToastModule.forRoot()
    ],
    declarations: [OsdRecvSpeedModalComponent],
    providers: [BsModalRef, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdRecvSpeedModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
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

  describe('getStoredPriority', () => {
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
      component.getStoredPriority(configOptionsLow, (priority) => {
        expect(priority.name).toBe('low');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });

    it('should return priority "default" if the config option values have been set accordingly', () => {
      component.getStoredPriority(configOptionsDefault, (priority) => {
        expect(priority.name).toBe('default');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });

    it('should return priority "high" if the config option values have been set accordingly', () => {
      component.getStoredPriority(configOptionsHigh, (priority) => {
        expect(priority.name).toBe('high');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });

    it('should return priority "custom" if the config option values do not match any priority', () => {
      component.getStoredPriority(configOptionsCustom, (priority) => {
        expect(priority.name).toBe('custom');
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeTruthy();
    });

    it('should return no priority if the config option values are incomplete', () => {
      component.getStoredPriority(configOptionsIncomplete, (priority) => {
        expect(priority.name).toBeNull();
      });
      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    });
  });

  describe('setDescription', () => {
    const configOptions = [
      {
        name: 'osd_max_backfills',
        desc: ''
      },
      {
        name: 'osd_recovery_max_active',
        desc: ''
      },
      {
        name: 'osd_recovery_max_single_start',
        desc: ''
      },
      {
        name: 'osd_recovery_sleep',
        desc: 'Time in seconds to sleep before next recovery or backfill op'
      }
    ];

    it('should set the description if one is given', () => {
      component.setDescription(configOptions);
      component.priorityAttrs.forEach((p) => {
        if (p.name === 'osd_recovery_sleep') {
          expect(p.desc).toBe('Time in seconds to sleep before next recovery or backfill op');
        } else {
          expect(p.desc).toBe('');
        }
      });
    });
  });
});
