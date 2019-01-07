import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import * as _ from 'lodash';
import { ToastModule } from 'ng2-toastr';
import { BsModalRef, ModalModule } from 'ngx-bootstrap/modal';
import { of } from 'rxjs';

import { configureTestBed, i18nProviders } from '../../../../../testing/unit-test-helper';
import { ConfigurationService } from '../../../../shared/api/configuration.service';
import { SharedModule } from '../../../../shared/shared.module';
import { OsdRecvSpeedModalComponent } from './osd-recv-speed-modal.component';

describe('OsdRecvSpeedModalComponent', () => {
  let component: OsdRecvSpeedModalComponent;
  let fixture: ComponentFixture<OsdRecvSpeedModalComponent>;
  let configService: ConfigurationService;

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
    configService = TestBed.get(ConfigurationService);
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
    const configOptionsLow = [
      {
        name: 'osd_max_backfills',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_max_active',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_max_single_start',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_sleep',
        value: [
          {
            section: 'osd',
            value: '0.5'
          }
        ]
      }
    ];

    const configOptionsDefault = [
      {
        name: 'osd_max_backfills',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_max_active',
        value: [
          {
            section: 'osd',
            value: '3'
          }
        ]
      },
      {
        name: 'osd_recovery_max_single_start',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_sleep',
        value: [
          {
            section: 'osd',
            value: '0'
          }
        ]
      }
    ];

    const configOptionsHigh = [
      {
        name: 'osd_max_backfills',
        value: [
          {
            section: 'osd',
            value: '4'
          }
        ]
      },
      {
        name: 'osd_recovery_max_active',
        value: [
          {
            section: 'osd',
            value: '4'
          }
        ]
      },
      {
        name: 'osd_recovery_max_single_start',
        value: [
          {
            section: 'osd',
            value: '4'
          }
        ]
      },
      {
        name: 'osd_recovery_sleep',
        value: [
          {
            section: 'osd',
            value: '0'
          }
        ]
      }
    ];

    const configOptionsCustom = [
      {
        name: 'osd_max_backfills',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_max_active',
        value: [
          {
            section: 'osd',
            value: '2'
          }
        ]
      },
      {
        name: 'osd_recovery_max_single_start',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_sleep',
        value: [
          {
            section: 'osd',
            value: '0'
          }
        ]
      }
    ];

    const configOptionsIncomplete = [
      {
        name: 'osd_max_backfills',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_max_single_start',
        value: [
          {
            section: 'osd',
            value: '1'
          }
        ]
      },
      {
        name: 'osd_recovery_sleep',
        value: [
          {
            section: 'osd',
            value: '0'
          }
        ]
      }
    ];

    it('should return priority "low" if the config option values have been set accordingly', fakeAsync(() => {
      spyOn(configService, 'get').and.callFake((configOptionName: string) => {
        const result = _.find(configOptionsLow, (configOption) => {
          return configOption.name === configOptionName;
        });
        return of(result);
      });

      component.getStoredPriority((priority) => {
        expect(priority.name).toBe('low');
      });
      tick();

      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    }));

    it('should return priority "default" if the config option values have been set accordingly', fakeAsync(() => {
      spyOn(configService, 'get').and.callFake((configOptionName: string) => {
        const result = _.find(configOptionsDefault, (configOption) => {
          return configOption.name === configOptionName;
        });
        return of(result);
      });

      component.getStoredPriority((priority) => {
        expect(priority.name).toBe('default');
      });
      tick();

      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    }));

    it('should return priority "high" if the config option values have been set accordingly', fakeAsync(() => {
      spyOn(configService, 'get').and.callFake((configOptionName: string) => {
        const result = _.find(configOptionsHigh, (configOption) => {
          return configOption.name === configOptionName;
        });
        return of(result);
      });

      component.getStoredPriority((priority) => {
        expect(priority.name).toBe('high');
      });
      tick();

      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    }));

    it('should return priority "custom" if the config option values do not match any priority', fakeAsync(() => {
      spyOn(configService, 'get').and.callFake((configOptionName: string) => {
        const result = _.find(configOptionsCustom, (configOption) => {
          return configOption.name === configOptionName;
        });
        return of(result);
      });

      component.getStoredPriority((priority) => {
        expect(priority.name).toBe('custom');
      });
      tick();

      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeTruthy();
    }));

    it('should return no priority if the config option values are incomplete', fakeAsync(() => {
      spyOn(configService, 'get').and.callFake((configOptionName: string) => {
        const result = _.find(configOptionsIncomplete, (configOption) => {
          return configOption.name === configOptionName;
        });
        return of(result);
      });

      component.getStoredPriority((priority) => {
        expect(priority.name).toBeNull();
      });
      tick();

      expect(component.osdRecvSpeedForm.getValue('customizePriority')).toBeFalsy();
    }));
  });
});
