import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { BehaviorSubject, of } from 'rxjs';

import {
  configureTestBed,
  FixtureHelper,
  FormHelper,
  i18nProviders
} from '../../../../../testing/unit-test-helper';
import { OrchestratorService } from '../../../../shared/api/orchestrator.service';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { SummaryService } from '../../../../shared/services/summary.service';
import { SharedModule } from '../../../../shared/shared.module';
import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';
import { InventoryDevicesComponent } from '../../inventory/inventory-devices/inventory-devices.component';
import { DevicesSelectionChangeEvent } from '../osd-devices-selection-groups/devices-selection-change-event.interface';
import { DevicesSelectionClearEvent } from '../osd-devices-selection-groups/devices-selection-clear-event.interface';
import { OsdDevicesSelectionGroupsComponent } from '../osd-devices-selection-groups/osd-devices-selection-groups.component';
import { OsdFormComponent } from './osd-form.component';

describe('OsdFormComponent', () => {
  let form: CdFormGroup;
  let component: OsdFormComponent;
  let formHelper: FormHelper;
  let fixture: ComponentFixture<OsdFormComponent>;
  let fixtureHelper: FixtureHelper;
  let orchService: OrchestratorService;
  let summaryService: SummaryService;
  const devices: InventoryDevice[] = [
    {
      hostname: 'node0',
      uid: '1',

      path: '/dev/sda',
      sys_api: {
        vendor: 'VENDOR',
        model: 'MODEL',
        size: 1024,
        rotational: 'false',
        human_readable_size: '1 KB'
      },
      available: true,
      rejected_reasons: [''],
      device_id: 'VENDOR-MODEL-ID',
      human_readable_type: 'nvme/ssd',
      osd_ids: []
    }
  ];

  const expectPreviewButton = (enabled: boolean) => {
    const debugElement = fixtureHelper.getElementByCss('.card-footer button');
    expect(debugElement.nativeElement.disabled).toBe(!enabled);
  };

  const selectDevices = (type: string) => {
    const event: DevicesSelectionChangeEvent = {
      type: type,
      filters: [],
      data: devices,
      dataOut: []
    };
    component.onDevicesSelected(event);
    if (type === 'data') {
      component.dataDeviceSelectionGroups.devices = devices;
    } else if (type === 'wal') {
      component.walDeviceSelectionGroups.devices = devices;
    } else if (type === 'db') {
      component.dbDeviceSelectionGroups.devices = devices;
    }
    fixture.detectChanges();
  };

  const clearDevices = (type: string) => {
    const event: DevicesSelectionClearEvent = {
      type: type,
      clearedDevices: []
    };
    component.onDevicesCleared(event);
    fixture.detectChanges();
  };

  const features = ['encrypted'];
  const checkFeatures = (enabled: boolean) => {
    for (const feature of features) {
      const element = fixtureHelper.getElementByCss(`#${feature}`).nativeElement;
      expect(element.disabled).toBe(!enabled);
      expect(element.checked).toBe(false);
    }
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      FormsModule,
      SharedModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ToastrModule.forRoot()
    ],
    providers: [i18nProviders],
    declarations: [OsdFormComponent, OsdDevicesSelectionGroupsComponent, InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdFormComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    form = component.form;
    formHelper = new FormHelper(form);
    orchService = TestBed.inject(OrchestratorService);
    summaryService = TestBed.inject(SummaryService);
    summaryService['summaryDataSource'] = new BehaviorSubject(null);
    summaryService['summaryData$'] = summaryService['summaryDataSource'].asObservable();
    summaryService['summaryDataSource'].next({ version: 'master' });
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('without orchestrator', () => {
    beforeEach(() => {
      spyOn(orchService, 'status').and.returnValue(of({ available: false }));
      spyOn(orchService, 'inventoryDeviceList').and.callThrough();
      fixture.detectChanges();
    });

    it('should display info panel to document', () => {
      fixtureHelper.expectElementVisible('cd-alert-panel', true);
      fixtureHelper.expectElementVisible('.col-sm-10 form', false);
    });

    it('should not call inventoryDeviceList', () => {
      expect(orchService.inventoryDeviceList).not.toHaveBeenCalled();
    });
  });

  describe('with orchestrator', () => {
    beforeEach(() => {
      spyOn(orchService, 'status').and.returnValue(of({ available: true }));
      spyOn(orchService, 'inventoryDeviceList').and.returnValue(of([]));
      fixture.detectChanges();
    });

    it('should display form', () => {
      fixtureHelper.expectElementVisible('cd-alert-panel', false);
      fixtureHelper.expectElementVisible('.cd-col-form form', true);
    });

    describe('without data devices selected', () => {
      it('should disable preview button', () => {
        expectPreviewButton(false);
      });

      it('should not display shared devices slots', () => {
        fixtureHelper.expectElementVisible('#walSlots', false);
        fixtureHelper.expectElementVisible('#dbSlots', false);
      });

      it('should disable the checkboxes', () => {
        checkFeatures(false);
      });
    });

    describe('with data devices selected', () => {
      beforeEach(() => {
        selectDevices('data');
      });

      it('should enable preview button', () => {
        expectPreviewButton(true);
      });

      it('should not display shared devices slots', () => {
        fixtureHelper.expectElementVisible('#walSlots', false);
        fixtureHelper.expectElementVisible('#dbSlots', false);
      });

      it('should enable the checkboxes', () => {
        checkFeatures(true);
      });

      it('should disable the checkboxes after clearing data devices', () => {
        clearDevices('data');
        checkFeatures(false);
      });

      describe('with shared devices selected', () => {
        beforeEach(() => {
          selectDevices('wal');
          selectDevices('db');
        });

        it('should display slots', () => {
          fixtureHelper.expectElementVisible('#walSlots', true);
          fixtureHelper.expectElementVisible('#dbSlots', true);
        });

        it('validate slots', () => {
          for (const control of ['walSlots', 'dbSlots']) {
            formHelper.expectValid(control);
            formHelper.expectValidChange(control, 1);
            formHelper.expectErrorChange(control, -1, 'min');
          }
        });

        describe('test clearing data devices', () => {
          beforeEach(() => {
            clearDevices('data');
          });

          it('should not display shared devices slots and should disable checkboxes', () => {
            fixtureHelper.expectElementVisible('#walSlots', false);
            fixtureHelper.expectElementVisible('#dbSlots', false);
            checkFeatures(false);
          });
        });
      });
    });
  });
});
