import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { CheckboxModule, NumberModule, RadioModule } from 'carbon-components-angular';

import { ToastrModule } from 'ngx-toastr';
import { BehaviorSubject, of } from 'rxjs';

import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { InventoryDevicesComponent } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-devices.component';
import { DashboardModule } from '~/app/ceph/dashboard/dashboard.module';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import {
  DeploymentOptions,
  OsdDeploymentOptions
} from '~/app/shared/models/osd-deployment-options';
import { SummaryService } from '~/app/shared/services/summary.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed, FixtureHelper, FormHelper } from '~/testing/unit-test-helper';
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
  let hostService: HostService;
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

  const deploymentOptions: DeploymentOptions = {
    options: {
      cost_capacity: {
        name: OsdDeploymentOptions.COST_CAPACITY,
        available: true,
        capacity: 0,
        used: 0,
        hdd_used: 0,
        ssd_used: 0,
        nvme_used: 0,
        title: 'Cost/Capacity-optimized',
        desc: 'All the available HDDs are selected'
      },
      throughput_optimized: {
        name: OsdDeploymentOptions.THROUGHPUT,
        available: false,
        capacity: 0,
        used: 0,
        hdd_used: 0,
        ssd_used: 0,
        nvme_used: 0,
        title: 'Throughput-optimized',
        desc: 'HDDs/SSDs are selected for data devices and SSDs/NVMes for DB/WAL devices'
      },
      iops_optimized: {
        name: OsdDeploymentOptions.IOPS,
        available: false,
        capacity: 0,
        used: 0,
        hdd_used: 0,
        ssd_used: 0,
        nvme_used: 0,
        title: 'IOPS-optimized',
        desc: 'All the available NVMes are selected'
      }
    },
    recommended_option: OsdDeploymentOptions.COST_CAPACITY
  };

  const expectPreviewButton = (enabled: boolean) => {
    expect(component.dataDeviceSelectionGroups.devices.length > 0).toBe(enabled);
  };

  const ensureSelectionGroups = () => {
    component.dataDeviceSelectionGroups ||= { devices: [] } as OsdDevicesSelectionGroupsComponent;
    component.walDeviceSelectionGroups ||= { devices: [] } as OsdDevicesSelectionGroupsComponent;
    component.dbDeviceSelectionGroups ||= { devices: [] } as OsdDevicesSelectionGroupsComponent;
  };

  const selectDevices = (type: string) => {
    ensureSelectionGroups();
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
    ensureSelectionGroups();
    const event: DevicesSelectionClearEvent = {
      type: type,
      clearedDevices: []
    };
    component.onDevicesCleared(event);
    if (type === 'data') {
      component.dataDeviceSelectionGroups.devices = [];
    } else if (type === 'wal') {
      component.walDeviceSelectionGroups.devices = [];
    } else if (type === 'db') {
      component.dbDeviceSelectionGroups.devices = [];
    }
    fixture.detectChanges();
  };

  const features = ['encrypted'];
  const checkFeatures = (enabled: boolean) => {
    for (const feature of features) {
      const control = form.get(feature);
      expect(control.disabled).toBe(!enabled);
      expect(control.value).toBe(false);
    }
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      FormsModule,
      RadioModule,
      CheckboxModule,
      NumberModule,
      SharedModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ToastrModule.forRoot(),
      DashboardModule
    ],
    declarations: [OsdFormComponent, OsdDevicesSelectionGroupsComponent, InventoryDevicesComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdFormComponent);
    fixtureHelper = new FixtureHelper(fixture);
    component = fixture.componentInstance;
    form = component.form;
    formHelper = new FormHelper(form);
    orchService = TestBed.inject(OrchestratorService);
    hostService = TestBed.inject(HostService);
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
      spyOn(hostService, 'inventoryDeviceList').and.callThrough();
      fixture.detectChanges();
    });

    it('should display info panel to document', () => {
      fixtureHelper.expectElementVisible('cd-alert-panel', true);
      fixtureHelper.expectElementVisible('.col-sm-10 form', false);
    });

    it('should not call inventoryDeviceList', () => {
      expect(hostService.inventoryDeviceList).not.toHaveBeenCalled();
    });
  });

  describe('with orchestrator', () => {
    beforeEach(() => {
      component.simpleDeployment = false;
      spyOn(orchService, 'status').and.returnValue(of({ available: true }));
      spyOn(hostService, 'inventoryDeviceList').and.returnValue(of([]));
      component.deploymentOptions = deploymentOptions;
      fixture.detectChanges();
      ensureSelectionGroups();
    });

    it('should display the accordion', () => {
      expect(component.hasOrchestrator).toBe(true);
      expect(component.optionNames).toEqual([
        OsdDeploymentOptions.COST_CAPACITY,
        OsdDeploymentOptions.THROUGHPUT,
        OsdDeploymentOptions.IOPS
      ]);
    });

    it('should display the three deployment scenarios', () => {
      const text = fixture.nativeElement.textContent;
      expect(text).toContain('Cost/Capacity-optimized');
      expect(text).toContain('Throughput-optimized');
      expect(text).toContain('IOPS-optimized');
    });

    it('should only disable the options that are not available', () => {
      expect(deploymentOptions.options['throughput_optimized'].available).toBeFalsy();
      expect(deploymentOptions.options['iops_optimized'].available).toBeFalsy();

      deploymentOptions.options['throughput_optimized'].available = true;
      fixture.detectChanges();
      expect(deploymentOptions.options['throughput_optimized'].available).toBeTruthy();
    });

    it('should be a Recommended option only when it is recommended by backend', () => {
      let text = fixture.nativeElement.textContent;
      expect(text).toContain('Cost/Capacity-optimized');
      expect(text).toContain('(Recommended)');

      deploymentOptions.recommended_option = OsdDeploymentOptions.THROUGHPUT;
      fixture.detectChanges();
      text = fixture.nativeElement.textContent;
      expect(text).toContain('Throughput-optimized');
      expect(text).toContain('(Recommended)');
    });

    describe('without data devices selected', () => {
      it('should disable preview button', () => {
        component.simpleDeployment = false;
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
          expect(component.walDeviceSelectionGroups.devices.length).toBeGreaterThan(0);
          expect(component.dbDeviceSelectionGroups.devices.length).toBeGreaterThan(0);
        });

        it('validate slots', () => {
          for (const control of ['walSlots', 'dbSlots']) {
            formHelper.expectValid(control);
            formHelper.expectValidChange(control, 1);
            formHelper.expectValidChange(control, -1);
          }
          expect(component.driveGroup.spec['wal_slots']).toBe(1);
          expect(component.driveGroup.spec['db_slots']).toBe(1);
        });

        describe('test clearing data devices', () => {
          beforeEach(() => {
            clearDevices('data');
          });

          it('should not display shared devices slots and should disable checkboxes', () => {
            expect(component.walDeviceSelectionGroups.devices.length).toBe(0);
            expect(component.dbDeviceSelectionGroups.devices.length).toBe(0);
            checkFeatures(false);
          });
        });
      });
    });
  });
});
