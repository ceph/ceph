import { HttpHeaders } from '@angular/common/http';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { CephSharedModule } from '~/app/ceph/shared/ceph-shared.module';
import { CoreModule } from '~/app/core/core.module';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { SharedModule } from '~/app/shared/shared.module';
import {
  configureTestBed,
  OrchestratorHelper,
  TableActionHelper
} from '~/testing/unit-test-helper';
import { HostsComponent } from './hosts.component';

class MockShowForceMaintenanceModal {
  showModal = false;
  showModalDialog(msg: string) {
    if (
      msg.includes('WARNING') &&
      !msg.includes('It is NOT safe to stop') &&
      !msg.includes('ALERT') &&
      !msg.includes('unsafe to stop')
    ) {
      this.showModal = true;
    }
  }
}

describe('HostsComponent', () => {
  let component: HostsComponent;
  let fixture: ComponentFixture<HostsComponent>;
  let hostListSpy: jasmine.Spy;
  let orchService: OrchestratorService;
  let showForceMaintenanceModal: MockShowForceMaintenanceModal;
  let headers: HttpHeaders;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ hosts: ['read', 'update', 'create', 'delete'] });
    }
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      CephSharedModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ToastrModule.forRoot(),
      CephModule,
      CoreModule
    ],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      TableActionsComponent
    ]
  });

  beforeEach(() => {
    showForceMaintenanceModal = new MockShowForceMaintenanceModal();
    fixture = TestBed.createComponent(HostsComponent);
    component = fixture.componentInstance;
    hostListSpy = spyOn(TestBed.inject(HostService), 'list');
    orchService = TestBed.inject(OrchestratorService);
    headers = new HttpHeaders().set('x-total-count', '10');
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render hosts list even with not permission mapped services', () => {
    const hostname = 'ceph.dev';
    const payload = [
      {
        services: [
          {
            type: 'osd',
            id: '0'
          },
          {
            type: 'rgw',
            id: 'rgw'
          },
          {
            type: 'notPermissionMappedService',
            id: '1'
          }
        ],
        hostname: hostname,
        labels: ['foo', 'bar'],
        headers: headers
      }
    ];

    OrchestratorHelper.mockStatus(false);
    fixture.detectChanges();
    hostListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));
    fixture.detectChanges();

    const spans = fixture.debugElement.nativeElement.querySelectorAll(
      '.datatable-body-cell-label span'
    );
    expect(spans[0].textContent.trim()).toBe(hostname);
  });

  it('should show the exact count of the repeating daemons', () => {
    const hostname = 'ceph.dev';
    const payload = [
      {
        service_instances: [
          {
            type: 'mgr',
            count: 2
          },
          {
            type: 'osd',
            count: 3
          },
          {
            type: 'rgw',
            count: 1
          }
        ],
        hostname: hostname,
        labels: ['foo', 'bar'],
        headers: headers
      }
    ];

    OrchestratorHelper.mockStatus(false);
    fixture.detectChanges();
    hostListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));
    fixture.detectChanges();

    const spans = fixture.debugElement.nativeElement.querySelectorAll(
      '.datatable-body-cell-label span span.badge.badge-background-primary'
    );
    expect(spans[0].textContent).toContain('mgr: 2');
    expect(spans[1].textContent).toContain('osd: 3');
    expect(spans[2].textContent).toContain('rgw: 1');
  });

  it('should test if host facts are transformed correctly if orch available', () => {
    const features = [OrchestratorFeature.HOST_FACTS];
    const payload = [
      {
        hostname: 'host_test',
        services: [
          {
            type: 'osd',
            id: '0'
          }
        ],
        cpu_count: 2,
        cpu_cores: 1,
        memory_total_kb: 1024,
        hdd_count: 4,
        hdd_capacity_bytes: 1024,
        flash_count: 4,
        flash_capacity_bytes: 1024,
        nic_count: 1,
        headers: headers
      }
    ];
    OrchestratorHelper.mockStatus(true, features);
    fixture.detectChanges();
    hostListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));
    expect(hostListSpy).toHaveBeenCalled();
    expect(component.hosts[0]['cpu_count']).toEqual(2);
    expect(component.hosts[0]['memory_total_bytes']).toEqual(1048576);
    expect(component.hosts[0]['raw_capacity']).toEqual(2048);
    expect(component.hosts[0]['hdd_count']).toEqual(4);
    expect(component.hosts[0]['flash_count']).toEqual(4);
    expect(component.hosts[0]['cpu_cores']).toEqual(1);
    expect(component.hosts[0]['nic_count']).toEqual(1);
  });

  it('should test if host facts are unavailable if no orch available', () => {
    const payload = [
      {
        hostname: 'host_test',
        services: [
          {
            type: 'osd',
            id: '0'
          }
        ],
        headers: headers
      }
    ];
    OrchestratorHelper.mockStatus(false);
    fixture.detectChanges();
    hostListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));
    fixture.detectChanges();

    const spans = fixture.debugElement.nativeElement.querySelectorAll(
      '.datatable-body-cell-label span'
    );
    expect(spans[7].textContent).toBe('-');
  });

  it('should test if host facts are unavailable if get_facts orch feature is not available', () => {
    const payload = [
      {
        hostname: 'host_test',
        services: [
          {
            type: 'osd',
            id: '0'
          }
        ],
        headers: headers
      }
    ];
    OrchestratorHelper.mockStatus(true);
    fixture.detectChanges();
    hostListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));
    fixture.detectChanges();

    const spans = fixture.debugElement.nativeElement.querySelectorAll(
      '.datatable-body-cell-label span'
    );
    expect(spans[7].textContent).toBe('-');
  });

  it('should test if memory/raw capacity columns shows N/A if facts are available but in fetching state', () => {
    const features = [OrchestratorFeature.HOST_FACTS];
    let hostPayload: any[];
    hostPayload = [
      {
        hostname: 'host_test',
        services: [
          {
            type: 'osd',
            id: '0'
          }
        ],
        cpu_count: 2,
        cpu_cores: 1,
        memory_total_kb: undefined,
        hdd_count: 4,
        hdd_capacity_bytes: undefined,
        flash_count: 4,
        flash_capacity_bytes: undefined,
        nic_count: 1,
        headers: headers
      }
    ];
    OrchestratorHelper.mockStatus(true, features);
    fixture.detectChanges();
    hostListSpy.and.callFake(() => of(hostPayload));
    fixture.detectChanges();

    component.getHosts(new CdTableFetchDataContext(() => undefined));
    expect(component.hosts[0]['memory_total_bytes']).toEqual('N/A');
    expect(component.hosts[0]['raw_capacity']).toEqual('N/A');
  });

  it('should show force maintenance modal when it is safe to stop host', () => {
    const errorMsg = `WARNING: Stopping 1 out of 1 daemons in Grafana service.
                    Service will not be operational with no daemons left. At
                    least 1 daemon must be running to guarantee service.`;
    showForceMaintenanceModal.showModalDialog(errorMsg);
    expect(showForceMaintenanceModal.showModal).toBeTruthy();
  });

  it('should not show force maintenance modal when error is an ALERT', () => {
    const errorMsg = `ALERT: Cannot stop active Mgr daemon, Please switch active Mgrs
                    with 'ceph mgr fail ceph-node-00'`;
    showForceMaintenanceModal.showModalDialog(errorMsg);
    expect(showForceMaintenanceModal.showModal).toBeFalsy();
  });

  it('should not show force maintenance modal when it is not safe to stop host', () => {
    const errorMsg = `WARNING: Stopping 1 out of 1 daemons in Grafana service.
                    Service will not be operational with no daemons left. At
                    least 1 daemon must be running to guarantee service.
                    It is NOT safe to stop ['mon.ceph-node-00']: not enough
                    monitors would be available (ceph-node-02) after stopping mons`;
    showForceMaintenanceModal.showModalDialog(errorMsg);
    expect(showForceMaintenanceModal.showModal).toBeFalsy();
  });

  it('should not show force maintenance modal when it is unsafe to stop host', () => {
    const errorMsg = 'unsafe to stop osd.0 because of some unknown reason';
    showForceMaintenanceModal.showModalDialog(errorMsg);
    expect(showForceMaintenanceModal.showModal).toBeFalsy();
  });

  describe('table actions', () => {
    const fakeHosts = require('./fixtures/host_list_response.json');

    beforeEach(() => {
      let headers = new HttpHeaders().set('x-total-count', '10');
      headers = headers.set('x-total-count', '10');
      fakeHosts[0].headers = headers;
      fakeHosts[1].headers = headers;
    });

    const testTableActions = async (
      orch: boolean,
      features: OrchestratorFeature[],
      tests: { selectRow?: number; expectResults: any }[]
    ) => {
      OrchestratorHelper.mockStatus(orch, features);
      fixture.detectChanges();
      await fixture.whenStable();

      component.getHosts(new CdTableFetchDataContext(() => undefined));
      hostListSpy.and.callFake(() => of(fakeHosts));
      fixture.detectChanges();
      for (const test of tests) {
        if (test.selectRow) {
          component.selection = new CdTableSelection();
          component.selection.selected = [test.selectRow];
        }
        await TableActionHelper.verifyTableActions(
          fixture,
          component.tableActions,
          test.expectResults
        );
      }
    };

    it('should have correct states when Orchestrator is enabled', async () => {
      const tests = [
        {
          expectResults: {
            Add: { disabled: false, disableDesc: '' },
            Edit: { disabled: true, disableDesc: '' },
            Remove: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeHosts[0], // non-orchestrator host
          expectResults: {
            Add: { disabled: false, disableDesc: '' },
            Edit: { disabled: true, disableDesc: component.messages.nonOrchHost },
            Remove: { disabled: true, disableDesc: component.messages.nonOrchHost }
          }
        },
        {
          selectRow: fakeHosts[1], // orchestrator host
          expectResults: {
            Add: { disabled: false, disableDesc: '' },
            Edit: { disabled: false, disableDesc: '' },
            Remove: { disabled: false, disableDesc: '' }
          }
        }
      ];

      const features = [
        OrchestratorFeature.HOST_ADD,
        OrchestratorFeature.HOST_LABEL_ADD,
        OrchestratorFeature.HOST_REMOVE,
        OrchestratorFeature.HOST_LABEL_REMOVE,
        OrchestratorFeature.HOST_DRAIN
      ];
      await testTableActions(true, features, tests);
    });

    it('should have correct states when Orchestrator is disabled', async () => {
      const resultNoOrchestrator = {
        disabled: true,
        disableDesc: orchService.disableMessages.noOrchestrator
      };
      const tests = [
        {
          expectResults: {
            Add: resultNoOrchestrator,
            Edit: { disabled: true, disableDesc: '' },
            Remove: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeHosts[0], // non-orchestrator host
          expectResults: {
            Add: resultNoOrchestrator,
            Edit: { disabled: true, disableDesc: component.messages.nonOrchHost },
            Remove: { disabled: true, disableDesc: component.messages.nonOrchHost }
          }
        },
        {
          selectRow: fakeHosts[1], // orchestrator host
          expectResults: {
            Add: resultNoOrchestrator,
            Edit: resultNoOrchestrator,
            Remove: resultNoOrchestrator
          }
        }
      ];
      await testTableActions(false, [], tests);
    });

    it('should have correct states when Orchestrator features are missing', async () => {
      const resultMissingFeatures = {
        disabled: true,
        disableDesc: orchService.disableMessages.missingFeature
      };
      const tests = [
        {
          expectResults: {
            Add: resultMissingFeatures,
            Edit: { disabled: true, disableDesc: '' },
            Remove: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeHosts[0], // non-orchestrator host
          expectResults: {
            Add: resultMissingFeatures,
            Edit: { disabled: true, disableDesc: component.messages.nonOrchHost },
            Remove: { disabled: true, disableDesc: component.messages.nonOrchHost }
          }
        },
        {
          selectRow: fakeHosts[1], // orchestrator host
          expectResults: {
            Add: resultMissingFeatures,
            Edit: resultMissingFeatures,
            Remove: resultMissingFeatures
          }
        }
      ];
      await testTableActions(true, [], tests);
    });
  });
});
