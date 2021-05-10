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
    component.clusterCreation = false;
    hostListSpy = spyOn(TestBed.inject(HostService), 'list');
    orchService = TestBed.inject(OrchestratorService);
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
        ceph_version: 'ceph version Development',
        labels: ['foo', 'bar']
      }
    ];

    OrchestratorHelper.mockStatus(true);
    hostListSpy.and.callFake(() => of(payload));
    fixture.detectChanges();

    return fixture.whenStable().then(() => {
      fixture.detectChanges();

      const spans = fixture.debugElement.nativeElement.querySelectorAll(
        '.datatable-body-cell-label span'
      );
      expect(spans[0].textContent).toBe(hostname);
    });
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
      hostListSpy.and.callFake(() => of(fakeHosts));
    });

    const testTableActions = async (
      orch: boolean,
      features: OrchestratorFeature[],
      tests: { selectRow?: number; expectResults: any }[]
    ) => {
      OrchestratorHelper.mockStatus(orch, features);
      fixture.detectChanges();
      await fixture.whenStable();

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
            Delete: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeHosts[0], // non-orchestrator host
          expectResults: {
            Add: { disabled: false, disableDesc: '' },
            Edit: { disabled: true, disableDesc: component.messages.nonOrchHost },
            Delete: { disabled: true, disableDesc: component.messages.nonOrchHost }
          }
        },
        {
          selectRow: fakeHosts[1], // orchestrator host
          expectResults: {
            Add: { disabled: false, disableDesc: '' },
            Edit: { disabled: false, disableDesc: '' },
            Delete: { disabled: false, disableDesc: '' }
          }
        }
      ];

      const features = [
        OrchestratorFeature.HOST_CREATE,
        OrchestratorFeature.HOST_LABEL_ADD,
        OrchestratorFeature.HOST_DELETE,
        OrchestratorFeature.HOST_LABEL_REMOVE
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
            Delete: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeHosts[0], // non-orchestrator host
          expectResults: {
            Add: resultNoOrchestrator,
            Edit: { disabled: true, disableDesc: component.messages.nonOrchHost },
            Delete: { disabled: true, disableDesc: component.messages.nonOrchHost }
          }
        },
        {
          selectRow: fakeHosts[1], // orchestrator host
          expectResults: {
            Add: resultNoOrchestrator,
            Edit: resultNoOrchestrator,
            Delete: resultNoOrchestrator
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
            Delete: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeHosts[0], // non-orchestrator host
          expectResults: {
            Add: resultMissingFeatures,
            Edit: { disabled: true, disableDesc: component.messages.nonOrchHost },
            Delete: { disabled: true, disableDesc: component.messages.nonOrchHost }
          }
        },
        {
          selectRow: fakeHosts[1], // orchestrator host
          expectResults: {
            Add: resultMissingFeatures,
            Edit: resultMissingFeatures,
            Delete: resultMissingFeatures
          }
        }
      ];
      await testTableActions(true, [], tests);
    });
  });
});
