import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';
import { ToastrModule } from 'ngx-toastr';
import { EMPTY, of } from 'rxjs';

import { CephModule } from '~/app/ceph/ceph.module';
import { PerformanceCounterModule } from '~/app/ceph/performance-counter/performance-counter.module';
import { CoreModule } from '~/app/core/core.module';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { ConfirmationModalComponent } from '~/app/shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '~/app/shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { FormModalComponent } from '~/app/shared/components/form-modal/form-modal.component';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { OrchestratorFeature } from '~/app/shared/models/orchestrator.enum';
import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import {
  configureTestBed,
  OrchestratorHelper,
  PermissionHelper,
  TableActionHelper
} from '~/testing/unit-test-helper';
import { OsdReweightModalComponent } from '../osd-reweight-modal/osd-reweight-modal.component';
import { OsdListComponent } from './osd-list.component';
import { ResizeObserver as ResizeObserverPolyfill } from '@juggle/resize-observer';

describe('OsdListComponent', () => {
  let component: OsdListComponent;
  let fixture: ComponentFixture<OsdListComponent>;
  let modalServiceShowSpy: jasmine.Spy;
  let osdService: OsdService;
  let orchService: OrchestratorService;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({
        'config-opt': ['read', 'update', 'create', 'delete'],
        osd: ['read', 'update', 'create', 'delete']
      });
    }
  };

  const getTableAction = (name: string) =>
    component.tableActions.find((action) => action.name === name);

  const setFakeSelection = () => {
    // Default data and selection
    const selection = [{ id: 1, tree: { device_class: 'ssd' } }];
    const data = [{ id: 1, tree: { device_class: 'ssd' } }];

    // Table data and selection
    component.selection = new CdTableSelection();
    component.selection.selected = selection;
    component.osds = data;
    component.permissions = fakeAuthStorageService.getPermissions();
  };

  const openActionModal = (actionName: string) => {
    setFakeSelection();
    getTableAction(actionName).click();
  };

  /**
   * The following modals are called after the information about their
   * safety to destroy/remove/mark them lost has been retrieved, hence
   * we will have to fake its request to be able to open those modals.
   */
  const mockSafeToDestroy = () => {
    spyOn(TestBed.inject(OsdService), 'safeToDestroy').and.callFake(() =>
      of({ is_safe_to_destroy: true })
    );
  };

  const mockSafeToDelete = () => {
    spyOn(TestBed.inject(OsdService), 'safeToDelete').and.callFake(() =>
      of({ is_safe_to_delete: true })
    );
  };

  const mockOrch = () => {
    const features = [OrchestratorFeature.OSD_CREATE, OrchestratorFeature.OSD_DELETE];
    OrchestratorHelper.mockStatus(true, features);
  };

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      PerformanceCounterModule,
      ToastrModule.forRoot(),
      CephModule,
      ReactiveFormsModule,
      NgbDropdownModule,
      RouterTestingModule,
      CoreModule,
      RouterTestingModule
    ],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      TableActionsComponent,
      ModalService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdListComponent);
    component = fixture.componentInstance;
    osdService = TestBed.inject(OsdService);
    modalServiceShowSpy = spyOn(TestBed.inject(ModalService), 'show').and.returnValue({
      // mock the close function, it might be called if there are async tests.
      close: jest.fn()
    });
    orchService = TestBed.inject(OrchestratorService);
    if (typeof window !== 'undefined') {
      window.ResizeObserver = window.ResizeObserver || ResizeObserverPolyfill;
    }
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should have columns that are sortable', () => {
    fixture.detectChanges();
    expect(
      component.columns
        .filter((column) => !(column.prop === undefined))
        .every((column) => Boolean(column.prop))
    ).toBeTruthy();
  });

  describe('getOsdList', () => {
    let osds: any[];
    let flagsSpy: jasmine.Spy;

    const createOsd = (n: number) =>
      <Record<string, any>>{
        in: 'in',
        up: 'up',
        tree: {
          device_class: 'ssd'
        },
        stats_history: {
          op_out_bytes: [
            [n, n],
            [n * 2, n * 2]
          ],
          op_in_bytes: [
            [n * 3, n * 3],
            [n * 4, n * 4]
          ]
        },
        stats: {
          stat_bytes_used: n * n,
          stat_bytes: n * n * n
        },
        state: []
      };

    const expectAttributeOnEveryOsd = (attr: string) =>
      expect(component.osds.every((osd) => Boolean(_.get(osd, attr)))).toBeTruthy();

    beforeEach(() => {
      spyOn(osdService, 'getList').and.callFake(() => of(osds));
      flagsSpy = spyOn(osdService, 'getFlags').and.callFake(() => of([]));
      osds = [createOsd(1), createOsd(2), createOsd(3)];
      component.getOsdList();
    });

    it('should replace "this.osds" with new data', () => {
      expect(component.osds.length).toBe(3);
      expect(osdService.getList).toHaveBeenCalledTimes(1);

      osds = [createOsd(4)];
      component.getOsdList();
      expect(component.osds.length).toBe(1);
      expect(osdService.getList).toHaveBeenCalledTimes(2);
    });

    it('should have custom attribute "collectedStates"', () => {
      expectAttributeOnEveryOsd('collectedStates');
      expect(component.osds[0].collectedStates).toEqual(['in', 'up']);
    });

    it('should have "destroyed" state in "collectedStates"', () => {
      osds[0].state.push('destroyed');
      osds[0].up = 0;
      component.getOsdList();

      expectAttributeOnEveryOsd('collectedStates');
      expect(component.osds[0].collectedStates).toEqual(['in', 'destroyed']);
    });

    it('should have custom attribute "stats_history.out_bytes"', () => {
      expectAttributeOnEveryOsd('stats_history.out_bytes');
      expect(component.osds[0].stats_history.out_bytes).toEqual([1, 2]);
    });

    it('should have custom attribute "stats_history.in_bytes"', () => {
      expectAttributeOnEveryOsd('stats_history.in_bytes');
      expect(component.osds[0].stats_history.in_bytes).toEqual([3, 4]);
    });

    it('should have custom attribute "stats.usage"', () => {
      expectAttributeOnEveryOsd('stats.usage');
      expect(component.osds[0].stats.usage).toBe(1);
      expect(component.osds[1].stats.usage).toBe(0.5);
      expect(component.osds[2].stats.usage).toBe(3 / 9);
    });

    it('should have custom attribute "cdIsBinary" to be true', () => {
      expectAttributeOnEveryOsd('cdIsBinary');
      expect(component.osds[0].cdIsBinary).toBe(true);
    });

    it('should return valid individual flags only', () => {
      const osd1 = createOsd(1);
      const osd2 = createOsd(2);
      osd1.state = ['noup', 'exists', 'up'];
      osd2.state = ['noup', 'exists', 'up', 'noin'];
      osds = [osd1, osd2];
      component.getOsdList();

      expect(component.osds[0].cdIndivFlags).toStrictEqual(['noup']);
      expect(component.osds[1].cdIndivFlags).toStrictEqual(['noup', 'noin']);
    });

    it('should not fail on empty individual flags list', () => {
      expect(component.osds[0].cdIndivFlags).toStrictEqual([]);
    });

    it('should not return disabled cluster-wide flags', () => {
      flagsSpy.and.callFake(() => of(['noout', 'nodown', 'sortbitwise']));
      component.getOsdList();
      expect(component.osds[0].cdClusterFlags).toStrictEqual(['noout', 'nodown']);

      flagsSpy.and.callFake(() => of(['noout', 'purged_snapdirs', 'nodown']));
      component.getOsdList();
      expect(component.osds[0].cdClusterFlags).toStrictEqual(['noout', 'nodown']);

      flagsSpy.and.callFake(() => of(['recovery_deletes', 'noout', 'pglog_hardlimit', 'nodown']));
      component.getOsdList();
      expect(component.osds[0].cdClusterFlags).toStrictEqual(['noout', 'nodown']);
    });

    it('should not fail on empty cluster-wide flags list', () => {
      flagsSpy.and.callFake(() => of([]));
      component.getOsdList();
      expect(component.osds[0].cdClusterFlags).toStrictEqual([]);
    });

    it('should have custom attribute "cdExecuting"', () => {
      osds[1].operational_status = 'unmanaged';
      osds[2].operational_status = 'deleting';
      component.getOsdList();
      expect(component.osds[0].cdExecuting).toBeUndefined();
      expect(component.osds[1].cdExecuting).toBeUndefined();
      expect(component.osds[2].cdExecuting).toBe('deleting');
    });
  });

  describe('show osd actions as defined', () => {
    const getOsdActions = () => {
      fixture.detectChanges();
      return fixture.debugElement.query(By.css('#cluster-wide-actions')).componentInstance
        .dropDownActions;
    };

    it('shows osd actions after osd-actions', () => {
      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css('#cluster-wide-actions'))).toBe(
        fixture.debugElement.queryAll(By.directive(TableActionsComponent))[1]
      );
    });

    it('shows both osd actions', () => {
      const osdActions = getOsdActions();
      expect(osdActions).toEqual(component.clusterWideActions);
      expect(osdActions.length).toBe(3);
    });

    it('shows only "Flags" action', () => {
      component.permissions.configOpt.read = false;
      const osdActions = getOsdActions();
      expect(osdActions[0].name).toBe('Flags');
      expect(osdActions.length).toBe(1);
    });

    it('shows only "Recovery Priority" action', () => {
      component.permissions.osd.read = false;
      const osdActions = getOsdActions();
      expect(osdActions[0].name).toBe('Recovery Priority');
      expect(osdActions[1].name).toBe('PG scrub');
      expect(osdActions.length).toBe(2);
    });

    it('shows no osd actions', () => {
      component.permissions.configOpt.read = false;
      component.permissions.osd.read = false;
      const osdActions = getOsdActions();
      expect(osdActions).toEqual([]);
    });
  });

  it('should test all TableActions combinations', () => {
    const permissionHelper: PermissionHelper = new PermissionHelper(component.permissions.osd);
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );

    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: [
          'Create',
          'Edit',
          'Flags',
          'Scrub',
          'Deep Scrub',
          'Reweight',
          'Mark Out',
          'Mark In',
          'Mark Down',
          'Mark Lost',
          'Purge',
          'Destroy',
          'Delete'
        ],
        primary: { multiple: 'Scrub', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,update': {
        actions: [
          'Create',
          'Edit',
          'Flags',
          'Scrub',
          'Deep Scrub',
          'Reweight',
          'Mark Out',
          'Mark In',
          'Mark Down'
        ],
        primary: { multiple: 'Scrub', executing: 'Edit', single: 'Edit', no: 'Create' }
      },
      'create,delete': {
        actions: ['Create', 'Mark Lost', 'Purge', 'Destroy', 'Delete'],
        primary: {
          multiple: 'Create',
          executing: 'Mark Lost',
          single: 'Mark Lost',
          no: 'Create'
        }
      },
      create: {
        actions: ['Create'],
        primary: { multiple: 'Create', executing: 'Create', single: 'Create', no: 'Create' }
      },
      'update,delete': {
        actions: [
          'Edit',
          'Flags',
          'Scrub',
          'Deep Scrub',
          'Reweight',
          'Mark Out',
          'Mark In',
          'Mark Down',
          'Mark Lost',
          'Purge',
          'Destroy',
          'Delete'
        ],
        primary: { multiple: 'Scrub', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      update: {
        actions: [
          'Edit',
          'Flags',
          'Scrub',
          'Deep Scrub',
          'Reweight',
          'Mark Out',
          'Mark In',
          'Mark Down'
        ],
        primary: { multiple: 'Scrub', executing: 'Edit', single: 'Edit', no: 'Edit' }
      },
      delete: {
        actions: ['Mark Lost', 'Purge', 'Destroy', 'Delete'],
        primary: {
          multiple: 'Mark Lost',
          executing: 'Mark Lost',
          single: 'Mark Lost',
          no: 'Mark Lost'
        }
      },
      'no-permissions': {
        actions: [],
        primary: { multiple: '', executing: '', single: '', no: '' }
      }
    });
  });

  describe('test table actions in submenu', () => {
    beforeEach(() => {
      fixture.detectChanges();
    });

    beforeEach(fakeAsync(() => {
      // The menu needs a click to render the dropdown!
      const dropDownToggle = fixture.debugElement.query(By.css('.dropdown-toggle'));
      dropDownToggle.triggerEventHandler('click', null);
      tick();
      fixture.detectChanges();
    }));

    it('has all menu entries disabled except create', () => {
      const tableActionElement = fixture.debugElement.query(By.directive(TableActionsComponent));
      const toClassName = TestBed.inject(TableActionsComponent).toClassName;
      const getActionClasses = (action: CdTableAction) =>
        tableActionElement.query(By.css(`[ngbDropdownItem].${toClassName(action)}`)).classes;

      component.tableActions.forEach((action) => {
        if (action.name === 'Create') {
          return;
        }
        expect(getActionClasses(action).disabled).toBe(true);
      });
    });
  });

  describe('tests if all modals are opened correctly', () => {
    /**
     * Helper function to check if a function opens a modal
     *
     * @param modalClass - The expected class of the modal
     */
    const expectOpensModal = (actionName: string, modalClass: any): void => {
      openActionModal(actionName);

      // @TODO: check why tsc is complaining when passing 'expectationFailOutput' param.
      expect(modalServiceShowSpy.calls.any()).toBeTruthy();
      expect(modalServiceShowSpy.calls.first().args[0]).toBe(modalClass);

      modalServiceShowSpy.calls.reset();
    };

    it('opens the reweight modal', () => {
      expectOpensModal('Reweight', OsdReweightModalComponent);
    });

    it('opens the form modal', () => {
      expectOpensModal('Edit', FormModalComponent);
    });

    it('opens all confirmation modals', () => {
      const modalClass = ConfirmationModalComponent;
      expectOpensModal('Mark Out', modalClass);
      expectOpensModal('Mark In', modalClass);
      expectOpensModal('Mark Down', modalClass);
    });

    it('opens all critical confirmation modals', () => {
      const modalClass = CriticalConfirmationModalComponent;
      mockSafeToDestroy();
      expectOpensModal('Mark Lost', modalClass);
      expectOpensModal('Purge', modalClass);
      expectOpensModal('Destroy', modalClass);
      mockOrch();
      mockSafeToDelete();
      expectOpensModal('Delete', modalClass);
    });
  });

  describe('tests if the correct methods are called on confirmation', () => {
    const expectOsdServiceMethodCalled = (
      actionName: string,
      osdServiceMethodName:
        | 'markOut'
        | 'markIn'
        | 'markDown'
        | 'markLost'
        | 'purge'
        | 'destroy'
        | 'delete'
    ): void => {
      const osdServiceSpy = spyOn(osdService, osdServiceMethodName).and.callFake(() => EMPTY);
      openActionModal(actionName);
      const initialState = modalServiceShowSpy.calls.first().args[1];
      const submit = initialState.onSubmit || initialState.submitAction;
      submit.call(component);

      expect(osdServiceSpy.calls.count()).toBe(1);
      expect(osdServiceSpy.calls.first().args[0]).toBe(1);

      // Reset spies to be able to recreate them
      osdServiceSpy.calls.reset();
      modalServiceShowSpy.calls.reset();
    };

    it('calls the corresponding service methods in confirmation modals', () => {
      expectOsdServiceMethodCalled('Mark Out', 'markOut');
      expectOsdServiceMethodCalled('Mark In', 'markIn');
      expectOsdServiceMethodCalled('Mark Down', 'markDown');
    });

    it('calls the corresponding service methods in critical confirmation modals', () => {
      mockSafeToDestroy();
      expectOsdServiceMethodCalled('Mark Lost', 'markLost');
      expectOsdServiceMethodCalled('Purge', 'purge');
      expectOsdServiceMethodCalled('Destroy', 'destroy');
      mockOrch();
      mockSafeToDelete();
      expectOsdServiceMethodCalled('Delete', 'delete');
    });
  });

  describe('table actions', () => {
    const fakeOsds = require('./fixtures/osd_list_response.json');

    beforeEach(() => {
      component.permissions = fakeAuthStorageService.getPermissions();
      spyOn(osdService, 'getList').and.callFake(() => of(fakeOsds));
      spyOn(osdService, 'getFlags').and.callFake(() => of([]));
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
            Create: { disabled: false, disableDesc: '' },
            Delete: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeOsds[0],
          expectResults: {
            Create: { disabled: false, disableDesc: '' },
            Delete: { disabled: false, disableDesc: '' }
          }
        },
        {
          selectRow: fakeOsds[1], // Select a row that is not managed.
          expectResults: {
            Create: { disabled: false, disableDesc: '' },
            Delete: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeOsds[2], // Select a row that is being deleted.
          expectResults: {
            Create: { disabled: false, disableDesc: '' },
            Delete: { disabled: true, disableDesc: '' }
          }
        }
      ];

      const features = [
        OrchestratorFeature.OSD_CREATE,
        OrchestratorFeature.OSD_DELETE,
        OrchestratorFeature.OSD_GET_REMOVE_STATUS
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
            Create: resultNoOrchestrator,
            Delete: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeOsds[0],
          expectResults: {
            Create: resultNoOrchestrator,
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
            Create: resultMissingFeatures,
            Delete: { disabled: true, disableDesc: '' }
          }
        },
        {
          selectRow: fakeOsds[0],
          expectResults: {
            Create: resultMissingFeatures,
            Delete: resultMissingFeatures
          }
        }
      ];
      await testTableActions(true, [], tests);
    });
  });
});
