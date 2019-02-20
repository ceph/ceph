import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { ToastModule } from 'ng2-toastr';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { BsModalService, ModalModule } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { of } from 'rxjs';

import { RouterTestingModule } from '@angular/router/testing';
import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../../testing/unit-test-helper';
import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '../../../../shared/shared.module';
import { PrometheusTabsComponent } from '../prometheus-tabs/prometheus-tabs.component';
import { SilencesListComponent } from './silences-list.component';

describe('SilencesListComponent', () => {
  let component: SilencesListComponent;
  let fixture: ComponentFixture<SilencesListComponent>;
  let prometheusService: PrometheusService;

  configureTestBed({
    imports: [
      SharedModule,
      BsDropdownModule.forRoot(),
      TabsModule.forRoot(),
      ModalModule.forRoot(),
      ToastModule.forRoot(),
      RouterTestingModule,
      HttpClientTestingModule
    ],
    declarations: [SilencesListComponent, PrometheusTabsComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SilencesListComponent);
    component = fixture.componentInstance;
    prometheusService = TestBed.get(PrometheusService);
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should mark silences that are not matching anything anymore', () => {});

  it('should mark silences that match multiple alerts with a badge', () => {});

  it('should show details', () => {});

  it('should should allow to expire (delete) a silence', () => {});

  it('should show the actions to add, edit and expire', () => {});

  describe('expire', () => {
    it('should show a waring if the alert is not resolved yet and no other silence will continue the silence', () => {});
  });

  describe('show action buttons and drop down actions depending on permissions', () => {
    let tableActions: TableActionsComponent;
    let scenario: { fn; empty; single };
    let permissionHelper: PermissionHelper;
    let silenceState: string;

    const getTableActionComponent = (): TableActionsComponent => {
      fixture.detectChanges();
      return fixture.debugElement.query(By.directive(TableActionsComponent)).componentInstance;
    };

    const setSilenceState = (state) => {
      silenceState = state;
    };

    const testNonExpiredSilenceScenario = () => {
      setSilenceState('active');
      permissionHelper.testScenarios(scenario);
      setSilenceState('pending');
      permissionHelper.testScenarios(scenario);
    };

    beforeEach(() => {
      permissionHelper = new PermissionHelper(component.permission, () =>
        getTableActionComponent()
      );
      permissionHelper.createSelection = () => ({ status: { state: silenceState } });
      scenario = {
        fn: () => tableActions.getCurrentButton().name,
        single: 'Edit',
        empty: 'Add'
      };
    });

    describe('with all', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      });

      it(`shows 'Edit' for single non expired silence else 'Add' as main action`, () => {
        scenario.single = 'Edit';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Add' as main action`, () => {
        scenario.single = 'Recreate';
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it('can use all actions', () => {
        expect(tableActions.tableActions.length).toBe(4);
        expect(tableActions.tableActions).toEqual(component.tableActions);
      });
    });

    describe('with read, create and update', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 0);
      });

      it(`shows 'Edit' for single non expired silence else 'Add' as main action`, () => {
        scenario.single = 'Edit';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Add' as main action`, () => {
        scenario.single = 'Recreate';
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use all actions except for 'Expire'`, () => {
        expect(tableActions.tableActions.length).toBe(3);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[1],
          component.tableActions[2]
        ]);
      });
    });

    describe('with read, create and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 1);
      });

      it(`shows 'Expire' for single non expired silence else 'Add' as main action`, () => {
        scenario.single = 'Expire';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Add' as main action`, () => {
        scenario.single = 'Recreate';
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use 'Add' and 'Expire' action`, () => {
        expect(tableActions.tableActions.length).toBe(3);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[1],
          component.tableActions[3]
        ]);
      });
    });

    describe('with read, edit and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 1);
      });

      it(`shows always 'Edit' as main action for any state`, () => {
        scenario.single = 'Edit';
        scenario.empty = 'Edit';
        testNonExpiredSilenceScenario();
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use 'Edit' and 'Expire' action`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[2],
          component.tableActions[3]
        ]);
      });
    });

    describe('with read and create', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 0, 0);
      });

      it(`shows always 'Add' as main action for single non expired silences`, () => {
        scenario.single = 'Add';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Add' as main action`, () => {
        scenario.single = 'Recreate';
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use 'Add' action`, () => {
        expect(tableActions.tableActions.length).toBe(2);
        expect(tableActions.tableActions).toEqual([
          component.tableActions[0],
          component.tableActions[1]
        ]);
      });
    });

    describe('with read and edit', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      });

      it(`shows always 'Edit' as main action for any state`, () => {
        scenario.single = 'Edit';
        scenario.empty = 'Edit';
        testNonExpiredSilenceScenario();
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use 'Edit' action`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[2]]);
      });
    });

    describe('with read and delete', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 0, 1);
      });

      it(`shows always 'Expire' as main action for any state`, () => {
        scenario.single = 'Expire';
        scenario.empty = 'Expire';
        testNonExpiredSilenceScenario();
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use 'Expire' action`, () => {
        expect(tableActions.tableActions.length).toBe(1);
        expect(tableActions.tableActions).toEqual([component.tableActions[3]]);
      });
    });

    describe('with only read', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(0, 0, 0);
      });

      it('shows no main action', () => {
        permissionHelper.testScenarios({
          fn: () => tableActions.getCurrentButton(),
          single: undefined,
          empty: undefined
        });
      });

      it('can use no actions', () => {
        expect(tableActions.tableActions.length).toBe(0);
        expect(tableActions.tableActions).toEqual([]);
      });
    });
  });

  describe('expire silence', () => {
    const setSelectedSilence = (silenceName: string) => {
      component.selection.selected = [{ id: silenceName }];
      component.selection.update();
    };

    const expireSilence = () => {
      component.expireSilence();
      const deletion: CriticalConfirmationModalComponent = component.modalRef.content;
      deletion.ngOnInit();
      deletion.callSubmitAction();
    };

    const testExpireSilence = (silenceId) => {
      setSelectedSilence(silenceId);
      expireSilence();
      expect(prometheusService.expireSilence).toHaveBeenCalledWith(silenceId);
    };

    beforeEach(() => {
      const mockObservable = () => of([]);
      spyOn(component, 'refresh').and.callFake(mockObservable);
      spyOn(prometheusService, 'expireSilence').and.callFake(mockObservable);
      spyOn(TestBed.get(BsModalService), 'show').and.callFake((deletionClass, config) => {
        return {
          content: Object.assign(new deletionClass(), config.initialState)
        };
      });
    });

    it('should expire a silence', () => {
      testExpireSilence('someSilenceId');
    });

    it('should refresh after expiring a silence', () => {
      testExpireSilence('someId');
      expect(component.refresh).toHaveBeenCalledTimes(1);
      testExpireSilence('someOtherId');
      expect(component.refresh).toHaveBeenCalledTimes(2);
    });
  });
});
