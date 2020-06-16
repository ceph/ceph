import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { BsModalRef, BsModalService, ModalModule } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { ToastrModule } from 'ngx-toastr';
import { of } from 'rxjs';

import {
  configureTestBed,
  i18nProviders,
  PermissionHelper
} from '../../../../../testing/unit-test-helper';
import { PrometheusService } from '../../../../shared/api/prometheus.service';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { NotificationService } from '../../../../shared/services/notification.service';
import { SharedModule } from '../../../../shared/shared.module';
import { SilenceListComponent } from './silence-list.component';

describe('SilenceListComponent', () => {
  let component: SilenceListComponent;
  let fixture: ComponentFixture<SilenceListComponent>;
  let prometheusService: PrometheusService;

  configureTestBed({
    imports: [
      SharedModule,
      BsDropdownModule.forRoot(),
      TabsModule.forRoot(),
      ModalModule.forRoot(),
      ToastrModule.forRoot(),
      RouterTestingModule,
      HttpClientTestingModule
    ],
    declarations: [SilenceListComponent],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SilenceListComponent);
    component = fixture.componentInstance;
    prometheusService = TestBed.get(PrometheusService);
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
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
        empty: 'Create'
      };
    });

    describe('with all', () => {
      beforeEach(() => {
        tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      });

      it(`shows 'Edit' for single non expired silence else 'Create' as main action`, () => {
        scenario.single = 'Edit';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Create' as main action`, () => {
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

      it(`shows 'Edit' for single non expired silence else 'Create' as main action`, () => {
        scenario.single = 'Edit';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Create' as main action`, () => {
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

      it(`shows 'Expire' for single non expired silence else 'Create' as main action`, () => {
        scenario.single = 'Expire';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Create' as main action`, () => {
        scenario.single = 'Recreate';
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use 'Create' and 'Expire' action`, () => {
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

      it(`shows always 'Create' as main action for single non expired silences`, () => {
        scenario.single = 'Create';
        testNonExpiredSilenceScenario();
      });

      it(`shows 'Recreate' for single expired silence else 'Create' as main action`, () => {
        scenario.single = 'Recreate';
        setSilenceState('expired');
        permissionHelper.testScenarios(scenario);
      });

      it(`can use 'Create' and 'Recreate' actions`, () => {
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
      deletion.modalRef = new BsModalRef();
      deletion.ngOnInit();
      deletion.callSubmitAction();
    };

    const expectSilenceToExpire = (silenceId) => {
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
      const notificationService = TestBed.get(NotificationService);
      spyOn(notificationService, 'show').and.stub();
      expectSilenceToExpire('someSilenceId');
      expect(notificationService.show).toHaveBeenCalledWith(
        NotificationType.success,
        'Expired Silence someSilenceId',
        undefined,
        undefined,
        'Prometheus'
      );
    });

    it('should refresh after expiring a silence', () => {
      expectSilenceToExpire('someId');
      expect(component.refresh).toHaveBeenCalledTimes(1);
      expectSilenceToExpire('someOtherId');
      expect(component.refresh).toHaveBeenCalledTimes(2);
    });
  });
});
