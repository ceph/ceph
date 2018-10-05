import { HttpClientTestingModule } from '@angular/common/http/testing';
import { DebugElement } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { RouterTestingModule } from '@angular/router/testing';

import { BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';
import { EMPTY, of } from 'rxjs';

import { configureTestBed, PermissionHelper } from '../../../../../testing/unit-test-helper';
import { OsdService } from '../../../../shared/api/osd.service';
import { ConfirmationModalComponent } from '../../../../shared/components/confirmation-modal/confirmation-modal.component';
import { CriticalConfirmationModalComponent } from '../../../../shared/components/critical-confirmation-modal/critical-confirmation-modal.component';
import { TableActionsComponent } from '../../../../shared/datatable/table-actions/table-actions.component';
import { CdTableSelection } from '../../../../shared/models/cd-table-selection';
import { Permissions } from '../../../../shared/models/permissions';
import { AuthStorageService } from '../../../../shared/services/auth-storage.service';
import { SharedModule } from '../../../../shared/shared.module';
import { PerformanceCounterModule } from '../../../performance-counter/performance-counter.module';
import { OsdDetailsComponent } from '../osd-details/osd-details.component';
import { OsdPerformanceHistogramComponent } from '../osd-performance-histogram/osd-performance-histogram.component';
import { OsdReweightModalComponent } from '../osd-reweight-modal/osd-reweight-modal.component';
import { OsdListComponent } from './osd-list.component';

describe('OsdListComponent', () => {
  let component: OsdListComponent;
  let fixture: ComponentFixture<OsdListComponent>;
  let modalServiceShowSpy: jasmine.Spy;

  const fakeAuthStorageService = {
    getPermissions: () => {
      return new Permissions({ osd: ['read', 'update', 'create', 'delete'] });
    }
  };

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      PerformanceCounterModule,
      TabsModule.forRoot(),
      SharedModule,
      ReactiveFormsModule,
      RouterTestingModule
    ],
    declarations: [OsdListComponent, OsdDetailsComponent, OsdPerformanceHistogramComponent],
    providers: [
      { provide: AuthStorageService, useValue: fakeAuthStorageService },
      TableActionsComponent,
      BsModalService
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(OsdListComponent);
    fixture.detectChanges();
    component = fixture.componentInstance;
    modalServiceShowSpy = spyOn(TestBed.get(BsModalService), 'show').and.stub();
  });

  const setFakeSelection = () => {
    // Default data and selection
    const selection = [{ id: 1 }];
    const data = [{ id: 1 }];

    // Table data and selection
    component.selection = new CdTableSelection();
    component.selection.selected = selection;
    component.selection.update();
    component.osds = data;
  };

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('show table actions as defined', () => {
    let tableActions: TableActionsComponent;
    let scenario: { fn; empty; single };
    let permissionHelper: PermissionHelper;

    const getTableActionComponent = () => {
      fixture.detectChanges();
      return fixture.debugElement.query(By.directive(TableActionsComponent)).componentInstance;
    };

    beforeEach(() => {
      permissionHelper = new PermissionHelper(component.permission, () =>
        getTableActionComponent()
      );
      scenario = {
        fn: () => tableActions.getCurrentButton().name,
        single: 'Scrub',
        empty: 'Scrub'
      };
      tableActions = permissionHelper.setPermissionsAndGetActions(1, 1, 1);
    });

    it('shows action button', () => permissionHelper.testScenarios(scenario));

    it('shows all actions', () => {
      expect(tableActions.tableActions.length).toBe(9);
      expect(tableActions.tableActions).toEqual(component.tableActions);
    });
  });

  describe('test table actions in submenu', () => {
    beforeEach(
      fakeAsync(() => {
        // The menu needs a click to render the dropdown!
        const dropDownToggle = fixture.debugElement.query(By.css('.dropdown-toggle'));
        dropDownToggle.triggerEventHandler('click', null);
        tick();
        fixture.detectChanges();
      })
    );

    /**
     * Helper function to retrieve menu item
     * @param selector
     */
    const getMenuItem = (selector: string): DebugElement => {
      return fixture.debugElement
        .query(By.directive(TableActionsComponent))
        .query(By.css(selector));
    };

    it('has menu entries disabled for entries without create permission', () => {
      component.tableActions
        .filter((tableAction) => tableAction.permission !== 'create')
        .map((tableAction) => tableAction.name)
        .map(TestBed.get(TableActionsComponent).toClassName)
        .map((className) => getMenuItem(`.${className}`))
        .forEach((debugElement) => {
          expect(debugElement.classes.disabled).toBe(true);
        });
    });
  });

  describe('tests if all modals are opened correctly', () => {
    /**
     * Helper function to check if a function opens a modal
     * @param fn
     * @param modalClass - The expected class of the modal
     */
    const expectOpensModal = (fn, modalClass): void => {
      setFakeSelection();
      fn();

      expect(modalServiceShowSpy.calls.any()).toBe(true, 'modalService.show called');
      expect(modalServiceShowSpy.calls.first()).toBeTruthy();
      expect(modalServiceShowSpy.calls.first().args[0]).toBe(modalClass);

      modalServiceShowSpy.calls.reset();
    };

    it('opens the appropriate modal', () => {
      expectOpensModal(() => component.reweight(), OsdReweightModalComponent);
      expectOpensModal(() => component.markOut(), ConfirmationModalComponent);
      expectOpensModal(() => component.markIn(), ConfirmationModalComponent);
      expectOpensModal(() => component.markDown(), ConfirmationModalComponent);

      // The following modals are called after the information about their
      // safety to destroy/remove/mark them lost has been retrieved, hence
      // we will have to fake its request to be able to open those modals.
      spyOn(TestBed.get(OsdService), 'safeToDestroy').and.callFake(() =>
        of({ 'safe-to-destroy': true })
      );

      expectOpensModal(() => component.markLost(), CriticalConfirmationModalComponent);
      expectOpensModal(() => component.remove(), CriticalConfirmationModalComponent);
      expectOpensModal(() => component.destroy(), CriticalConfirmationModalComponent);
    });
  });

  describe('tests if the correct methods are called on confirmation', () => {
    const expectOsdServiceMethodCalled = (fn: Function, osdServiceMethodName: string): void => {
      setFakeSelection();
      const osdServiceSpy = spyOn(TestBed.get(OsdService), osdServiceMethodName).and.callFake(
        () => EMPTY
      );

      modalServiceShowSpy.calls.reset();
      fn(); // calls show on BsModalService
      // Calls onSubmit given to `bsModalService.show()`
      const initialState = modalServiceShowSpy.calls.first().args[1].initialState;
      const action = initialState.onSubmit || initialState.submitAction;
      action.call(component);

      expect(osdServiceSpy.calls.count()).toBe(1);
      expect(osdServiceSpy.calls.first().args[0]).toBe(1);
      modalServiceShowSpy.calls.reset();
      osdServiceSpy.calls.reset();
    };

    it('calls the corresponding service methods', () => {
      // Purposely `reweight`
      expectOsdServiceMethodCalled(() => component.markOut(), 'markOut');
      expectOsdServiceMethodCalled(() => component.markIn(), 'markIn');
      expectOsdServiceMethodCalled(() => component.markDown(), 'markDown');

      spyOn(TestBed.get(OsdService), 'safeToDestroy').and.callFake(() =>
        of({ 'safe-to-destroy': true })
      );

      expectOsdServiceMethodCalled(() => component.markLost(), 'markLost');
      expectOsdServiceMethodCalled(() => component.remove(), 'remove');
      expectOsdServiceMethodCalled(() => component.destroy(), 'destroy');
    });
  });
});
