import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed, PermissionHelper } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../components/components.module';
import { CdTableAction } from '../../models/cd-table-action';
import { CdTableSelection } from '../../models/cd-table-selection';
import { Permission } from '../../models/permissions';
import { TableActionsComponent } from './table-actions.component';

describe('TableActionsComponent', () => {
  let component: TableActionsComponent;
  let fixture: ComponentFixture<TableActionsComponent>;
  let addAction: CdTableAction;
  let editAction: CdTableAction;
  let protectAction: CdTableAction;
  let unprotectAction: CdTableAction;
  let deleteAction: CdTableAction;
  let copyAction: CdTableAction;
  let scenario;
  let permissionHelper: PermissionHelper;

  const setUpTableActions = () => {
    component.tableActions = [
      addAction,
      editAction,
      protectAction,
      unprotectAction,
      copyAction,
      deleteAction
    ];
  };

  const getTableActionComponent = (): TableActionsComponent => {
    setUpTableActions();
    component.ngOnInit();
    return component;
  };

  configureTestBed({
    declarations: [TableActionsComponent],
    imports: [ComponentsModule, RouterTestingModule]
  });

  beforeEach(() => {
    addAction = {
      permission: 'create',
      icon: 'fa-plus',
      buttonCondition: (selection: CdTableSelection) => !selection.hasSelection,
      name: 'Add'
    };
    editAction = {
      permission: 'update',
      icon: 'fa-pencil',
      name: 'Edit'
    };
    copyAction = {
      permission: 'create',
      icon: 'fa-copy',
      buttonCondition: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      name: 'Copy'
    };
    deleteAction = {
      permission: 'delete',
      icon: 'fa-times',
      buttonCondition: (selection: CdTableSelection) => selection.hasSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSelection || selection.first().cdExecuting,
      name: 'Delete'
    };
    protectAction = {
      permission: 'update',
      icon: 'fa-lock',
      buttonCondition: () => false,
      visible: (selection: CdTableSelection) => selection.hasSingleSelection,
      name: 'Protect'
    };
    unprotectAction = {
      permission: 'update',
      icon: 'fa-unlock',
      buttonCondition: () => false,
      visible: (selection: CdTableSelection) => !selection.hasSingleSelection,
      name: 'Unprotect'
    };
    fixture = TestBed.createComponent(TableActionsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    component.permission = new Permission();
    component.permission.read = true;
    permissionHelper = new PermissionHelper(component.permission, () => getTableActionComponent());
    permissionHelper.setPermissionsAndGetActions(1, 1, 1);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should ngInit should be called with no permissions', () => {
    component.permission = undefined;
    component.ngOnInit();
    expect(component.tableActions).toEqual([]);
    expect(component.dropDownActions).toEqual([]);
  });

  describe('useRouterLink', () => {
    const testLink = '/api/some/link';
    it('should use a link generated from a function', () => {
      addAction.routerLink = () => testLink;
      expect(component.useRouterLink(addAction)).toBe(testLink);
    });

    it('should use the link as it is because it is a string', () => {
      addAction.routerLink = testLink;
      expect(component.useRouterLink(addAction)).toBe(testLink);
    });

    it('should not return anything because no link is defined', () => {
      expect(component.useRouterLink(addAction)).toBe(undefined);
    });

    it('should not return anything because the action is disabled', () => {
      editAction.routerLink = testLink;
      expect(component.useRouterLink(editAction)).toBe(undefined);
    });
  });

  describe('disableSelectionAction', () => {
    beforeEach(() => {
      scenario = {
        fn: () => null,
        multiple: false,
        singleExecuting: false,
        single: false,
        empty: false
      };
    });

    it('tests disabling addAction', () => {
      scenario.fn = () => component.disableSelectionAction(addAction);
      permissionHelper.testScenarios(scenario);
    });

    it('tests disabling editAction', () => {
      scenario.fn = () => component.disableSelectionAction(editAction);
      scenario.multiple = true;
      scenario.empty = true;
      scenario.singleExecuting = true;
      permissionHelper.testScenarios(scenario);
    });

    it('tests disabling deleteAction', () => {
      scenario.fn = () => component.disableSelectionAction(deleteAction);
      scenario.multiple = false;
      scenario.empty = true;
      scenario.singleExecuting = true;
      permissionHelper.testScenarios(scenario);
    });

    it('tests disabling copyAction', () => {
      scenario.fn = () => component.disableSelectionAction(copyAction);
      scenario.multiple = true;
      scenario.empty = true;
      scenario.singleExecuting = true;
      permissionHelper.testScenarios(scenario);
    });
  });

  describe('get current button', () => {
    const hiddenScenario = () => {
      scenario.multiple = undefined;
      scenario.empty = undefined;
      scenario.singleExecuting = undefined;
      scenario.single = undefined;
    };

    const setScenario = (defaultAction, selectionAction) => {
      scenario.single = selectionAction;
      scenario.singleExecuting = selectionAction;
      scenario.multiple = defaultAction;
      scenario.empty = defaultAction;
    };

    beforeEach(() => {
      scenario = {
        fn: () => component.getCurrentButton(),
        singleExecuting: copyAction,
        single: copyAction,
        empty: addAction
      };
    });

    it('gets add for no, edit for single and delete for multiple selections', () => {
      setScenario(addAction, editAction);
      scenario.multiple = deleteAction;
      permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      permissionHelper.testScenarios(scenario);
    });

    it('gets add action except for selections where it shows edit action', () => {
      setScenario(addAction, editAction);
      permissionHelper.setPermissionsAndGetActions(1, 1, 0);
      permissionHelper.testScenarios(scenario);
    });

    it('gets add for no, copy for single and delete for multiple selections', () => {
      setScenario(addAction, copyAction);
      scenario.multiple = deleteAction;
      permissionHelper.setPermissionsAndGetActions(1, 0, 1);
      permissionHelper.testScenarios(scenario);
    });

    it('gets add action except for selections where it shows copy action', () => {
      setScenario(addAction, copyAction);
      permissionHelper.setPermissionsAndGetActions(1, 0, 0);
      permissionHelper.testScenarios(scenario);
    });

    it('should always get edit action except delete for multiple items', () => {
      setScenario(editAction, editAction);
      scenario.multiple = deleteAction;
      permissionHelper.setPermissionsAndGetActions(0, 1, 1);
      permissionHelper.testScenarios(scenario);
    });

    it('should always get edit action', () => {
      setScenario(editAction, editAction);
      permissionHelper.setPermissionsAndGetActions(0, 1, 0);
      permissionHelper.testScenarios(scenario);
    });

    it('should always get delete action', () => {
      setScenario(deleteAction, deleteAction);
      permissionHelper.setPermissionsAndGetActions(0, 0, 1);
      permissionHelper.testScenarios(scenario);
    });

    it('should not get any button with no permissions', () => {
      hiddenScenario();
      permissionHelper.setPermissionsAndGetActions(0, 0, 0);
      permissionHelper.testScenarios(scenario);
    });

    it('should not get any button if only a drop down should be shown', () => {
      hiddenScenario();
      component.onlyDropDown = 'Drop down label';
      permissionHelper.setPermissionsAndGetActions(1, 1, 1);
      permissionHelper.testScenarios(scenario);
    });
  });

  describe('show drop down', () => {
    const testShowDropDownActions = (perms, expected) => {
      permissionHelper.setPermissionsAndGetActions(perms[0], perms[1], perms[2]);
      expect(`${perms} ${component.showDropDownActions()}`).toBe(`${perms} ${expected}`);
    };

    it('is shown if multiple items are found depending on the permissions', () => {
      [[1, 0, 0], [1, 1, 1], [1, 1, 0], [1, 0, 1], [0, 1, 1], [0, 1, 0]].forEach((perms) => {
        testShowDropDownActions(perms, true);
      });
    });

    it('is not shown if only 1 or less items are found depending on the permissions', () => {
      [[0, 0, 1], [0, 0, 0]].forEach((perms) => {
        testShowDropDownActions(perms, false);
      });
    });
  });

  describe('with drop down only', () => {
    beforeEach(() => {
      component.onlyDropDown = 'displayMe';
    });

    it('should not return any button with getCurrentButton', () => {
      expect(component.getCurrentButton()).toBeFalsy();
    });
  });
});
