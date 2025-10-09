import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { PipesModule } from '~/app/shared/pipes/pipes.module';

import { ComponentsModule } from '~/app/shared/components/components.module';
import { CdTableAction } from '~/app/shared/models/cd-table-action';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { Permission } from '~/app/shared/models/permissions';
import { configureTestBed, PermissionHelper } from '~/testing/unit-test-helper';
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
  let permissionHelper: PermissionHelper;

  configureTestBed({
    declarations: [TableActionsComponent],
    imports: [ComponentsModule, PipesModule, RouterTestingModule]
  });

  beforeEach(() => {
    addAction = {
      permission: 'create',
      icon: 'fa-plus',
      canBePrimary: (selection: CdTableSelection) => !selection.hasSelection,
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
      canBePrimary: (selection: CdTableSelection) => selection.hasSingleSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSingleSelection || selection.first().cdExecuting,
      name: 'Copy'
    };
    deleteAction = {
      permission: 'delete',
      icon: 'fa-times',
      canBePrimary: (selection: CdTableSelection) => selection.hasSelection,
      disable: (selection: CdTableSelection) =>
        !selection.hasSelection || selection.first().cdExecuting,
      name: 'Delete'
    };
    protectAction = {
      permission: 'update',
      icon: 'fa-lock',
      canBePrimary: () => false,
      visible: (selection: CdTableSelection) => selection.hasSingleSelection,
      name: 'Protect'
    };
    unprotectAction = {
      permission: 'update',
      icon: 'fa-unlock',
      canBePrimary: () => false,
      visible: (selection: CdTableSelection) => !selection.hasSingleSelection,
      name: 'Unprotect'
    };
    fixture = TestBed.createComponent(TableActionsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    component.permission = new Permission();
    component.permission.read = true;
    component.tableActions = [
      addAction,
      editAction,
      protectAction,
      unprotectAction,
      copyAction,
      deleteAction
    ];
    permissionHelper = new PermissionHelper(component.permission);
    permissionHelper.setPermissionsAndGetActions(component.tableActions);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call ngInit without permissions', () => {
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

  it('should test all TableActions combinations', () => {
    const tableActions: TableActionsComponent = permissionHelper.setPermissionsAndGetActions(
      component.tableActions
    );
    expect(tableActions).toEqual({
      'create,update,delete': {
        actions: ['Add', 'Edit', 'Protect', 'Unprotect', 'Copy', 'Delete'],
        primary: {
          multiple: 'Add',
          executing: 'Add',
          single: 'Add',
          no: 'Add'
        }
      },
      'create,update': {
        actions: ['Add', 'Edit', 'Protect', 'Unprotect', 'Copy'],
        primary: {
          multiple: 'Add',
          executing: 'Add',
          single: 'Add',
          no: 'Add'
        }
      },
      'create,delete': {
        actions: ['Add', 'Copy', 'Delete'],
        primary: {
          multiple: 'Add',
          executing: 'Add',
          single: 'Add',
          no: 'Add'
        }
      },
      create: {
        actions: ['Add', 'Copy'],
        primary: {
          multiple: 'Add',
          executing: 'Add',
          single: 'Add',
          no: 'Add'
        }
      },
      'update,delete': {
        actions: ['Edit', 'Protect', 'Unprotect', 'Delete'],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      },
      update: {
        actions: ['Edit', 'Protect', 'Unprotect'],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      },
      delete: {
        actions: ['Delete'],
        primary: {
          multiple: 'Delete',
          executing: 'Delete',
          single: 'Delete',
          no: 'Delete'
        }
      },
      'no-permissions': {
        actions: [],
        primary: {
          multiple: '',
          executing: '',
          single: '',
          no: ''
        }
      }
    });
  });

  it('should convert any name to a proper CSS class', () => {
    expect(component.toClassName({ name: 'Create' } as CdTableAction)).toBe('create');
    expect(component.toClassName({ name: 'Mark x down' } as CdTableAction)).toBe('mark-x-down');
    expect(component.toClassName({ name: '?Su*per!' } as CdTableAction)).toBe('super');
  });

  describe('useDisableDesc', () => {
    it('should return a description if disable method returns a string', () => {
      const deleteWithDescAction: CdTableAction = {
        permission: 'delete',
        icon: 'fa-times',
        canBePrimary: (selection: CdTableSelection) => selection.hasSelection,
        disable: () => {
          return 'Delete action disabled description';
        },
        name: 'DeleteDesc'
      };

      expect(component.useDisableDesc(deleteWithDescAction)).toBe(
        'Delete action disabled description'
      );
    });

    it('should return no description if disable does not return string', () => {
      expect(component.useDisableDesc(deleteAction)).toBeUndefined();
    });
  });

  describe('useClickAction', () => {
    const editClickAction: CdTableAction = {
      permission: 'update',
      icon: 'fa-pencil',
      name: 'Edit',
      click: () => {
        return 'Edit action click';
      }
    };

    it('should call click action if action is not disabled', () => {
      editClickAction.disable = () => {
        return false;
      };
      expect(component.useClickAction(editClickAction)).toBe('Edit action click');
    });

    it('should not call click action if action is disabled', () => {
      editClickAction.disable = () => {
        return true;
      };
      expect(component.useClickAction(editClickAction)).toBeFalsy();
    });
  });
});
