import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TableActionsComponent } from './table-actions.component';
import { CdTableAction } from '../../models/cd-table-action';
import { CdTableSelection } from '../../models/cd-table-selection';
import { Permission } from '../../models/permissions';

describe('TableActionsComponent', () => {
  let component: TableActionsComponent;
  let fixture: ComponentFixture<TableActionsComponent>;

  const mockPermission: Permission = {
    create: true,
    update: true,
    delete: false,
    read: false
  };

  const mockSelection: CdTableSelection = {
    hasSingleSelection: true,
    first: () => ({ cdExecuting: false }),
  } as any;

  const actions: CdTableAction[] = [
    {
      name: 'Create',
      permission: 'create',
      visible: () => true,
      disable: () => false,
      click: jasmine.createSpy('click'),
      title: 'Create item',
      icon: ''
    },
    {
      name: 'Update',
      permission: 'update',
      visible: () => true,
      disable: () => false,
      click: jasmine.createSpy('click'),
      title: 'Update item',
      icon: ''
    },
    {
      name: 'Delete',
      permission: 'delete',
      visible: () => true,
      disable: () => false,
      click: jasmine.createSpy('click'),
      title: 'Delete item',
      icon: ''
    }
  ];

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TableActionsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(TableActionsComponent);
    component = fixture.componentInstance;
    component.permission = mockPermission;
    component.selection = mockSelection;
    component.tableActions = [...actions];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should filter actions based on permissions', () => {
    component.permission = { create: true, update: false, delete: false, read: false };
    component.tableActions = [...actions];
    component.onSelectionChange();
    // Only actions with permission 'create' and visible() === true should remain
    expect(component.tableActions.length).toBe(1);
    expect(component.tableActions[0].permission).toBe('create');
  });

  it('should update dropDownActions on selection change', () => {
    component.tableActions = [...actions];
    component.onSelectionChange();
    // dropDownActions should be updated to match visible actions
    expect(component.dropDownActions.length).toBe(3);
  });

  it('should set currentAction to first action if dropDownOnly is undefined', () => {
    component.dropDownActions = [...actions];
    component.dropDownOnly = undefined;
    component.onSelectionChange();
    expect(component.currentAction?.permission).toBe('create');
  });

  it('should set currentAction to the only action if one action is available', () => {
    component.dropDownActions = [actions[1]];
    component.dropDownOnly = undefined;
    component.onSelectionChange();
    expect(component.currentAction?.permission).toBe('update');
  });

  it('should return correct class name', () => {
    const action: CdTableAction = { name: 'Create Action', permission: 'create' } as any;
    expect(component.toClassName(action)).toBe('create-action');
  });

  it('should use router link if available and not disabled', () => {
    const action: CdTableAction = {
      ...actions[0],
      routerLink: '/test',
      disable: () => false
    };
    spyOn(component, 'disableSelectionAction').and.returnValue(false);
    expect(component.useRouterLink(action)).toBe('/test');
  });

  it('should return undefined for router link if disabled', () => {
    const action: CdTableAction = {
      ...actions[0],
      routerLink: '/test',
      disable: () => true
    };
    spyOn(component, 'disableSelectionAction').and.returnValue(true);
    expect(component.useRouterLink(action)).toBeUndefined();
  });

  it('should disable selection action for update/delete with no selection', () => {
    const action: CdTableAction = { permission: 'update' } as any;
    component.selection = { hasSingleSelection: false, first: () => null } as any;
    expect(component.disableSelectionAction(action)).toBe(true);
  });

  it('should call click handler if action is not disabled', () => {
    const action: CdTableAction = { ...actions[0], click: jasmine.createSpy('click'), disable: () => false };
    spyOn(component, 'disableSelectionAction').and.returnValue(false);
    component.useClickAction(action);
    expect(action.click).toHaveBeenCalled();
  });

  it('should not call click handler if action is disabled', () => {
    const action: CdTableAction = { ...actions[0], click: jasmine.createSpy('click'), disable: () => true };
    spyOn(component, 'disableSelectionAction').and.returnValue(true);
    component.useClickAction(action);
    expect(action.click).not.toHaveBeenCalled();
  });

  it('should return disable description if disable returns a string', () => {
    const action: CdTableAction = {
      ...actions[0],
      disable: () => 'Disabled reason',
      title: 'Action title'
    };
    expect(component.useDisableDesc(action)).toBe('Disabled reason');
  });

  it('should return title if disable is not present', () => {
    const action: CdTableAction = {
      ...actions[0],
      disable: undefined,
      title: 'Action title'
    };
    expect(component.useDisableDesc(action)).toBe('Action title');
  });

  it('should return undefined if neither disable nor title is present', () => {
    const action: CdTableAction = {
      ...actions[0],
      disable: undefined,
      title: undefined
    };
    expect(component.useDisableDesc(action)).toBeUndefined();
  });
});