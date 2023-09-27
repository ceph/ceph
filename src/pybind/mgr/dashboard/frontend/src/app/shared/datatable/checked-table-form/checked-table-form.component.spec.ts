import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CheckedTableFormComponent } from './checked-table-form.component';
import { TableComponent } from '../table/table.component';
import { TableKeyValueComponent } from '../table-key-value/table-key-value.component';
import { TablePaginationComponent } from '../table-pagination/table-pagination.component';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { FormHelper, configureTestBed } from '~/testing/unit-test-helper';
import { CdFormGroup } from '../../forms/cd-form-group';
import { FormControl } from '@angular/forms';

describe('CheckedTableFormComponent', () => {
  let component: CheckedTableFormComponent;
  let fixture: ComponentFixture<CheckedTableFormComponent>;
  let formHelper: FormHelper;
  let form: CdFormGroup;

  let fakeColumns = [
    {
      prop: 'scope',
      name: $localize`All`,
      flexGrow: 1
    },
    {
      prop: 'read',
      name: $localize`Read`,
      flexGrow: 1
    },
    {
      prop: 'write',
      name: $localize`Write`,
      flexGrow: 1
    },
    {
      prop: 'execute',
      name: $localize`Execute`,
      flexGrow: 1
    }
  ];

  configureTestBed({
    declarations: [
      CheckedTableFormComponent,
      TableComponent,
      TableKeyValueComponent,
      TablePaginationComponent
    ],
    imports: [NgxDatatableModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CheckedTableFormComponent);
    component = fixture.componentInstance;
    component.columns = fakeColumns;
    component.data = [
      { scope: 'owner', read: true, write: true, execute: true },
      { scope: 'group', read: true, write: true, execute: true },
      { scope: 'other', read: true, write: true, execute: true }
    ];
    component.scopes = ['owner', 'group', 'others'];
    component.form = new CdFormGroup({
      scopes_permissions: new FormControl({})
    });
    component.inputField = 'scopes_permissions';
    component.isTableForOctalMode = true;
    form = component.form;
    formHelper = new FormHelper(form);
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should check all perms for a scope', () => {
    formHelper.setValue('scopes_permissions', { owner: ['read'] });
    component.onClickCellCheckbox('group', 'scope');
    const scopes_permissions = form.getValue('scopes_permissions');
    expect(Object.keys(scopes_permissions)).toContain('group');
    expect(scopes_permissions['group']).toEqual(['read', 'write', 'execute'].sort());
  });

  it('should uncheck all perms for a scope', () => {
    formHelper.setValue('scopes_permissions', { owner: ['read', 'write', 'execute'] });
    component.onClickCellCheckbox('owner', 'scope');
    const scopes_permissions = form.getValue('scopes_permissions');
    expect(Object.keys(scopes_permissions)).not.toContain('owner');
  });

  it('should uncheck all scopes and perms', () => {
    component.scopes = ['owner', 'group'];
    formHelper.setValue('scopes_permissions', {
      owner: ['read', 'execute'],
      group: ['write']
    });
    component.onClickHeaderCheckbox('scope', ({
      target: { checked: false }
    } as unknown) as Event);
    const scopes_permissions = form.getValue('scopes_permissions');
    expect(scopes_permissions).toEqual({});
  });

  it('should check all scopes and perms', () => {
    component.scopes = ['owner', 'group'];
    formHelper.setValue('scopes_permissions', {
      owner: ['read', 'write'],
      group: ['execute']
    });
    component.onClickHeaderCheckbox('scope', ({ target: { checked: true } } as unknown) as Event);
    const scopes_permissions = form.getValue('scopes_permissions');
    const keys = Object.keys(scopes_permissions);
    expect(keys).toEqual(['owner', 'group']);
    keys.forEach((key) => {
      expect(scopes_permissions[key].sort()).toEqual(['execute', 'read', 'write']);
    });
  });

  it('should check if column is checked', () => {
    component.data = [
      { scope: 'a', read: true, write: true, execute: true },
      { scope: 'b', read: false, write: true, execute: false }
    ];
    expect(component.isRowChecked('a')).toBeTruthy();
    expect(component.isRowChecked('b')).toBeFalsy();
    expect(component.isRowChecked('c')).toBeFalsy();
  });

  it('should check if header is checked', () => {
    component.data = [
      { scope: 'a', read: true, write: true, execute: true },
      { scope: 'b', read: false, write: true, execute: false }
    ];
    expect(component.isHeaderChecked('read')).toBeFalsy();
    expect(component.isHeaderChecked('write')).toBeTruthy();
    expect(component.isHeaderChecked('execute')).toBeFalsy();
  });
});
