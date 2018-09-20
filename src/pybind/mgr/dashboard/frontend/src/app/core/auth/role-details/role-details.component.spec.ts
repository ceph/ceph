import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastModule } from 'ng2-toastr';
import { TabsModule } from 'ngx-bootstrap';

import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { RoleDetailsComponent } from './role-details.component';

describe('RoleDetailsComponent', () => {
  let component: RoleDetailsComponent;
  let fixture: ComponentFixture<RoleDetailsComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        SharedModule,
        ToastModule.forRoot(),
        TabsModule.forRoot(),
        RouterTestingModule,
        HttpClientTestingModule
      ],
      declarations: [RoleDetailsComponent]
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RoleDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should create scopes permissions [1/2]', () => {
    component.scopes = ['log', 'rgw'];
    component.selection = new CdTableSelection();
    component.selection.selected = [
      {
        description: 'RGW Manager',
        name: 'rgw-manager',
        scopes_permissions: {
          rgw: ['read', 'create', 'update', 'delete']
        },
        system: true
      }
    ];
    component.selection.update();
    expect(component.scopes_permissions.length).toBe(0);
    component.ngOnChanges();
    expect(component.scopes_permissions).toEqual([
      { scope: 'log', read: false, create: false, update: false, delete: false },
      { scope: 'rgw', read: true, create: true, update: true, delete: true }
    ]);
  });

  it('should create scopes permissions [2/2]', () => {
    component.scopes = ['cephfs', 'log', 'rgw'];
    component.selection = new CdTableSelection();
    component.selection.selected = [
      {
        description: 'Test',
        name: 'test',
        scopes_permissions: {
          log: ['read', 'update'],
          rgw: ['read', 'create', 'update']
        },
        system: false
      }
    ];
    component.selection.update();
    expect(component.scopes_permissions.length).toBe(0);
    component.ngOnChanges();
    expect(component.scopes_permissions).toEqual([
      { scope: 'cephfs', read: false, create: false, update: false, delete: false },
      { scope: 'log', read: true, create: false, update: true, delete: false },
      { scope: 'rgw', read: true, create: true, update: true, delete: false }
    ]);
  });
});
