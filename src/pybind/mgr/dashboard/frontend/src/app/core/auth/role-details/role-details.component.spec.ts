import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { RoleDetailsComponent } from './role-details.component';

describe('RoleDetailsComponent', () => {
  let component: RoleDetailsComponent;
  let fixture: ComponentFixture<RoleDetailsComponent>;

  configureTestBed({
    imports: [SharedModule, RouterTestingModule, HttpClientTestingModule, NgbNavModule],
    declarations: [RoleDetailsComponent],
    providers: i18nProviders
  });

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
    component.selection = {
      description: 'RGW Manager',
      name: 'rgw-manager',
      scopes_permissions: {
        rgw: ['read', 'create', 'update', 'delete']
      },
      system: true
    };
    expect(component.scopes_permissions.length).toBe(0);
    component.ngOnChanges();
    expect(component.scopes_permissions).toEqual([
      { scope: 'log', read: false, create: false, update: false, delete: false },
      { scope: 'rgw', read: true, create: true, update: true, delete: true }
    ]);
  });

  it('should create scopes permissions [2/2]', () => {
    component.scopes = ['cephfs', 'log', 'rgw'];
    component.selection = {
      description: 'Test',
      name: 'test',
      scopes_permissions: {
        log: ['read', 'update'],
        rgw: ['read', 'create', 'update']
      },
      system: false
    };
    expect(component.scopes_permissions.length).toBe(0);
    component.ngOnChanges();
    expect(component.scopes_permissions).toEqual([
      { scope: 'cephfs', read: false, create: false, update: false, delete: false },
      { scope: 'log', read: true, create: false, update: true, delete: false },
      { scope: 'rgw', read: true, create: true, update: true, delete: false }
    ]);
  });
});
