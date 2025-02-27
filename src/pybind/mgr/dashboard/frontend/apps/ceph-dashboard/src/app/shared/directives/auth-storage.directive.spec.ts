import { Component } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Permissions } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AuthStorageDirective } from './auth-storage.directive';
@Component({
  template: `<div id="permitted" *cdScope="condition; matchAll: matchAll"></div>`
})
export class AuthStorageDirectiveTestComponent {
  condition: string | string[] | object;
  matchAll = true;
}

describe('AuthStorageDirective', () => {
  let fixture: ComponentFixture<AuthStorageDirectiveTestComponent>;
  let component: AuthStorageDirectiveTestComponent;
  let getPermissionsSpy: jasmine.Spy;
  let el: HTMLElement;

  configureTestBed({
    declarations: [AuthStorageDirective, AuthStorageDirectiveTestComponent],
    providers: [AuthStorageService]
  });

  beforeEach(() => {
    getPermissionsSpy = spyOn(TestBed.inject(AuthStorageService), 'getPermissions');
    getPermissionsSpy.and.returnValue(new Permissions({ osd: ['read'], rgw: ['read'] }));
    fixture = TestBed.createComponent(AuthStorageDirectiveTestComponent);
    el = fixture.debugElement.nativeElement;
    component = fixture.componentInstance;
  });

  it('should show div on valid condition', () => {
    // String condition
    component.condition = 'rgw';
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeTruthy();

    // Array condition
    component.condition = ['osd', 'rgw'];
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeTruthy();

    // Object condition
    component.condition = { rgw: ['read'], osd: ['read'] };
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeTruthy();
  });

  it('should show div with loose matching', () => {
    component.matchAll = false;
    fixture.detectChanges();

    // Array condition
    component.condition = ['configOpt', 'osd', 'rgw'];
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeTruthy();

    // Object condition
    component.condition = { rgw: ['read', 'update', 'fake'], osd: ['read'] };
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeTruthy();
  });

  it('should not show div on invalid condition', () => {
    // String condition
    component.condition = 'fake';
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeFalsy();

    // Array condition
    component.condition = ['configOpt', 'osd', 'rgw'];
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeFalsy();

    // Object condition
    component.condition = { rgw: ['read', 'update'], osd: ['read'] };
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeFalsy();
  });

  it('should hide div on condition change', () => {
    component.condition = 'osd';
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeTruthy();

    component.condition = 'grafana';
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeFalsy();
  });

  it('should hide div on permission change', () => {
    component.condition = ['osd'];
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeTruthy();

    getPermissionsSpy.and.returnValue(new Permissions({}));
    component.condition = 'osd';
    fixture.detectChanges();
    expect(el.querySelector('#permitted')).toBeFalsy();
  });
});
