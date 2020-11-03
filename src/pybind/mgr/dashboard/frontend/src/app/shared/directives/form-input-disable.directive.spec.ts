import { Component, DebugElement, Input } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { Permission } from '../models/permissions';
import { AuthStorageService } from '../services/auth-storage.service';
import { FormInputDisableDirective } from './form-input-disable.directive';
import { FormScopeDirective } from './form-scope.directive';

@Component({
  template: `
    <form cdFormScope="osd">
      <input type="checkbox" />
    </form>
  `
})
export class FormDisableComponent {}

class MockFormScopeDirective {
  @Input() cdFormScope = 'osd';
}

describe('FormInputDisableDirective', () => {
  let fakePermissions: Permission;
  let authStorageService: AuthStorageService;
  let directive: FormInputDisableDirective;
  let fixture: ComponentFixture<FormDisableComponent>;
  let inputElement: DebugElement;
  configureTestBed({
    declarations: [FormScopeDirective, FormInputDisableDirective, FormDisableComponent]
  });

  beforeEach(() => {
    directive = new FormInputDisableDirective(
      new MockFormScopeDirective(),
      new AuthStorageService(),
      null
    );

    fakePermissions = {
      create: false,
      update: false,
      read: false,
      delete: false
    };
    authStorageService = TestBed.get(AuthStorageService);
    spyOn(authStorageService, 'getPermissions').and.callFake(() => ({
      osd: fakePermissions
    }));

    fixture = TestBed.createComponent(FormDisableComponent);
    inputElement = fixture.debugElement.query(By.css('input'));
  });

  afterEach(() => {
    directive = null;
  });

  it('should create an instance', () => {
    expect(directive).toBeTruthy();
  });

  it('should disable the input if update permission is false', () => {
    fixture.detectChanges();
    expect(inputElement.nativeElement.disabled).toBeTruthy();
  });

  it('should not disable the input if update permission is true', () => {
    fakePermissions.update = true;
    fakePermissions.read = false;
    fixture.detectChanges();
    expect(inputElement.nativeElement.disabled).toBeFalsy();
  });
});
