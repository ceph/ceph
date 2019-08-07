import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, FormHelper, i18nProviders } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../../shared/components/components.module';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SharedModule } from '../../../shared/shared.module';
import { UserPasswordFormComponent } from './user-password-form.component';

describe('UserPasswordFormComponent', () => {
  let component: UserPasswordFormComponent;
  let fixture: ComponentFixture<UserPasswordFormComponent>;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  let httpTesting: HttpTestingController;
  let router: Router;
  let authStorageService: AuthStorageService;

  configureTestBed(
    {
      imports: [
        HttpClientTestingModule,
        RouterTestingModule,
        ReactiveFormsModule,
        ComponentsModule,
        ToastrModule.forRoot(),
        SharedModule
      ],
      declarations: [UserPasswordFormComponent],
      providers: i18nProviders
    },
    true
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(UserPasswordFormComponent);
    component = fixture.componentInstance;
    form = component.userForm;
    httpTesting = TestBed.get(HttpTestingController);
    router = TestBed.get(Router);
    authStorageService = TestBed.get(AuthStorageService);
    spyOn(router, 'navigate');
    fixture.detectChanges();
    formHelper = new FormHelper(form);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should validate old password required', () => {
    formHelper.expectErrorChange('oldpassword', '', 'required');
    formHelper.expectValidChange('oldpassword', 'foo');
  });

  it('should validate password match', () => {
    formHelper.setValue('newpassword', 'aaa');
    formHelper.expectErrorChange('confirmnewpassword', 'bbb', 'match');
    formHelper.expectValidChange('confirmnewpassword', 'aaa');
  });

  it('should validate password strength very strong', () => {
    formHelper.setValue('newpassword', 'testpassword#!$!@$');
    component.checkPassword('testpassword#!$!@$');
    expect(component.passwordStrengthDescription).toBe('Very strong');
    expect(component.passwordStrengthLevel).toBe('passwordStrengthLevel4');
  });

  it('should validate password strength strong', () => {
    formHelper.setValue('newpassword', 'testpassword0047!@');
    component.checkPassword('testpassword0047!@');
    expect(component.passwordStrengthDescription).toBe('Strong');
    expect(component.passwordStrengthLevel).toBe('passwordStrengthLevel3');
  });

  it('should validate password strength ok ', () => {
    formHelper.setValue('newpassword', 'mypassword1!@');
    component.checkPassword('mypassword1!@');
    expect(component.passwordStrengthDescription).toBe('OK');
    expect(component.passwordStrengthLevel).toBe('passwordStrengthLevel2');
  });

  it('should validate password strength weak', () => {
    formHelper.setValue('newpassword', 'mypassword1');
    component.checkPassword('mypassword1');
    expect(component.passwordStrengthDescription).toBe('Weak');
    expect(component.passwordStrengthLevel).toBe('passwordStrengthLevel1');
  });

  it('should validate password strength too weak', () => {
    formHelper.setValue('newpassword', 'abc0');
    component.checkPassword('abc0');
    expect(component.passwordStrengthDescription).toBe('Too weak');
    expect(component.passwordStrengthLevel).toBe('passwordStrengthLevel0');
  });

  it('should submit', () => {
    spyOn(authStorageService, 'getUsername').and.returnValue('xyz');
    formHelper.setMultipleValues({
      oldpassword: 'foo',
      newpassword: 'bar'
    });
    formHelper.setValue('confirmnewpassword', 'bar', true);
    component.onSubmit();
    const request = httpTesting.expectOne('api/user/xyz/change_password');
    expect(request.request.method).toBe('POST');
    expect(request.request.body).toEqual({
      old_password: 'foo',
      new_password: 'bar'
    });
    request.flush({});
    expect(router.navigate).toHaveBeenCalledWith(['/logout']);
  });
});
