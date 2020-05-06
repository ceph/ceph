import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, FormHelper, i18nProviders } from '../../../../testing/unit-test-helper';
import { AuthService } from '../../../shared/api/auth.service';
import { ComponentsModule } from '../../../shared/components/components.module';
import { CdFormGroup } from '../../../shared/forms/cd-form-group';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { SharedModule } from '../../../shared/shared.module';
import { LoginPasswordFormComponent } from './login-password-form.component';

describe('LoginPasswordFormComponent', () => {
  let component: LoginPasswordFormComponent;
  let fixture: ComponentFixture<LoginPasswordFormComponent>;
  let form: CdFormGroup;
  let formHelper: FormHelper;
  let httpTesting: HttpTestingController;
  let router: Router;
  let authStorageService: AuthStorageService;
  let authService: AuthService;

  configureTestBed({
    imports: [
      HttpClientTestingModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ComponentsModule,
      ToastrModule.forRoot(),
      SharedModule
    ],
    declarations: [LoginPasswordFormComponent],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginPasswordFormComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);
    router = TestBed.inject(Router);
    authStorageService = TestBed.inject(AuthStorageService);
    authService = TestBed.inject(AuthService);
    spyOn(router, 'navigate');
    fixture.detectChanges();
    form = component.userForm;
    formHelper = new FormHelper(form);
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should submit', () => {
    spyOn(component, 'onPasswordChange').and.callThrough();
    spyOn(authService, 'logout');
    spyOn(authStorageService, 'getUsername').and.returnValue('test1');
    formHelper.setMultipleValues({
      oldpassword: 'foo',
      newpassword: 'bar'
    });
    formHelper.setValue('confirmnewpassword', 'bar', true);
    component.onSubmit();
    const request = httpTesting.expectOne('api/user/test1/change_password');
    request.flush({});
    expect(component.onPasswordChange).toHaveBeenCalled();
    expect(authService.logout).toHaveBeenCalled();
  });

  it('should cancel', () => {
    spyOn(authService, 'logout');
    component.onCancel();
    expect(authService.logout).toHaveBeenCalled();
  });
});
