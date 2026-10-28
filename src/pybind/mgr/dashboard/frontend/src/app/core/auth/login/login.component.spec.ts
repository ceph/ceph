import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of, throwError } from 'rxjs';

import { AuthService } from '~/app/shared/api/auth.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AuthModule } from '../auth.module';
import { LoginComponent } from './login.component';

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;
  let routerNavigateSpy: jasmine.Spy;
  let authServiceLoginSpy: jasmine.Spy;

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule, AuthModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginComponent);
    component = fixture.componentInstance;
    routerNavigateSpy = spyOn(TestBed.inject(Router), 'navigate');
    routerNavigateSpy.and.returnValue(true);
    authServiceLoginSpy = spyOn(TestBed.inject(AuthService), 'login');
    authServiceLoginSpy.and.returnValue(of(null));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should ensure no modal dialogs are opened', () => {
    component['modalService']['modalsCount'] = 2;
    component.ngOnInit();
    expect(component['modalService'].hasOpenModals()).toBeFalsy();
  });

  it('should not show create cluster wizard if cluster creation was successful', () => {
    component.postInstalled = true;
    const router = TestBed.inject(Router);
    const navigateByUrlSpy = spyOn(router, 'navigateByUrl').and.returnValue(Promise.resolve(true));
    component.login();

    expect(navigateByUrlSpy).toHaveBeenCalledWith('/');
    expect(routerNavigateSpy).not.toHaveBeenCalled();
  });

  it('should navigate to add-storage?welcome=true when cluster is not post-installed', () => {
    component.postInstalled = false;
    const router = TestBed.inject(Router);
    const navigateByUrlSpy = spyOn(router, 'navigateByUrl').and.returnValue(Promise.resolve(true));
    spyOn(TestBed.inject(AuthStorageService), 'getPermissions').and.returnValue({
      configOpt: { create: true, read: true, update: true, delete: true }
    } as any);
    component.login();

    expect(navigateByUrlSpy).toHaveBeenCalledWith('/add-storage?welcome=true');
    expect(routerNavigateSpy).not.toHaveBeenCalled();
  });

  it('should show inline error on failed login', () => {
    const err = {
      error: { detail: 'Invalid credentials', code: 'invalid_credentials', component: 'auth' },
      status: 400,
      statusText: 'Bad Request',
      url: 'api/auth',
      preventDefault: jasmine.createSpy('preventDefault')
    };
    authServiceLoginSpy.and.returnValue(throwError(err));
    component.model.password = 'secret';

    component.login();

    expect(component.errorMessage).toBe('Invalid credentials');
    expect(component.model.password).toBe('secret');
    expect(err.preventDefault).toHaveBeenCalled();
    expect(routerNavigateSpy).not.toHaveBeenCalled();
  });

  it('should clear error on retry', () => {
    component.errorMessage = 'Previous error';
    authServiceLoginSpy.and.returnValue(of(null));

    component.login();

    expect(component.errorMessage).toBe('');
  });
});
