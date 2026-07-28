import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { of } from 'rxjs';

import { AuthService } from '~/app/shared/api/auth.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { AuthModule } from '../auth.module';
import { LoginComponent } from './login.component';

describe('LoginComponent', () => {
  let component: LoginComponent;
  let fixture: ComponentFixture<LoginComponent>;
  let routerNavigateByUrlSpy: jasmine.Spy;
  let router: Router;
  let routerCreateUrlTreeSpy: jasmine.Spy;
  let authServiceLoginSpy: jasmine.Spy;

  configureTestBed({
    imports: [RouterTestingModule, HttpClientTestingModule, AuthModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoginComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    routerNavigateByUrlSpy = spyOn(router, 'navigateByUrl');
    routerNavigateByUrlSpy.and.returnValue(Promise.resolve(true));
    routerCreateUrlTreeSpy = spyOn(router, 'createUrlTree').and.callThrough();
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
    component.loginForm.setValue({ username: 'admin', password: 'admin' });
    component.login();

    expect(routerNavigateByUrlSpy).toHaveBeenCalledTimes(1);
    expect(routerNavigateByUrlSpy).toHaveBeenCalledWith('/');
  });

  it('should show create cluster wizard if cluster creation was failed', () => {
    component.postInstalled = false;
    component.loginForm.setValue({ username: 'admin', password: 'admin' });
    component.login();

    expect(routerNavigateByUrlSpy).toHaveBeenCalledTimes(1);
    expect(routerCreateUrlTreeSpy).toHaveBeenCalledWith(['/add-storage'], {
      queryParams: { welcome: true }
    });
  });
});
