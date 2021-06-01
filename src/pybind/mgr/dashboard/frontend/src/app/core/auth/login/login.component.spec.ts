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
    component.login();

    expect(routerNavigateSpy).toHaveBeenCalledTimes(1);
    expect(routerNavigateSpy).toHaveBeenCalledWith(['/']);
  });

  it('should show create cluster wizard if cluster creation was failed', () => {
    component.postInstalled = false;
    component.login();

    expect(routerNavigateSpy).toHaveBeenCalledTimes(1);
    expect(routerNavigateSpy).toHaveBeenCalledWith(['/create-cluster']);
  });
});
