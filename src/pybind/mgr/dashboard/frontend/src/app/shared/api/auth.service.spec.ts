import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';
import { Router, Routes } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { AuthStorageService } from '../services/auth-storage.service';
import { AuthService } from './auth.service';

describe('AuthService', () => {
  let service: AuthService;
  let httpTesting: HttpTestingController;

  const routes: Routes = [{ path: 'login', children: [] }];

  configureTestBed({
    providers: [AuthService, AuthStorageService],
    imports: [HttpClientTestingModule, RouterTestingModule.withRoutes(routes)]
  });

  beforeEach(() => {
    service = TestBed.inject(AuthService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should login and save the user', fakeAsync(() => {
    const fakeCredentials = { username: 'foo', password: 'bar' };
    const fakeResponse = { username: 'foo', token: 'tokenbytes' };
    service.login(fakeCredentials).subscribe();
    const req = httpTesting.expectOne('api/auth');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(fakeCredentials);
    req.flush(fakeResponse);
    tick();
    expect(localStorage.getItem('dashboard_username')).toBe('foo');
    expect(localStorage.getItem('access_token')).toBe('tokenbytes');
  }));

  it('should logout and remove the user', () => {
    const router = TestBed.inject(Router);
    spyOn(router, 'navigate').and.stub();

    service.logout();
    const req = httpTesting.expectOne('api/auth/logout');
    expect(req.request.method).toBe('POST');
    req.flush({ redirect_url: '#/login' });
    expect(localStorage.getItem('dashboard_username')).toBe(null);
    expect(router.navigate).toBeCalledTimes(1);
  });
});
