import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { AuthStorageService } from '../services/auth-storage.service';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { AuthService } from './auth.service';

describe('AuthService', () => {
  let service: AuthService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [AuthService, AuthStorageService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(AuthService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it(
    'should login and save the user',
    fakeAsync(() => {
      const fakeCredentials = { username: 'foo', password: 'bar' };
      const fakeResponse = { username: 'foo', token: 'tokenbytes' };
      service.login(<any>fakeCredentials);
      const req = httpTesting.expectOne('api/auth');
      expect(req.request.method).toBe('POST');
      expect(req.request.body).toEqual(fakeCredentials);
      req.flush(fakeResponse);
      tick();
      expect(localStorage.getItem('dashboard_username')).toBe('foo');
      expect(localStorage.getItem('access_token')).toBe('tokenbytes');
    })
  );

  it(
    'should logout and remove the user',
    fakeAsync(() => {
      service.logout();
      const req = httpTesting.expectOne('api/auth');
      expect(req.request.method).toBe('DELETE');
      req.flush({ username: 'foo' });
      tick();
      expect(localStorage.getItem('dashboard_username')).toBe(null);
    })
  );
});
