import { AuthStorageService } from './auth-storage.service';

describe('AuthStorageService', () => {
  let service: AuthStorageService;
  const username = 'foobar';

  beforeEach(() => {
    service = new AuthStorageService();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should store username', () => {
    service.set(username, '');
    expect(localStorage.getItem('dashboard_username')).toBe(username);
  });

  it('should remove username', () => {
    service.set(username, '');
    service.remove();
    expect(localStorage.getItem('dashboard_username')).toBe(null);
  });

  it('should be loggedIn', () => {
    service.set(username, '');
    expect(service.isLoggedIn()).toBe(true);
  });

  it('should not be loggedIn', () => {
    service.remove();
    expect(service.isLoggedIn()).toBe(false);
  });

  it('should be SSO', () => {
    service.set(username, {}, true);
    expect(localStorage.getItem('sso')).toBe('true');
    expect(service.isSSO()).toBe(true);
  });

  it('should not be SSO', () => {
    service.set(username);
    expect(localStorage.getItem('sso')).toBe('false');
    expect(service.isSSO()).toBe(false);
  });
});
