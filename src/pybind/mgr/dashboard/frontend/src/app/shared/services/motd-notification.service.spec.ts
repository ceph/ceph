import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { Motd } from '~/app/shared/api/motd.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { MotdNotificationService } from './motd-notification.service';

describe('MotdNotificationService', () => {
  let service: MotdNotificationService;

  configureTestBed({
    providers: [MotdNotificationService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(MotdNotificationService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should hide [1]', () => {
    spyOn(service.motdSource, 'next');
    spyOn(service.motdSource, 'getValue').and.returnValue({
      severity: 'info',
      expires: '',
      message: 'foo',
      md5: 'acbd18db4cc2f85cedef654fccc4a4d8'
    });
    service.hide();
    expect(localStorage.getItem('dashboard_motd_hidden')).toBe(
      'info:acbd18db4cc2f85cedef654fccc4a4d8'
    );
    expect(sessionStorage.getItem('dashboard_motd_hidden')).toBeNull();
    expect(service.motdSource.next).toBeCalledWith(null);
  });

  it('should hide [2]', () => {
    spyOn(service.motdSource, 'getValue').and.returnValue({
      severity: 'warning',
      expires: '',
      message: 'bar',
      md5: '37b51d194a7513e45b56f6524f2d51f2'
    });
    service.hide();
    expect(sessionStorage.getItem('dashboard_motd_hidden')).toBe(
      'warning:37b51d194a7513e45b56f6524f2d51f2'
    );
    expect(localStorage.getItem('dashboard_motd_hidden')).toBeNull();
  });

  it('should process response [1]', () => {
    const motd: Motd = {
      severity: 'danger',
      expires: '',
      message: 'foo',
      md5: 'acbd18db4cc2f85cedef654fccc4a4d8'
    };
    spyOn(service.motdSource, 'next');
    service.processResponse(motd);
    expect(service.motdSource.next).toBeCalledWith(motd);
  });

  it('should process response [2]', () => {
    const motd: Motd = {
      severity: 'warning',
      expires: '',
      message: 'foo',
      md5: 'acbd18db4cc2f85cedef654fccc4a4d8'
    };
    localStorage.setItem('dashboard_motd_hidden', 'info');
    service.processResponse(motd);
    expect(sessionStorage.getItem('dashboard_motd_hidden')).toBeNull();
    expect(localStorage.getItem('dashboard_motd_hidden')).toBeNull();
  });

  it('should process response [3]', () => {
    const motd: Motd = {
      severity: 'info',
      expires: '',
      message: 'foo',
      md5: 'acbd18db4cc2f85cedef654fccc4a4d8'
    };
    spyOn(service.motdSource, 'next');
    localStorage.setItem('dashboard_motd_hidden', 'info:acbd18db4cc2f85cedef654fccc4a4d8');
    service.processResponse(motd);
    expect(service.motdSource.next).not.toBeCalled();
  });

  it('should process response [4]', () => {
    const motd: Motd = {
      severity: 'info',
      expires: '',
      message: 'foo',
      md5: 'acbd18db4cc2f85cedef654fccc4a4d8'
    };
    spyOn(service.motdSource, 'next');
    localStorage.setItem('dashboard_motd_hidden', 'info:37b51d194a7513e45b56f6524f2d51f2');
    service.processResponse(motd);
    expect(service.motdSource.next).toBeCalled();
  });

  it('should process response [5]', () => {
    const motd: Motd = {
      severity: 'info',
      expires: '',
      message: 'foo',
      md5: 'acbd18db4cc2f85cedef654fccc4a4d8'
    };
    spyOn(service.motdSource, 'next');
    localStorage.setItem('dashboard_motd_hidden', 'danger:acbd18db4cc2f85cedef654fccc4a4d8');
    service.processResponse(motd);
    expect(service.motdSource.next).toBeCalled();
  });
});
