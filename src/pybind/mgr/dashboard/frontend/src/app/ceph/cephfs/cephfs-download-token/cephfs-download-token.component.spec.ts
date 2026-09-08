import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NotificationService } from '~/app/shared/services/notification.service';
import { TextToDownloadService } from '~/app/shared/services/text-to-download.service';
import { CephfsDownloadTokenComponent } from './cephfs-download-token.component';

describe('CephfsDownloadTokenComponent', () => {
  let component: CephfsDownloadTokenComponent;
  let fixture: ComponentFixture<CephfsDownloadTokenComponent>;

  const notificationServiceMock = {
    show: jest.fn()
  };

  const textToDownloadServiceMock = {
    download: jest.fn()
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    await TestBed.configureTestingModule({
      declarations: [CephfsDownloadTokenComponent],
      providers: [
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: TextToDownloadService, useValue: textToDownloadServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsDownloadTokenComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('downloadToken', () => {
    it('should download token file using siteName', () => {
      component.token = 'my-token';
      component.siteName = 'site-a';
      component.filesystemName = 'fs1';
      component.downloadToken();

      expect(textToDownloadServiceMock.download).toHaveBeenCalledWith(
        'my-token',
        'cephfs-bootstrap-token-site-a.txt'
      );
    });

    it('should fall back to filesystemName when siteName is empty', () => {
      component.token = 'my-token';
      component.siteName = '';
      component.filesystemName = 'myfs';
      component.downloadToken();

      expect(textToDownloadServiceMock.download).toHaveBeenCalledWith(
        'my-token',
        'cephfs-bootstrap-token-myfs.txt'
      );
    });

    it('should sanitize invalid filename characters', () => {
      component.token = 'my-token';
      component.siteName = 'Foo/Bar';
      component.downloadToken();

      expect(textToDownloadServiceMock.download).toHaveBeenCalledWith(
        'my-token',
        'cephfs-bootstrap-token-Foo_Bar.txt'
      );
    });

    it('should not download when token is empty', () => {
      component.token = '';
      component.downloadToken();
      expect(textToDownloadServiceMock.download).not.toHaveBeenCalled();
    });
  });

  describe('onClose', () => {
    it('should emit closed event', () => {
      const closedSpy = jest.spyOn(component.closed, 'emit');
      component.onClose();
      expect(closedSpy).toHaveBeenCalled();
    });
  });
});
