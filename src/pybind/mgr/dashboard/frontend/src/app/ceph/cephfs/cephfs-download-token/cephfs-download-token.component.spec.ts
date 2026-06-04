import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsDownloadTokenComponent } from './cephfs-download-token.component';

describe('CephfsDownloadTokenComponent', () => {
  let component: CephfsDownloadTokenComponent;
  let fixture: ComponentFixture<CephfsDownloadTokenComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsDownloadTokenComponent],
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
    let createElementSpy: jest.SpyInstance;
    let anchorEl: { href: string; download: string; click: jest.Mock };
    let originalCreateObjectURL: typeof window.URL.createObjectURL;
    let originalRevokeObjectURL: typeof window.URL.revokeObjectURL;

    beforeEach(() => {
      anchorEl = { href: '', download: '', click: jest.fn() };
      createElementSpy = jest.spyOn(document, 'createElement').mockReturnValue(anchorEl as any);

      originalCreateObjectURL = window.URL.createObjectURL;
      originalRevokeObjectURL = window.URL.revokeObjectURL;
      window.URL.createObjectURL = jest.fn().mockReturnValue('blob:fake-url');
      window.URL.revokeObjectURL = jest.fn();
    });

    afterEach(() => {
      createElementSpy.mockRestore();
      window.URL.createObjectURL = originalCreateObjectURL;
      window.URL.revokeObjectURL = originalRevokeObjectURL;
    });

    it('should download token file using siteName', () => {
      component.token = 'my-token';
      component.siteName = 'site-a';
      component.filesystemName = 'fs1';
      component.downloadToken();

      expect(createElementSpy).toHaveBeenCalledWith('a');
      expect(window.URL.createObjectURL).toHaveBeenCalled();
      expect(anchorEl.download).toBe('cephfs-bootstrap-token-site-a.txt');
      expect(anchorEl.click).toHaveBeenCalled();
      expect(window.URL.revokeObjectURL).toHaveBeenCalledWith('blob:fake-url');
    });

    it('should fall back to filesystemName when siteName is empty', () => {
      component.token = 'my-token';
      component.siteName = '';
      component.filesystemName = 'myfs';
      component.downloadToken();

      expect(anchorEl.download).toBe('cephfs-bootstrap-token-myfs.txt');
    });

    it('should not download when token is empty', () => {
      component.token = '';
      component.downloadToken();
      expect(createElementSpy).not.toHaveBeenCalled();
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
