import { TestBed } from '@angular/core/testing';

import * as BrowserDetect from 'detect-browser';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

import { configureTestBed } from '~/testing/unit-test-helper';
import { Copy2ClipboardButtonComponent } from './copy2clipboard-button.component';

describe('Copy2ClipboardButtonComponent', () => {
  let component: Copy2ClipboardButtonComponent;

  configureTestBed({
    providers: [
      {
        provide: NotificationService,
        useValue: {
          show: jest.fn()
        }
      }
    ]
  });

  it('should create an instance', () => {
    component = new Copy2ClipboardButtonComponent(null);
    expect(component).toBeTruthy();
  });

  describe('test onClick behaviours', () => {
    let notificationService: NotificationService;
    let queryFn: jest.SpyInstance;
    let writeTextFn: jest.SpyInstance;

    beforeEach(() => {
      notificationService = TestBed.inject(NotificationService);
      component = new Copy2ClipboardButtonComponent(notificationService);
      jest.spyOn(component as any, 'getText').mockReturnValue('foo');
      Object.assign(navigator, {
        permissions: { query: jest.fn() },
        clipboard: {
          writeText: jest.fn()
        }
      });
      queryFn = jest.spyOn(navigator.permissions, 'query');
    });

    it('should not call permissions API', async () => {
      jest
        .spyOn(BrowserDetect, 'detect')
        .mockReturnValue({ name: 'firefox', version: '120.0.0', os: 'Linux', type: 'browser' });
      writeTextFn = jest.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

      await component.onClick();
      expect(queryFn).not.toHaveBeenCalled();
      expect(writeTextFn).toHaveBeenCalledWith('foo');
      expect(notificationService.show).toHaveBeenCalled();
    });

    it('should call permissions API', () => {
      jest
        .spyOn(BrowserDetect, 'detect')
        .mockReturnValue({ name: 'chrome', version: '120.0.0', os: 'Linux', type: 'browser' });
      jest.spyOn(navigator.permissions, 'query').mockResolvedValue({ state: 'granted' } as any);
      jest.spyOn(navigator.clipboard, 'writeText').mockResolvedValue(undefined);

      component.onClick();
      expect(queryFn).toHaveBeenCalled();
    });

    it('should show error notification when clipboard fails', async () => {
      jest.spyOn(BrowserDetect, 'detect').mockReturnValue({ name: 'firefox' } as any);
      jest.spyOn(navigator.clipboard, 'writeText').mockRejectedValue(new Error('Failed'));

      await component.onClick();
      await Promise.resolve();
      const calls = (notificationService.show as jest.Mock).mock.calls;
      expect(calls).toContainEqual([
        NotificationType.error,
        'Error',
        'Failed to copy text to the clipboard.'
      ]);
    });

    it('should disable copy when the provided source text is blank', () => {
      component.byId = false;
      (component as any).source = '  ';

      expect(component.isCopyDisabled).toBe(true);
    });
  });

  describe('textarea source', () => {
    let textarea: HTMLTextAreaElement;

    beforeEach(() => {
      component = new Copy2ClipboardButtonComponent(TestBed.inject(NotificationService));
      textarea = document.createElement('textarea');
      textarea.id = 'copy-source-token';
      textarea.value = '';
      textarea.textContent = '\n          ';
      document.body.appendChild(textarea);
      (component as any).source = textarea.id;
    });

    afterEach(() => {
      document.body.removeChild(textarea);
    });

    it('should stay disabled while the textarea has no value', () => {
      expect(component.isCopyDisabled).toBe(true);
    });

    it('should enable copy once the textarea has a value', () => {
      textarea.value = 'bootstrap-token';

      expect(component.isCopyDisabled).toBe(false);
    });
  });
});
