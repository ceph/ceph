import { TestBed } from '@angular/core/testing';

import * as BrowserDetect from 'detect-browser';
import { ToastrService } from 'ngx-toastr';

import { configureTestBed } from '~/testing/unit-test-helper';
import { Copy2ClipboardButtonDirective } from './copy2clipboard-button.directive';

describe('Copy2clipboardButtonDirective', () => {
  let directive: Copy2ClipboardButtonDirective;

  configureTestBed({
    providers: [
      {
        provide: ToastrService,
        useValue: {
          error: () => true,
          success: () => true
        }
      }
    ]
  });

  it('should create an instance', () => {
    directive = new Copy2ClipboardButtonDirective(null, null, null);
    expect(directive).toBeTruthy();
  });

  describe('test onClick behaviours', () => {
    let toastrService: ToastrService;
    let queryFn: jasmine.Spy;
    let writeTextFn: jasmine.Spy;

    beforeEach(() => {
      toastrService = TestBed.inject(ToastrService);
      directive = new Copy2ClipboardButtonDirective(null, null, toastrService);
      spyOn<any>(directive, 'getText').and.returnValue('foo');
      Object.assign(navigator, {
        permissions: { query: jest.fn() },
        clipboard: {
          writeText: jest.fn()
        }
      });
      queryFn = spyOn(navigator.permissions, 'query');
    });

    it('should not call permissions API', () => {
      spyOn(BrowserDetect, 'detect').and.returnValue({ name: 'firefox' });
      writeTextFn = spyOn(navigator.clipboard, 'writeText').and.returnValue(
        new Promise<void>((resolve, _) => {
          resolve();
        })
      );
      directive.onClick();
      expect(queryFn).not.toHaveBeenCalled();
      expect(writeTextFn).toHaveBeenCalledWith('foo');
    });

    it('should call permissions API', () => {
      spyOn(BrowserDetect, 'detect').and.returnValue({ name: 'chrome' });
      directive.onClick();
      expect(queryFn).toHaveBeenCalled();
    });
  });
});
