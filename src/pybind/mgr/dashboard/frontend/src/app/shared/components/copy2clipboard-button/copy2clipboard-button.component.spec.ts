import { TestBed } from '@angular/core/testing';

import * as BrowserDetect from 'detect-browser';
import { ToastrService } from 'ngx-toastr';

import { configureTestBed } from '~/testing/unit-test-helper';
import { Copy2ClipboardButtonComponent } from './copy2clipboard-button.component';

describe('Copy2ClipboardButtonComponent', () => {
  let component: Copy2ClipboardButtonComponent;

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
    component = new Copy2ClipboardButtonComponent(null);
    expect(component).toBeTruthy();
  });

  describe('test onClick behaviours', () => {
    let toastrService: ToastrService;
    let queryFn: jasmine.Spy;
    let writeTextFn: jasmine.Spy;

    beforeEach(() => {
      toastrService = TestBed.inject(ToastrService);
      component = new Copy2ClipboardButtonComponent(toastrService);
      spyOn<any>(component, 'getText').and.returnValue('foo');
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
      component.onClick();
      expect(queryFn).not.toHaveBeenCalled();
      expect(writeTextFn).toHaveBeenCalledWith('foo');
    });

    it('should call permissions API', () => {
      spyOn(BrowserDetect, 'detect').and.returnValue({ name: 'chrome' });
      component.onClick();
      expect(queryFn).toHaveBeenCalled();
    });
  });
});
