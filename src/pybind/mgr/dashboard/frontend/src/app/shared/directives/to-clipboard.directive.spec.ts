import { TestBed } from '@angular/core/testing';
import { ToastrModule, ToastrService } from 'ngx-toastr';
import { configureTestBed } from '../../../testing/unit-test-helper';
import { ToClipboardDirective } from './to-clipboard.directive';

describe('ToClipboardDirective', () => {
  let directive: ToClipboardDirective;

  configureTestBed({
    imports: [ToastrModule.forRoot()]
  });

  beforeEach(() => {
    directive = new ToClipboardDirective(TestBed.get(ToastrService));
  });

  it('should create an instance', () => {
    expect(directive).toBeTruthy();
  });

  it('should copy content to the clipboard', () => {
    expect(document.execCommand).toBeUndefined();

    document.execCommand = (..._args): boolean => true;
    spyOn(document, 'execCommand').and.stub();

    directive.data = 'test data';
    directive.copyToClipboard();

    expect(document.execCommand).toHaveBeenCalledTimes(1);
  });
});
