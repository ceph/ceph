import { Directive, HostListener, Input } from '@angular/core';
import { ToastrService } from 'ngx-toastr';

@Directive({
  selector: '[cdToClipboard]'
})
export class ToClipboardDirective {
  @Input()
  data = '';

  constructor(private toastr: ToastrService) {}

  @HostListener('click')
  copyToClipboard() {
    if (this.data) {
      const elem = document.createElement('textarea');
      elem.value = this.data;
      document.body.appendChild(elem);
      elem.select();
      document.execCommand('copy');
      document.body.removeChild(elem);

      this.toastr.success('Copied text to the clipboard successfully.');
    }
  }
}
