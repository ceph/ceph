import { Directive, HostListener, Input } from '@angular/core';

import { saveAs } from 'file-saver';

@Directive({
  selector: '[cdTextToDownload]'
})
export class TextToDownloadDirective {
  @Input()
  cdTextToDownload = '';
  @Input()
  filename = '';

  constructor() {}

  @HostListener('click')
  download() {
    saveAs(new Blob([this.cdTextToDownload]), this.filename);
  }
}
