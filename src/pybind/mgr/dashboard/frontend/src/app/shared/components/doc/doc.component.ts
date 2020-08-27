import { Component, Input, OnInit } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { DocService } from '../../../shared/services/doc.service';

@Component({
  selector: 'cd-doc',
  templateUrl: './doc.component.html',
  styleUrls: ['./doc.component.scss']
})
export class DocComponent implements OnInit {
  @Input() section: string;
  @Input() docText = this.i18n(`documentation`);

  docUrl: string;

  constructor(private docService: DocService, private i18n: I18n) {}

  ngOnInit() {
    this.docService.subscribeOnce(this.section, (url: string) => {
      this.docUrl = url;
    });
  }
}
