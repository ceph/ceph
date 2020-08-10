import { Component, Input, OnInit } from '@angular/core';

import { DocService } from '../../../shared/services/doc.service';

@Component({
  selector: 'cd-doc',
  templateUrl: './doc.component.html',
  styleUrls: ['./doc.component.scss']
})
export class DocComponent implements OnInit {
  @Input() section: string;
  @Input() docText = $localize`documentation`;

  docUrl: string;

  constructor(private docService: DocService) {}

  ngOnInit() {
    this.docService.subscribeOnce(this.section, (url: string) => {
      this.docUrl = url;
    });
  }
}
