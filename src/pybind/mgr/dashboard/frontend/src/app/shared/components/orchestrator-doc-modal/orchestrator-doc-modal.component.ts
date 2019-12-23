import { Component, OnInit } from '@angular/core';

import { BsModalRef } from 'ngx-bootstrap/modal';

@Component({
  selector: 'cd-orchestrator-doc-modal',
  templateUrl: './orchestrator-doc-modal.component.html',
  styleUrls: ['./orchestrator-doc-modal.component.scss']
})
export class OrchestratorDocModalComponent implements OnInit {
  actionDescription: string;
  itemDescription: string;

  constructor(public bsModalRef: BsModalRef) {}

  ngOnInit() {}

  onSubmit() {
    this.bsModalRef.hide();
  }
}
