import { Component } from '@angular/core';

import { BsModalRef } from 'ngx-bootstrap/modal';

@Component({
  selector: 'cd-orchestrator-doc-modal',
  templateUrl: './orchestrator-doc-modal.component.html',
  styleUrls: ['./orchestrator-doc-modal.component.scss']
})
export class OrchestratorDocModalComponent {
  actionDescription: string;
  itemDescription: string;

  constructor(public bsModalRef: BsModalRef) {}

  onSubmit() {
    this.bsModalRef.hide();
  }
}
