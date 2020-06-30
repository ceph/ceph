import { Component } from '@angular/core';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

@Component({
  selector: 'cd-orchestrator-doc-modal',
  templateUrl: './orchestrator-doc-modal.component.html',
  styleUrls: ['./orchestrator-doc-modal.component.scss']
})
export class OrchestratorDocModalComponent {
  actionDescription: string;
  itemDescription: string;

  constructor(public activeModal: NgbActiveModal) {}

  onSubmit() {
    this.activeModal.close();
  }
}
