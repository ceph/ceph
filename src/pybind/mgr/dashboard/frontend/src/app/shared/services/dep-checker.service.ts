import { Injectable } from '@angular/core';

import { OrchestratorService } from '../api/orchestrator.service';
import { OrchestratorDocModalComponent } from '../components/orchestrator-doc-modal/orchestrator-doc-modal.component';
import { ModalService } from './modal.service';

@Injectable({
  providedIn: 'root'
})
export class DepCheckerService {
  constructor(private orchService: OrchestratorService, private modalService: ModalService) {}

  /**
   * Check if orchestrator is available. Display an information modal if not.
   * If orchestrator is available, then the provided function will be called.
   * This helper function can be used with table actions.
   * @param {string} actionDescription name of the action.
   * @param {string} itemDescription the item's name that the action operates on.
   * @param {Function} func the function to be called if orchestrator is available.
   */
  checkOrchestratorOrModal(actionDescription: string, itemDescription: string, func: Function) {
    this.orchService.status().subscribe((status) => {
      if (status.available) {
        func();
      } else {
        this.modalService.show(OrchestratorDocModalComponent, {
          actionDescription: actionDescription,
          itemDescription: itemDescription
        });
      }
    });
  }
}
