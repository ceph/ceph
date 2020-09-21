import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { RbdMirroringService } from '../../../../shared/api/rbd-mirroring.service';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { FinishedTask } from '../../../../shared/models/finished-task';
import { TaskWrapperService } from '../../../../shared/services/task-wrapper.service';

@Component({
  selector: 'cd-edit-site-mode-modal',
  templateUrl: './edit-site-name-modal.component.html',
  styleUrls: ['./edit-site-name-modal.component.scss']
})
export class EditSiteNameModalComponent implements OnInit {
  siteName: string;

  editSiteNameForm: CdFormGroup;

  constructor(
    public activeModal: NgbActiveModal,
    private rbdMirroringService: RbdMirroringService,
    private taskWrapper: TaskWrapperService
  ) {
    this.createForm();
  }

  createForm() {
    this.editSiteNameForm = new CdFormGroup({
      siteName: new FormControl('', {})
    });
  }

  ngOnInit() {
    this.editSiteNameForm.get('siteName').setValue(this.siteName);
    this.rbdMirroringService.getSiteName().subscribe((response: any) => {
      this.editSiteNameForm.get('siteName').setValue(response.site_name);
    });
  }

  update() {
    const action = this.taskWrapper.wrapTaskAroundCall({
      task: new FinishedTask('rbd/mirroring/site_name/edit', {}),
      call: this.rbdMirroringService.setSiteName(this.editSiteNameForm.getValue('siteName'))
    });

    action.subscribe({
      error: () => this.editSiteNameForm.setErrors({ cdSubmitButton: true }),
      complete: () => {
        this.rbdMirroringService.refresh();
        this.activeModal.close();
      }
    });
  }
}
