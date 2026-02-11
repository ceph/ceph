import { Component, Input } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';

@Component({
  selector: 'cd-nvmeof-edit-host-key-modal',
  templateUrl: './nvmeof-edit-host-key-modal.component.html',
  styleUrls: ['./nvmeof-edit-host-key-modal.component.scss']
})
export class NvmeofEditHostKeyModalComponent {
  @Input() hostName: string;
  @Input() dhchapKey: string;

  form: FormGroup;

  constructor(private fb: FormBuilder, private modalService: ModalCdsService) {
    this.form = this.fb.group({
      dhchapKey: ['']
    });
  }

  ngOnInit() {
    this.form.patchValue({ dhchapKey: this.dhchapKey });
  }

  save() {
    // Emit or call API to update key
    this.modalService.dismissAll();
  }

  cancel() {
    this.modalService.dismissAll();
  }
}
