import { Location } from '@angular/common';
import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { UntypedFormGroup, NgForm } from '@angular/forms';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { ModalService } from '~/app/shared/services/modal.service';
import { SubmitButtonComponent } from '../submit-button/submit-button.component';
import { ModalCdsService } from '../../services/modal-cds.service';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'cd-form-button-panel',
  templateUrl: './form-button-panel.component.html',
  styleUrls: ['./form-button-panel.component.scss']
})
export class FormButtonPanelComponent implements OnInit {
  @ViewChild(SubmitButtonComponent)
  submitButton: SubmitButtonComponent;

  @Output()
  submitActionEvent = new EventEmitter();
  @Output()
  backActionEvent = new EventEmitter();

  @Input()
  form: UntypedFormGroup | NgForm;
  @Input()
  showSubmit = true;
  @Input()
  showCancel = true;
  @Input()
  wrappingClass = '';
  @Input()
  btnClass = '';
  @Input()
  submitText?: string;
  @Input()
  cancelText?: string;
  @Input()
  disabled = false;
  @Input()
  modalForm = false;
  @Input()
  submitBtnType: 'primary' | 'danger';

  hasModalOutlet = false;

  constructor(
    private location: Location,
    private actionLabels: ActionLabelsI18n,
    private modalService: ModalService,
    private cdsModalService: ModalCdsService,
    private route: ActivatedRoute
  ) {}

  ngOnInit() {
    this.submitText = this.submitText || this.actionLabels.CREATE;
    this.cancelText = this.cancelText || this.actionLabels.CANCEL;
    this.hasModalOutlet = this.route.outlet === 'modal';
  }

  submitAction() {
    this.submitActionEvent.emit();
  }

  backAction() {
    if (this.backActionEvent.observers.length === 0) {
      if (this.modalForm && this.cdsModalService.hasOpenModals()) {
        this.cdsModalService.dismissAll();
      } else if (this.modalForm && this.hasModalOutlet) {
        this.location.back();
      } else if (this.modalService.hasOpenModals()) {
        this.modalService.dismissAll();
      } else {
        this.location.back();
      }
    } else {
      this.backActionEvent.emit();
    }
  }
}
