import { Location } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { BaseModal } from 'carbon-components-angular';

@Component({
  selector: 'cd-modal',
  templateUrl: './modal.component.html',
  styleUrls: ['./modal.component.scss']
})
export class ModalComponent extends BaseModal {
  @Input()
  open = true;

  @Input()
  size: "sm" | "md" | "lg" = "md";

  @Input()
  hasScrollingContent = true;

  @Input()
  title = '';

  @Input()
  titleHelperText = '';

  @Input()
  formAllFieldsRequired = false;

  /**
   * Should be a function that is triggered when the modal is hidden.
   */
  @Output()
  hide = new EventEmitter();

  constructor(
    private route: ActivatedRoute,
    private location: Location
  ) {
    super();
    this.open = this.route.outlet === 'modal' || true;
  }

  closeModal() {
    if (this.route.outlet === 'modal') this.location.back();
    else this.open = false;
  }
}
