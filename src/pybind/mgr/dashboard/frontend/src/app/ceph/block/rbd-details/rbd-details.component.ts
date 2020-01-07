import { Component, Input, TemplateRef, ViewChild } from '@angular/core';

import { RbdFormModel } from '../rbd-form/rbd-form.model';

@Component({
  selector: 'cd-rbd-details',
  templateUrl: './rbd-details.component.html',
  styleUrls: ['./rbd-details.component.scss']
})
export class RbdDetailsComponent {
  @Input()
  selection: RbdFormModel;
  @Input()
  images: any;
  @ViewChild('poolConfigurationSourceTpl', { static: true })
  poolConfigurationSourceTpl: TemplateRef<any>;

  constructor() {}
}
