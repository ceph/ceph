import { animate, state, style, transition, trigger } from '@angular/animations';
import { Component } from '@angular/core';

import { Toast, ToastPackage, ToastrService } from 'ngx-toastr';
import { Icons } from '../../enum/icons.enum';
import { CdIndividualConfig } from '../../models/cd-notification';

@Component({
  selector: 'cd-toast',
  templateUrl: './toast.component.html',
  styleUrls: ['./toast.component.scss'],
  animations: [
    // https://github.com/scttcper/ngx-toastr/issues/552
    trigger('flyInOut', [
      state('inactive', style({ opacity: 0 })),
      state('active', style({ opacity: 1 })),
      state('removed', style({ opacity: 0 })),
      transition('inactive => active', animate('{{ easeTime }}ms {{ easing }}')),
      transition('active => removed', animate('{{ easeTime }}ms {{ easing }}'))
    ])
  ]
})
export class ToastComponent extends Toast {
  public static textClasses = ['text-danger', 'text-info', 'text-success'];
  public static iconClasses = [Icons.warning, Icons.info, Icons.check];

  public textClass: string;
  public iconClass: string;
  public config: CdIndividualConfig;
  public errorCode: number;

  constructor(protected toastrService: ToastrService, public toastPackage: ToastPackage) {
    super(toastrService, toastPackage);
    this.config = toastPackage.config as CdIndividualConfig;
    this.iconClass = ToastComponent.iconClasses[this.config.type];
    this.textClass = ToastComponent.textClasses[this.config.type];
    this.errorCode = this.config.errorCode;

    if (this.errorCode) {
      this.message =
        this.message +
        `<br />
      <small class="float-right">Error code: ` +
        this.errorCode +
        `</small>`;
    }
  }
}
