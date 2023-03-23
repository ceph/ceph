import { ActivatedRoute, Router } from '@angular/router';
import { Component, EventEmitter, Input, Output } from '@angular/core';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-back-button',
  templateUrl: './back-button.component.html',
  styleUrls: ['./back-button.component.scss']
})
export class BackButtonComponent {
  @Output() backAction = new EventEmitter();
  @Input() name: string = this.actionLabels.CANCEL;
  
   

  constructor(private router: Router, private route: ActivatedRoute, private actionLabels: ActionLabelsI18n) {}

  back() {
    if(this.backAction.observers.length === 0){
      this.router.navigate(['../'], {relativeTo: this.route})
    } else {
      this.backAction.emit();
    }
     
  }
}
