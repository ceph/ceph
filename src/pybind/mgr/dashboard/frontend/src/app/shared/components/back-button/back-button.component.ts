import { Location } from '@angular/common';
import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';

@Component({
  selector: 'cd-back-button',
  templateUrl: './back-button.component.html',
  styleUrls: ['./back-button.component.scss']
})
export class BackButtonComponent implements OnInit {
  @Output() backAction = new EventEmitter();
  @Input() name?: string;
  @Input() disabled = false;
  @Input() modalForm = false;
  @Input() showSubmit = false;

  hasModalOutlet = false;

  constructor(
    private location: Location,
    private actionLabels: ActionLabelsI18n,
    private route: ActivatedRoute
  ) {}

  ngOnInit(): void {
    this.name = this.name || this.actionLabels.CANCEL;
    this.hasModalOutlet = this.route.outlet === 'modal';
  }

  back() {
    if (!this.disabled) {
      if (this.backAction.observers.length === 0 || this.hasModalOutlet) {
        this.location.back();
      } else {
        this.backAction.emit();
      }
    }
  }
}
