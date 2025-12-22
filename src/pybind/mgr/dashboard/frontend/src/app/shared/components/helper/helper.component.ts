import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-helper',
  templateUrl: './helper.component.html',
  styleUrls: ['./helper.component.scss'],
  standalone: false
})
export class HelperComponent {
  @Input()
  class: string;

  @Input()
  iconClass = '';

  @Input()
  html: any;
}
