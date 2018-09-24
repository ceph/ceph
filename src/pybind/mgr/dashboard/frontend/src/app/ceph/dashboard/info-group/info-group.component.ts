import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-info-group',
  templateUrl: './info-group.component.html',
  styleUrls: ['./info-group.component.scss']
})
export class InfoGroupComponent {
  @Input()
  groupTitle: string;
}
