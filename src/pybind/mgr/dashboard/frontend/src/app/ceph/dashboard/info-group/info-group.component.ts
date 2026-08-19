import { Component, Input } from '@angular/core';

import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-info-group',
  templateUrl: './info-group.component.html',
  styleUrls: ['./info-group.component.scss'],
  standalone: false
})
export class InfoGroupComponent {
  icons = Icons;
  @Input()
  groupTitle: string;
}
