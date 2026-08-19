import { Component } from '@angular/core';

import { Icons } from '~/app/shared/enum/icons.enum';

@Component({
  selector: 'cd-loading-panel',
  templateUrl: './loading-panel.component.html',
  styleUrls: ['./loading-panel.component.scss'],
  standalone: false
})
export class LoadingPanelComponent {
  icons = Icons;
}
