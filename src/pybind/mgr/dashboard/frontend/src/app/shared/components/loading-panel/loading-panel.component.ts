import { Component } from '@angular/core';

import { Icons } from '../../../shared/enum/icons.enum';

@Component({
  selector: 'cd-loading-panel',
  templateUrl: './loading-panel.component.html',
  styleUrls: ['./loading-panel.component.scss']
})
export class LoadingPanelComponent {
  icons = Icons;
}
