import { Component } from '@angular/core';

import { Icons } from '../../shared/enum/icons.enum';

@Component({
  selector: 'cd-forbidden',
  templateUrl: './forbidden.component.html',
  styleUrls: ['./forbidden.component.scss']
})
export class ForbiddenComponent {
  icons = Icons;
}
