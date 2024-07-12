import { Component, Input } from '@angular/core';
import { Icons } from '../../enum/icons.enum';

@Component({
  selector: 'cd-progress',
  templateUrl: './progress.component.html',
  styleUrls: ['./progress.component.scss']
})
export class ProgressComponent {
  icons = Icons;
  @Input() value: number;
  @Input() label: string;
  @Input() status: string;
  @Input() description: string;
  @Input() subLabel: string;
  @Input() completedItems: string;
  @Input() actionName: string;
  @Input() helperText: string;
  @Input() footerText: string;
  @Input() isPaused: boolean;
}
