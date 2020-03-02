import { ChangeDetectionStrategy, Component, Input } from '@angular/core';
import { Observable } from 'rxjs';

export interface IBadgeOnChange {
  class?: string | string[] | Set<string> | { [klass: string]: any };
  style?: { [klass: string]: any };
  label?: any;
  hidden?: any;
}

export interface IBadgeConfig {
  onChange: Observable<IBadgeOnChange>;
}

@Component({
  selector: 'cd-badge',
  templateUrl: './badge.component.html',
  styleUrls: ['./badge.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class BadgeComponent {
  @Input() config: IBadgeConfig;
}
