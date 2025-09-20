import { Component, Input } from '@angular/core';
import { RgwRateLimitConfig } from '../models/rgw-rate-limit';

@Component({
  selector: 'cd-rgw-rate-limit-details',
  templateUrl: './rgw-rate-limit-details.component.html',
  styleUrls: ['./rgw-rate-limit-details.component.scss']
})
export class RgwRateLimitDetailsComponent {
  @Input() rateLimitConfig: RgwRateLimitConfig;
  @Input() type: string;
}
