import { Component, Input } from '@angular/core';
import { RGW_RATE_LIMIT_FIELDS } from '../models/rgw-rate-limit-fields';
import {
  isRgwRateLimitUnlimited,
  RgwRateLimitConfig,
  RgwRateLimitNumericKey
} from '../models/rgw-rate-limit';

@Component({
  selector: 'cd-rgw-rate-limit-details',
  templateUrl: './rgw-rate-limit-details.component.html',
  styleUrls: ['./rgw-rate-limit-details.component.scss'],
  standalone: false
})
export class RgwRateLimitDetailsComponent {
  @Input() rateLimitConfig: RgwRateLimitConfig;
  @Input() type!: string;
  @Input() showHeading = true;

  /** Same field order as the form: read, write, list, delete, then byte limits. */
  readonly limitFields = RGW_RATE_LIMIT_FIELDS;

  isUnlimited(key: RgwRateLimitNumericKey): boolean {
    return isRgwRateLimitUnlimited(this.rateLimitConfig?.[key]);
  }
}
