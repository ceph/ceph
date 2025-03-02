import { AfterViewInit, Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { GlobalRateLimitConfig, RgwRateLimitConfig } from '../models/rgw-rate-limit';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { AbstractControl, FormGroup, ValidationErrors, Validators } from '@angular/forms';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import _ from 'lodash';
import { NotificationService } from '~/app/shared/services/notification.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';

@Component({
  selector: 'cd-rgw-rate-limit',
  templateUrl: './rgw-rate-limit.component.html',
  styleUrls: ['./rgw-rate-limit.component.scss']
})
export class RgwRateLimitComponent implements OnInit, AfterViewInit {
  globalRateLimit: GlobalRateLimitConfig['user_ratelimit' | 'bucket_ratelimit'];
  form: CdFormGroup;
  @Input() type: string;

  @Output() formValue = new EventEmitter();
  @Output() rateLimitFormGroup = new EventEmitter<FormGroup>();

  @Input()
  isEditing: boolean;

  @Input() id: string;

  bid: string;
  @Input() set allowBid(value: string) {
    this.bid = value;
    if (this.isEditing && !!this.bid && this.type == 'bucket') {
      this.getRateLimitFormValues();
    }
  }
  constructor(
    private formBuilder: CdFormBuilder,
    private rgwUserService: RgwUserService,
    private rgwBucketService: RgwBucketService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    // get the global rate Limit
    if (this.type === 'user') {
      this.rgwUserService.getGlobalUserRateLimit().subscribe(
        (data: GlobalRateLimitConfig) => {
          if (data && data.user_ratelimit !== undefined) {
            this.globalRateLimit = data.user_ratelimit;
          }
        },
        (error: any) => {
          this.notificationService.show(NotificationType.error, error);
        }
      );
      this.isEditing ? this.getRateLimitFormValues() : '';
    } else {
      this.rgwBucketService.getGlobalBucketRateLimit().subscribe(
        (data: GlobalRateLimitConfig) => {
          if (data && data.bucket_ratelimit !== undefined) {
            this.globalRateLimit = data.bucket_ratelimit;
          }
        },
        (error: any) => {
          this.notificationService.show(NotificationType.error, error);
        }
      );
    }
    // rate limit form
    this.form = this.formBuilder.group({
      rate_limit_enabled: [false],
      rate_limit_max_readOps_unlimited: [true],
      rate_limit_max_readOps: [
        null,
        [
          CdValidators.composeIf(
            {
              rate_limit_enabled: true,
              rate_limit_max_readOps_unlimited: false
            },
            [Validators.required, this.rateLimitIopmMaxSizeValidator]
          )
        ]
      ],
      rate_limit_max_writeOps_unlimited: [true],
      rate_limit_max_writeOps: [
        null,
        [
          CdValidators.composeIf(
            {
              rate_limit_enabled: true,
              rate_limit_max_writeOps_unlimited: false
            },
            [Validators.required, this.rateLimitIopmMaxSizeValidator]
          )
        ]
      ],
      rate_limit_max_readBytes_unlimited: [true],
      rate_limit_max_readBytes: [
        null,
        [
          CdValidators.composeIf(
            {
              rate_limit_enabled: true,
              rate_limit_max_readBytes_unlimited: false
            },
            [Validators.required, this.rateLimitBytesMaxSizeValidator]
          )
        ]
      ],
      rate_limit_max_writeBytes_unlimited: [true],
      rate_limit_max_writeBytes: [
        null,
        [
          CdValidators.composeIf(
            {
              rate_limit_enabled: true,
              rate_limit_max_writeBytes_unlimited: false
            },
            [Validators.required, this.rateLimitBytesMaxSizeValidator]
          )
        ]
      ]
    });
  }
  /**
   * Helper function to populate Form Values
   * when edit rate limit edit is called.
   */
  private populateFormValues(data: RgwRateLimitConfig) {
    this.form.get('rate_limit_enabled').setValue(data.enabled);
    this._setRateLimitProperty(
      'rate_limit_max_readBytes',
      'rate_limit_max_readBytes_unlimited',
      data.max_read_bytes
    );
    this._setRateLimitProperty(
      'rate_limit_max_writeBytes',
      'rate_limit_max_writeBytes_unlimited',
      data.max_write_bytes
    );
    this._setRateLimitProperty(
      'rate_limit_max_readOps',
      'rate_limit_max_readOps_unlimited',
      data.max_read_ops
    );
    this._setRateLimitProperty(
      'rate_limit_max_writeOps',
      'rate_limit_max_writeOps_unlimited',
      data.max_write_ops
    );
  }
  /**
   * Helper function to call api and get Rate Limit Values
   * on load for user and bucket
   */
  private getRateLimitFormValues() {
    if (this.type === 'user') {
      this.rgwUserService.getUserRateLimit(this.id).subscribe(
        (resp: GlobalRateLimitConfig) => {
          this.populateFormValues(resp.user_ratelimit);
        },
        (error: any) => {
          this.notificationService.show(NotificationType.error, error);
        }
      );
    } else {
      this.rgwBucketService.getBucketRateLimit(this.bid).subscribe(
        (resp: GlobalRateLimitConfig) => {
          this.populateFormValues(resp.bucket_ratelimit);
        },
        (error: any) => {
          this.notificationService.show(NotificationType.error, error);
        }
      );
    }
  }
  /**
   * Validate the rate limit bytes maximum size, e.g. 30, 1K, 30 PiB/m or 1.9 MiB/m.
   */
  rateLimitBytesMaxSizeValidator(control: AbstractControl): ValidationErrors | null {
    return new FormatterService().performValidation(
      control,
      '^(\\d+(\\.\\d+)?)\\s*(B/m|K(B|iB/m)?|M(B|iB/m)?|G(B|iB/m)?|T(B|iB/m)?|P(B|iB/m)?)?$',
      { rateByteMaxSize: true }
    );
  }
  /**
   * Validate the rate limit operations maximum size
   */
  rateLimitIopmMaxSizeValidator(control: AbstractControl): ValidationErrors | null {
    return new FormatterService().iopmMaxSizeValidator(control);
  }
  getRateLimitFormValue() {
    if (this._isRateLimitFormDirty()) return this._getRateLimitArgs();
    return null;
  }
  ngAfterViewInit() {
    this.rateLimitFormGroup.emit(this.form);
  }
  /**
   * Check if the user rate limit has been modified.
   * @return {Boolean} Returns TRUE if the user rate limit has been modified.
   */
  private _isRateLimitFormDirty(): boolean {
    return [
      'rate_limit_enabled',
      'rate_limit_max_readOps_unlimited',
      'rate_limit_max_readOps',
      'rate_limit_max_writeOps_unlimited',
      'rate_limit_max_writeOps',
      'rate_limit_max_readBytes_unlimited',
      'rate_limit_max_readBytes',
      'rate_limit_max_writeBytes_unlimited',
      'rate_limit_max_writeBytes'
    ].some((path) => {
      return this.form.get(path).dirty;
    });
  }
  /**
   * Helper function to get the arguments for the API request when the user
   * rate limit configuration has been modified.
   */
  private _getRateLimitArgs(): RgwRateLimitConfig {
    const result: RgwRateLimitConfig = {
      enabled: this.form.getValue('rate_limit_enabled'),
      max_read_ops: 0,
      max_write_ops: 0,
      max_read_bytes: 0,
      max_write_bytes: 0
    };
    if (!this.form.getValue('rate_limit_max_readOps_unlimited')) {
      result['max_read_ops'] = this.form.getValue('rate_limit_max_readOps');
    }
    if (!this.form.getValue('rate_limit_max_writeOps_unlimited')) {
      result['max_write_ops'] = this.form.getValue('rate_limit_max_writeOps');
    }
    if (!this.form.getValue('rate_limit_max_readBytes_unlimited')) {
      // Convert the given value to bytes.
      result['max_read_bytes'] = new FormatterService().toBytes(
        this.form.getValue('rate_limit_max_readBytes')
      );
    }
    if (!this.form.getValue('rate_limit_max_writeBytes_unlimited')) {
      result['max_write_bytes'] = new FormatterService().toBytes(
        this.form.getValue('rate_limit_max_writeBytes')
      );
    }
    return result;
  }

  /**
   * Helper function to map the values for the Rate Limit when the user
   * rate limit gets loaded for first time or edited.
   */

  private _setRateLimitProperty(rateLimitKey: string, unlimitedKey: string, property: number) {
    if (property === 0) {
      this.form.get(unlimitedKey).setValue(true);
      this.form.get(rateLimitKey).setValue('');
    } else {
      this.form.get(unlimitedKey).setValue(false);
      this.form.get(rateLimitKey).setValue(property);
    }
  }
}
