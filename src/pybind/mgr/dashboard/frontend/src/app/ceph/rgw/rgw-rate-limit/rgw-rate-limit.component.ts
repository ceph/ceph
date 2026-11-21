import {
  AfterViewInit,
  Component,
  EventEmitter,
  Input,
  OnInit,
  Output,
  ViewChild
} from '@angular/core';
import {
  AbstractControl,
  FormGroup,
  NgForm,
  ValidationErrors,
  ValidatorFn,
  Validators
} from '@angular/forms';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { USER } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import {
  RGW_RATE_LIMIT_BANNER_FIELDS,
  RGW_RATE_LIMIT_FIELDS,
  RgwRateLimitField
} from '../models/rgw-rate-limit-fields';
import {
  GlobalRateLimitConfig,
  RgwRateLimitConfig,
  RgwRateLimitScope
} from '../models/rgw-rate-limit';

@Component({
  selector: 'cd-rgw-rate-limit',
  templateUrl: './rgw-rate-limit.component.html',
  styleUrls: ['./rgw-rate-limit.component.scss'],
  standalone: false
})
export class RgwRateLimitComponent implements OnInit, AfterViewInit {
  @Input() type: string;
  @Input() isEditing: boolean;
  @Input() id: string;

  @Output() formValue = new EventEmitter();
  @Output() rateLimitFormGroup = new EventEmitter<FormGroup>();

  @ViewChild('frm')
  formDir?: NgForm;

  globalRateLimit: RgwRateLimitScope;
  form: CdFormGroup;

  /**
   * Operation limits, then byte limits. List and delete are with the other
   * operation limits so both user and bucket forms render the same controls.
   */
  readonly rateLimitFields = RGW_RATE_LIMIT_FIELDS;
  readonly globalBannerFields = RGW_RATE_LIMIT_BANNER_FIELDS;

  bid: string;

  @Input()
  set allowBid(value: string) {
    this.bid = value;
    if (this.isEditing && !!this.bid && this.type === 'bucket') {
      this.getRateLimitFormValues();
    }
  }

  constructor(
    private formBuilder: CdFormBuilder,
    private rgwUserService: RgwUserService,
    private rgwBucketService: RgwBucketService,
    private notificationService: NotificationService,
    private formatter: FormatterService
  ) {
    this.createForm();
  }

  ngOnInit(): void {
    if (this.type === USER) {
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
      if (this.isEditing) {
        this.getRateLimitFormValues();
      }
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
  }

  ngAfterViewInit(): void {
    this.rateLimitFormGroup.emit(this.form);
  }

  getRateLimitFormValue(): RgwRateLimitConfig | null {
    if (this._isRateLimitFormDirty()) {
      return this._getRateLimitArgs();
    }
    return null;
  }

  /**
   * Message for the active threshold input. Ops and byte limits share this so
   * list and delete report the same required / size errors as read and write.
   */
  errorText(field: RgwRateLimitField): string {
    const controlName = field.control;
    if (this.form.showError(controlName, this.formDir, 'required')) {
      return $localize`This field is required.`;
    }
    const sizeError = field.kind === 'ops' ? 'rateOpsMaxSize' : 'rateByteMaxSize';
    if (this.form.showError(controlName, this.formDir, sizeError)) {
      return $localize`The value is not valid.`;
    }
    if (this.form.showError(controlName, this.formDir, 'min')) {
      return $localize`Enter a positive number.`;
    }
    return '';
  }

  /**
   * Validate the rate limit operations maximum size.
   */
  rateLimitIopmMaxSizeValidator(control: AbstractControl): ValidationErrors | null {
    return this.formatter.iopmMaxSizeValidator(control);
  }

  /**
   * Validate the rate limit bytes maximum size, e.g. 30, 1K, 30 PiB/m or 1.9 MiB/m.
   */
  rateLimitBytesMaxSizeValidator(control: AbstractControl): ValidationErrors | null {
    return this.formatter.performValidation(
      control,
      '^(\\d+(\\.\\d+)?)\\s*(B/m|K(B|iB/m)?|M(B|iB/m)?|G(B|iB/m)?|T(B|iB/m)?|P(B|iB/m)?)?$',
      { rateByteMaxSize: true }
    );
  }

  private createForm(): void {
    const controls: Record<string, unknown> = {
      rate_limit_enabled: [false]
    };
    for (const field of this.rateLimitFields) {
      controls[field.unlimitedControl] = [true];
      controls[field.control] = [null, [this.limitValidator(field)]];
    }
    this.form = this.formBuilder.group(controls);
  }

  /**
   * Same conditional validator for every threshold: required and size-checked
   * only while rate limiting is enabled and that threshold is not unlimited.
   */
  private limitValidator(field: RgwRateLimitField): ValidatorFn {
    const sizeValidator =
      field.kind === 'ops'
        ? this.rateLimitIopmMaxSizeValidator.bind(this)
        : this.rateLimitBytesMaxSizeValidator.bind(this);
    return CdValidators.composeIf(
      {
        rate_limit_enabled: true,
        [field.unlimitedControl]: false
      },
      [Validators.required, sizeValidator]
    );
  }

  /**
   * Helper function to populate Form Values
   * when edit rate limit edit is called.
   */
  private populateFormValues(data: RgwRateLimitConfig): void {
    this.form.get('rate_limit_enabled').setValue(data.enabled);
    for (const field of this.rateLimitFields) {
      this._setRateLimitProperty(field.control, field.unlimitedControl, data[field.apiKey] ?? 0);
    }
  }

  /**
   * Helper function to call api and get Rate Limit Values
   * on load for user and bucket
   */
  private getRateLimitFormValues(): void {
    if (this.type === USER) {
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
   * Check if the user rate limit has been modified.
   * @return {Boolean} Returns TRUE if the user rate limit has been modified.
   */
  private _isRateLimitFormDirty(): boolean {
    const paths = [
      'rate_limit_enabled',
      ...this.rateLimitFields.flatMap((field) => [field.unlimitedControl, field.control])
    ];
    return paths.some((path) => this.form.get(path).dirty);
  }

  /**
   * Helper function to get the arguments for the API request when the user
   * rate limit configuration has been modified.
   */
  private _getRateLimitArgs(): RgwRateLimitConfig {
    const result = { enabled: this.form.getValue('rate_limit_enabled') } as RgwRateLimitConfig;
    for (const field of this.rateLimitFields) {
      result[field.apiKey] = 0;
      if (this.form.getValue(field.unlimitedControl)) {
        continue;
      }
      result[field.apiKey] =
        field.kind === 'bytes'
          ? (this.formatter.toBytes(this.form.getValue(field.control)) ?? 0)
          : this.form.getValue(field.control);
    }
    return result;
  }

  /**
   * Helper function to map the values for the Rate Limit when the user
   * rate limit gets loaded for first time or edited.
   */
  private _setRateLimitProperty(
    rateLimitKey: string,
    unlimitedKey: string,
    property: number
  ): void {
    if (property === 0) {
      this.form.get(unlimitedKey).setValue(true);
      this.form.get(rateLimitKey).setValue('');
    } else {
      this.form.get(unlimitedKey).setValue(false);
      this.form.get(rateLimitKey).setValue(property);
    }
  }
}
