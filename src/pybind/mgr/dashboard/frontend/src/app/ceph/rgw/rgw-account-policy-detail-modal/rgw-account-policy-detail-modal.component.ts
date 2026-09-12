import { Component, Inject, OnInit, Optional } from '@angular/core';
import { Validators } from '@angular/forms';
import { BaseModal } from 'carbon-components-angular';
import { forkJoin, of } from 'rxjs';
import { catchError } from 'rxjs/operators';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwIamPolicyService } from '~/app/shared/api/rgw-iam-policy.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import {
  DEFAULT_IAM_POLICY_DOCUMENT,
  IamPolicy,
  IamPolicyTag,
  IamPolicyVersion
} from '../models/rgw-iam-policy';

@Component({
  selector: 'cd-rgw-account-policy-detail-modal',
  templateUrl: './rgw-account-policy-detail-modal.component.html',
  styleUrls: ['./rgw-account-policy-detail-modal.component.scss'],
  standalone: false
})
export class RgwAccountPolicyDetailModalComponent extends BaseModal implements OnInit {
  policy: IamPolicy;
  accountId: string;
  policyDocument = '';
  versions: IamPolicyVersion[] = [];
  tags: IamPolicyTag[] = [];
  selectedVersionId = '';
  selectedVersionDocument = '';
  loading = true;

  versionForm: CdFormGroup;
  tagForm: CdFormGroup;
  showVersionForm = false;

  constructor(
    @Optional() @Inject('policy') policy: IamPolicy,
    @Optional() @Inject('accountId') accountId: string,
    private rgwIamPolicyService: RgwIamPolicyService,
    private notificationService: NotificationService,
    public actionLabels: ActionLabelsI18n,
    private formBuilder: CdFormBuilder
  ) {
    super();
    this.policy = policy;
    this.accountId = accountId;
  }

  ngOnInit(): void {
    this.versionForm = this.formBuilder.group({
      policy_doc: [DEFAULT_IAM_POLICY_DOCUMENT, [Validators.required, CdValidators.json()]],
      set_as_default: [true]
    });
    this.tagForm = this.formBuilder.group({
      tag_key: ['', [Validators.required]],
      tag_value: ['', [Validators.required]]
    });
    this.loadPolicyData();
  }

  loadPolicyData(): void {
    this.loading = true;
    forkJoin({
      policy: this.rgwIamPolicyService
        .get(this.accountId, this.policy.Arn)
        .pipe(catchError(() => of(this.policy))),
      versions: this.rgwIamPolicyService
        .listVersions(this.accountId, this.policy.Arn)
        .pipe(catchError(() => of([] as IamPolicyVersion[]))),
      tags: this.rgwIamPolicyService
        .listTags(this.accountId, this.policy.Arn)
        .pipe(catchError(() => of([] as IamPolicyTag[])))
    }).subscribe({
      next: ({ policy, versions, tags }) => {
        this.policy = { ...this.policy, ...policy };
        this.policyDocument = this.formatDocument(policy.PolicyDocument);
        this.versions = versions || [];
        this.tags = tags || [];
        if (this.versions.length > 0 && !this.selectedVersionId) {
          const defaultVersion =
            this.versions.find((version) => this.isDefaultVersion(version)) || this.versions[0];
          this.selectVersion(defaultVersion.VersionId);
        }
        this.loading = false;
      },
      error: () => {
        this.loading = false;
      }
    });
  }

  selectVersion(versionId: string): void {
    this.selectedVersionId = versionId;
    this.rgwIamPolicyService.getVersion(this.accountId, this.policy.Arn, versionId).subscribe({
      next: (version) => {
        this.selectedVersionDocument = this.formatDocument(version.Document || version);
      },
      error: () => {
        this.selectedVersionDocument = '';
      }
    });
  }

  setDefaultVersion(versionId: string): void {
    this.rgwIamPolicyService
      .setDefaultVersion(this.accountId, this.policy.Arn, versionId)
      .subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Default policy version updated`
          );
          this.loadPolicyData();
        }
      });
  }

  deleteVersion(versionId: string): void {
    this.rgwIamPolicyService.deleteVersion(this.accountId, this.policy.Arn, versionId).subscribe({
      next: () => {
        this.notificationService.show(NotificationType.success, $localize`Policy version deleted`);
        this.selectedVersionId = '';
        this.loadPolicyData();
      }
    });
  }

  toggleVersionForm(): void {
    this.showVersionForm = !this.showVersionForm;
  }

  createVersion(): void {
    if (this.versionForm.invalid) {
      return;
    }
    const payload = this.versionForm.getRawValue();
    this.rgwIamPolicyService
      .createVersion(this.accountId, this.policy.Arn, payload.policy_doc, payload.set_as_default)
      .subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Policy version created`
          );
          this.showVersionForm = false;
          this.versionForm.reset({
            policy_doc: DEFAULT_IAM_POLICY_DOCUMENT,
            set_as_default: true
          });
          this.loadPolicyData();
        },
        error: () => {
          this.versionForm.setErrors({ cdSubmitButton: true });
        }
      });
  }

  addTag(): void {
    if (this.tagForm.invalid) {
      return;
    }
    const payload = this.tagForm.getRawValue();
    this.rgwIamPolicyService
      .addTags(this.accountId, this.policy.Arn, [
        { Key: payload.tag_key, Value: payload.tag_value }
      ])
      .subscribe({
        next: () => {
          this.notificationService.show(NotificationType.success, $localize`Tag added`);
          this.tagForm.reset();
          this.loadPolicyData();
        },
        error: () => {
          this.tagForm.setErrors({ cdSubmitButton: true });
        }
      });
  }

  removeTag(tagKey: string): void {
    this.rgwIamPolicyService.removeTags(this.accountId, this.policy.Arn, [tagKey]).subscribe({
      next: () => {
        this.notificationService.show(NotificationType.success, $localize`Tag removed`);
        this.loadPolicyData();
      }
    });
  }

  isDefaultVersion(version: IamPolicyVersion): boolean {
    return version.IsDefaultVersion === true || version.IsDefaultVersion === 'true';
  }

  private formatDocument(document: unknown): string {
    if (document === null || document === undefined || document === '') {
      return '';
    }
    if (typeof document === 'string') {
      try {
        return JSON.stringify(JSON.parse(document), null, 2);
      } catch {
        return document;
      }
    }
    return JSON.stringify(document, null, 2);
  }
}
