import { Component, Inject, OnInit, Optional, ChangeDetectorRef } from '@angular/core';
import {
  FormArray,
  Validators,
  AbstractControl,
  FormGroup,
  ValidationErrors
} from '@angular/forms';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdForm } from '~/app/shared/forms/cd-form';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';
import { Topic } from '~/app/shared/models/topic.model';
import { Bucket } from '../models/rgw-bucket';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { Router } from '@angular/router';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import {
  EventOption,
  NotificationConfig,
  S3KeyFilterValue,
  TopicConfiguration
} from '~/app/shared/models/notification-configuration.model';

@Component({
  selector: 'cd-rgw-notification-form',
  templateUrl: './rgw-notification-form.component.html',
  styleUrls: ['./rgw-notification-form.component.scss']
})
export class RgwNotificationFormComponent extends CdForm implements OnInit {
  notificationForm: CdFormGroup;
  eventOption: ComboBoxItem[] = EventOption;
  skeyFilterValue: string[] = [];
  notificationConfig: NotificationConfig;
  topics: Partial<Topic[]> = [];
  topicArn: string[] = [];
  notification_id: string;
  notificationList: any;
  constructor(
    @Inject('bucket') public bucket: Bucket,
    @Optional() @Inject('selectedNotification') public selectedNotification: TopicConfiguration,
    @Optional() @Inject('editing') public editing = false,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService,
    private rgwTopicService: RgwTopicService,
    private notificationService: NotificationService,
    private fb: CdFormBuilder,
    private router: Router,
    private cdRef: ChangeDetectorRef
  ) {
    super();
  }

  ngOnInit() {
    this.notificationList = this.rgwBucketService
      .getBucketNotificationList(this.bucket.bucket)
      .subscribe({
        next: (notificationList: TopicConfiguration[]) => {
          this.notificationList = notificationList;
        }
      });

    this.skeyFilterValue = Object.values(S3KeyFilterValue);
    this.createNotificationForm();
    this.getTopicName().then(() => {
      if (this.editing && this.selectedNotification) {
        this.notification_id = this.selectedNotification.Id;
        this.notificationForm.get('id').disable();
        this.patchNotificationForm(this.selectedNotification);
      }
    });
  }
  getTopicName(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.rgwTopicService.listTopic().subscribe({
        next: (topics: Topic[]) => {
          this.topics = topics;
          this.topicArn = topics.map((topic: Topic) => topic.arn);
          resolve();
        },
        error: (err) => reject(err)
      });
    });
  }

  patchNotificationForm(config: any): void {
    this.cdRef.detectChanges();
    this.notificationForm.patchValue({
      id: config.Id,
      topic: typeof config.Topic === 'object' ? config.Topic.arn : config.Topic,
      event: config.Event
    });

    this.setFilterRules('s3Key', config.Filter?.S3Key?.FilterRule);
    this.setFilterRules('s3Metadata', config.Filter?.S3Metadata?.FilterRule);
    this.setFilterRules('s3Tags', config.Filter?.S3Tags?.FilterRule);
  }

  setFilterRules(type: string, rules: any[] = []): void {
    const formArray = this.getFormArray(type);
    formArray.clear();

    if (!rules || rules.length === 0) {
      formArray.push(this.createNameValueGroup());
      return;
    }

    rules.forEach((rule) => {
      formArray.push(this.fb.group({ Name: [rule.Name], Value: [rule.Value] }));
    });
  }

  createNotificationForm() {
    this.notificationForm = this.fb.group({
      id: [null, [Validators.required, this.duplicateNotificationId.bind(this)]],
      topic: [null, [Validators.required]],
      event: [[], []],
      filter: this.fb.group({
        s3Key: this.fb.array([this.createNameValueGroup()]),
        s3Metadata: this.fb.array([this.createNameValueGroup()]),
        s3Tags: this.fb.array([this.createNameValueGroup()])
      })
    });
  }

  duplicateNotificationId(control: AbstractControl): ValidationErrors | null {
    const currentId = control.value?.trim();
    if (!currentId) return null;
    if (Array.isArray(this.notificationList)) {
      const duplicateFound = this.notificationList.some(
        (notification: TopicConfiguration) => notification.Id === currentId
      );

      return duplicateFound ? { duplicate: true } : null;
    }

    return null;
  }

  private createNameValueGroup(): CdFormGroup {
    return this.fb.group({
      Name: [null],
      Value: [null]
    });
  }

  get s3KeyFilters(): FormArray {
    return this.getFormArray('s3Key');
  }

  get s3MetadataFilters(): FormArray {
    return this.getFormArray('s3Metadata');
  }

  get s3TagsFilters(): FormArray {
    return this.getFormArray('s3Tags');
  }

  getFormArray(arrayName: string): FormArray {
    const filterGroup = this.notificationForm.controls['filter'] as FormGroup;
    return filterGroup.controls[arrayName] as FormArray;
  }

  addRow(arrayName: string, index: number): void {
    const array = this.getFormArray(arrayName);
    array.insert(index + 1, this.createNameValueGroup());
  }

  removeRow(arrayName: string, index: number): void {
    const formArray = this.getFormArray(arrayName);
    if (formArray && formArray.length > 1 && index >= 0 && index < formArray.length) {
      formArray.removeAt(index);
    } else if (formArray.length === 1) {
      const group = formArray.at(0) as FormGroup;
      group.reset();
    }

    this.cdRef.detectChanges();
  }

  showInvalid(field: string): boolean {
    const control: AbstractControl | null = this.notificationForm.get(field);
    return control?.invalid && (control.dirty || control.touched);
  }

  onSubmit() {
    if (!this.notificationForm.valid) {
      this.notificationForm.markAllAsTouched();
      this.notificationForm.setErrors({ cdSubmitButton: true });
      return;
    }

    const formValue = this.notificationForm.getRawValue();

    const buildRules = (rules: any[]) =>
      rules
        ?.map((item) => (item.Name && item.Value ? { Name: item.Name, Value: item.Value } : null))
        .filter((rule) => rule !== null) || [];
    const isUpdate = !!formValue.id;

    const successMessage = isUpdate
      ? $localize`Bucket notification updated successfully`
      : $localize`Bucket notification created successfully`;

    const notificationConfiguration = {
      TopicConfiguration: {
        Id: formValue.id,
        Topic: formValue.topic,
        Event: formValue.event,
        Filter: {
          S3Key: { FilterRules: buildRules(formValue.filter?.s3Key) },
          S3Metadata: { FilterRules: buildRules(formValue.filter?.s3Metadata) },
          S3Tags: { FilterRules: buildRules(formValue.filter?.s3Tags) }
        }
      }
    };

    this.rgwBucketService
      .setNotification(
        this.bucket.bucket,
        JSON.stringify(notificationConfiguration),
        this.bucket.owner
      )
      .subscribe({
        next: () => {
          this.notificationService.show(NotificationType.success, successMessage);
        },
        error: (error: any) => {
          this.notificationService.show(
            NotificationType.error,
            error.message || 'An error occurred while creating the notification.'
          );
          this.notificationForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.closeModal();
        }
      });
  }

  goToCreateNotification() {
    this.router.navigate(['rgw/notification/create']);
    this.closeModal();
  }
}
