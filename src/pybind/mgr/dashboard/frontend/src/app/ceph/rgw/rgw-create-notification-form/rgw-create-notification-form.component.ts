import { Component, Inject, OnInit, Optional, ChangeDetectorRef } from '@angular/core';
import { FormArray, Validators, AbstractControl, FormGroup } from '@angular/forms';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdForm } from '~/app/shared/forms/cd-form';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';
import { EVENT_OPTIONS, S3KEYFILTERVALUE, Topic } from '~/app/shared/models/topic.model';
import { Bucket } from '../models/rgw-bucket';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { Router } from '@angular/router';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import {
  NotificationConfig,
  TopicConfiguration
} from '~/app/shared/models/notification-configuration.model';

@Component({
  selector: 'cd-rgw-create-notification-form',
  templateUrl: './rgw-create-notification-form.component.html',
  styleUrls: ['./rgw-create-notification-form.component.scss']
})
export class RgwCreateNotificationFormComponent extends CdForm implements OnInit {
  notificationForm: CdFormGroup;
  eventOption: ComboBoxItem[] = EVENT_OPTIONS;
  skeyFilterValue: string[] = [];
  notificationConfig: NotificationConfig;
  topics: Partial<Topic[]> = [];
  topicArn: string[] = [];
  notification_id: string;
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
    this.skeyFilterValue = Object.values(S3KEYFILTERVALUE);
    this.createNotificationForm();
    this.getTopicName().then(() => {
      if (this.editing && this.selectedNotification) {
        this.notification_id = this.selectedNotification.Id;
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
      id: [null, [Validators.required]],
      topic: [null, [Validators.required]],
      event: [[], []],
      filter: this.fb.group({
        s3Key: this.fb.array([this.createNameValueGroup()]),
        s3Metadata: this.fb.array([this.createNameValueGroup()]),
        s3Tags: this.fb.array([this.createNameValueGroup()])
      })
    });
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

    const isUpdate = !!formValue.id;
    const successMessage = isUpdate
      ? $localize`Bucket notification ${this.notification_id} updated successfully`
      : $localize`Bucket notification created successfully`;

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
