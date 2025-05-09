import { Component, Inject, OnInit, Optional } from '@angular/core';
import { FormArray, Validators, AbstractControl, FormGroup } from '@angular/forms';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdForm } from '~/app/shared/forms/cd-form';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';
import { EVENT_OPTIONS, S3KEYFILTERVALUE, Topic } from '~/app/shared/models/topic.model';
import { Bucket } from '../models/rgw-bucket';
import { Rule } from '../models/rgw-bucket-lifecycle';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { Router } from '@angular/router';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { NotificationConfig, TopicConfiguration } from '~/app/shared/models/notification-configuration.model';

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

  constructor(
    @Inject('bucket') public bucket: Bucket,
    @Optional() @Inject('selectedNotification') public selectedNotification: Rule,
    @Optional() @Inject('editing') public editing = false,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService,
    private rgwTopicService: RgwTopicService,
    private notificationService: NotificationService,
    private fb: CdFormBuilder,
    private router: Router
  ) {
    super();
  }

  ngOnInit() {
    this.skeyFilterValue = Object.values(S3KEYFILTERVALUE);

    this.rgwTopicService.listTopic().subscribe((topics: Topic[]) => {
      this.topics = topics;
      this.topicArn = topics.map((topic: Topic) => topic.arn);
    });

    this.createNotificationForm();
  }

  createNotificationForm() {
    this.notificationForm = this.fb.group({
      id: [null, [Validators.required]],
      topic: [null, [Validators.required]],
      event: [[], [Validators.required]],
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
    const array = this.getFormArray(arrayName);
    if (array.length > 1) {
      array.removeAt(index);
    }
  }

  showInvalid(field: string): boolean {
    const control: AbstractControl | null = this.notificationForm.get(field);
    return control?.invalid && (control.dirty || control.touched);
  }

  onSubmit() {
    if (!this.notificationForm.valid) {
      this.notificationService.show(NotificationType.error, $localize`Please fill out all required fields.`);
      return;
    }

    const formValue = this.notificationForm.getRawValue();

    const buildRules = (rules: any[]) =>
      rules
        ?.map(item => (item.Name && item.Value ? { Name: item.Name, Value: item.Value } : null))
        .filter(rule => rule !== null) || [];

    const notificationConfiguration: TopicConfiguration = {
      Id: formValue.id,
      Topic: formValue.topic,
      Event: formValue.event,
      Filter: {
        Key: { FilterRules: buildRules(formValue.filter?.s3Key) },
        Metadata: { FilterRules: buildRules(formValue.filter?.s3Metadata) },
        Tags: { FilterRules: buildRules(formValue.filter?.s3Tags) }
      }
    };

    console.log('Payload:', JSON.stringify(notificationConfiguration));

    this.rgwBucketService
      .setNotification(this.bucket.bucket, JSON.stringify(notificationConfiguration), this.bucket.owner)
      .subscribe({
        next: () => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Bucket notification created successfully`
          );
        },
        error: (error: any) => {
          this.notificationService.show(NotificationType.error, error.message || 'An error occurred while creating the notification.');
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
