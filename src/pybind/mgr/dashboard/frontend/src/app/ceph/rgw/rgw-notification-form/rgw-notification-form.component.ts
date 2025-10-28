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
  events,
  FilterRules,
  s3KeyFilter,
  s3KeyFilterTexts,
  s3MetadataFilterTexts,
  s3TagsFilterTexts,
  TopicConfiguration
} from '~/app/shared/models/notification-configuration.model';

@Component({
  selector: 'cd-rgw-notification-form',
  templateUrl: './rgw-notification-form.component.html',
  styleUrls: ['./rgw-notification-form.component.scss']
})
export class RgwNotificationFormComponent extends CdForm implements OnInit {
  notificationForm: CdFormGroup;
  eventOption: ComboBoxItem[] = events;
  s3KeyFilterValue: string[] = [];
  topics: Partial<Topic[]> = [];
  topicArn: string[] = [];
  notification_id: string;
  notificationList: TopicConfiguration[] = [];
  filterTypes: string[] = ['s3Key', 's3Metadata', 's3Tags'];
  typeLabels: Record<string, string> = {
    s3Key: 'S3 Key configuration',
    s3Metadata: 'S3 Metadata configuration',
    s3Tags: 'S3 Tags configuration'
  };

  filterSettings: Record<
    string,
    {
      options: string[] | null;
      isDropdown: boolean;
      namePlaceholder: string;
      valuePlaceholder: string;
      nameHelper: string;
      valueHelper: string;
    }
  > = {
    s3Key: {
      options: null,
      isDropdown: true,
      namePlaceholder: s3KeyFilterTexts.namePlaceholder,
      valuePlaceholder: s3KeyFilterTexts.valuePlaceholder,
      nameHelper: s3KeyFilterTexts.nameHelper,
      valueHelper: s3KeyFilterTexts.valueHelper
    },
    s3Metadata: {
      options: null,
      isDropdown: false,
      namePlaceholder: s3MetadataFilterTexts.namePlaceholder,
      valuePlaceholder: s3MetadataFilterTexts.valuePlaceholder,
      nameHelper: s3MetadataFilterTexts.nameHelper,
      valueHelper: s3MetadataFilterTexts.valueHelper
    },
    s3Tags: {
      options: null,
      isDropdown: false,
      namePlaceholder: s3TagsFilterTexts.namePlaceholder,
      valuePlaceholder: s3TagsFilterTexts.valuePlaceholder,
      nameHelper: s3TagsFilterTexts.nameHelper,
      valueHelper: s3TagsFilterTexts.valueHelper
    }
  };

  get filterControls(): Record<string, FormArray> {
    const controls: Record<string, FormArray> = {};
    this.filterTypes.forEach((type) => {
      controls[type] = this.getFormArray(type);
    });
    return controls;
  }

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
    this.s3KeyFilterValue = Object.values(s3KeyFilter);
    this.filterSettings.s3Key.options = this.s3KeyFilterValue;
    this.createNotificationForm();
    this.rgwBucketService.listNotification(this.bucket.bucket).subscribe({
      next: (notificationList: TopicConfiguration[]) => {
        this.notificationList = notificationList;

        this.getTopicName().then(() => {
          if (this.editing && this.selectedNotification) {
            this.notification_id = this.selectedNotification.Id;
            this.notificationForm.get('id').disable();
            this.patchNotificationForm(this.selectedNotification);
          }
        });
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

  setFilterRules(type: string, rules: FilterRules[] = []): void {
    let formArray = this.getFormArray(type);
    if (!formArray) {
      const filterGroup = this.notificationForm.get('filter') as FormGroup;
      filterGroup.setControl(type, this.fb.array([]));
      formArray = this.getFormArray(type);
    }

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

  getFormArray(arrayName: string): FormArray {
    const filterGroup = this.notificationForm.get('filter') as FormGroup;
    return filterGroup?.get(arrayName) as FormArray;
  }

  getFiltersControls(type: string): FormArray {
    return this.getFormArray(type);
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
    const buildRules = (rules: FilterRules[]) => {
      const seen = new Set<string>();
      return (
        rules
          ?.filter((item) => item.Name && item.Value)
          .filter((item) => {
            if (seen.has(item.Name)) return false;
            seen.add(item.Name);
            return true;
          }) || []
      );
    };

    const successMessage = this.editing
      ? $localize`Bucket notification updated successfully`
      : $localize`Bucket notification created successfully`;

    const notificationConfiguration = {
      TopicConfiguration: {
        Id: formValue.id,
        Topic: formValue.topic,
        Event: formValue.event,
        Filter: {
          S3Key: {
            FilterRules: buildRules(formValue.filter?.s3Key)
          },
          S3Metadata: {
            FilterRules: buildRules(formValue.filter?.s3Metadata)
          },
          S3Tags: {
            FilterRules: buildRules(formValue.filter?.s3Tags)
          }
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
          this.notificationService.show(NotificationType.error, error.message);
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
