import { Component, Inject, OnInit, Optional } from '@angular/core';
import { FormArray, Validators, AbstractControl } from '@angular/forms';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdForm } from '~/app/shared/forms/cd-form';
import { ComboBoxItem } from '~/app/shared/models/combo-box.model';
import { EVENT_OPTIONS, S3KEYFILTERVALUE } from '~/app/shared/models/topic.model';
import { Bucket } from '../models/rgw-bucket';
import { Rule } from '../models/rgw-bucket-lifecycle';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { Router } from '@angular/router';

@Component({
  selector: 'cd-rgw-create-notification-form',
  templateUrl: './rgw-create-notification-form.component.html',
  styleUrls: ['./rgw-create-notification-form.component.scss']
})
export class RgwCreateNotificationFormComponent extends CdForm implements OnInit {
  notificationForm: CdFormGroup;
  eventOption: ComboBoxItem[] = EVENT_OPTIONS;
  skeyFilterValue: string[] = [];

  // Define supported event types
  validEventTypes: string[] = [
    's3:ObjectCreated:*',
    's3:ObjectRemoved:*',
    's3:ObjectRestore:*'
  ];

  constructor(
    @Inject('bucket') public bucket: Bucket,
    @Optional() @Inject('selectedNotification') public selectedNotification: Rule,
    @Optional() @Inject('editing') public editing = false,
    public actionLabels: ActionLabelsI18n,
    private rgwBucketService: RgwBucketService,
    private notificationService: NotificationService,
    private fb: CdFormBuilder,
    private router: Router
  ) {
    super();
  }

  ngOnInit() {
    this.skeyFilterValue = Object.values(S3KEYFILTERVALUE);
    this.createNotificationForm();
  }

  createNotificationForm() {
    this.notificationForm = this.fb.group({
      id: [null, [Validators.required]],
      topic: [null, [Validators.required]],
      event: [[], [Validators.required]],
      filter: this.fb.group({
        s3Key: this.fb.array([
          this.createNameValueGroup()
        ]),
        s3Metadata: this.fb.array([
          this.createNameValueGroup()
        ]),
        s3Tags: this.fb.array([
          this.createNameValueGroup()
        ])
      })
    });
  }

  private createNameValueGroup(): CdFormGroup {
    return this.fb.group({
      Name: [null, [Validators.required]],
      Value: [null, [Validators.required]]
    });
  }

  get s3KeyFilters(): FormArray {
    return this.notificationForm.get('filter.s3Key') as FormArray;
  }

  get s3MetadataFilters(): FormArray {
    return this.notificationForm.get('filter.s3Metadata') as FormArray;
  }

  get s3TagsFilters(): FormArray {
    return this.notificationForm.get('filter.s3Tags') as FormArray;
  }

  private getFormArray(arrayName: string): FormArray {
    return this.notificationForm.get(`filter.${arrayName}`) as FormArray;
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

  onSubmit(): void {
    if (this.notificationForm.valid) {
      const data = this.notificationForm.getRawValue();
      const cleanFilterRules = (rules: any[]) =>
        rules.filter(rule => rule?.Name && rule?.Value);
      const keyFilters = cleanFilterRules(data.filter.s3Key);
      const metadataFilters = cleanFilterRules(data.filter.s3Metadata);
      const tagFilters = cleanFilterRules(data.filter.s3Tags);
      const invalidEvents = data.event.filter((event: string) => !this.validEventTypes.includes(event));
      if (invalidEvents.length > 0) {
        this.notificationService.show(
          NotificationType.error,
          `Invalid event types: ${invalidEvents.join(', ')}`
        );
        return;
      }
      const topicXml = `
        <TopicConfiguration>
          <Id>${data.id}</Id>
          <Topic>${data.topic}</Topic>
          ${data.event.map((e: string) => `<Event>${e}</Event>`).join('')}
          <Filter>
            ${keyFilters.length ? `
            <S3Key>
              ${keyFilters.map(rule => `
              <FilterRule>
                <Name>${rule.Name}</Name>
                <Value>${rule.Value}</Value>
              </FilterRule>
              `).join('')}
            </S3Key>` : ''}
            ${metadataFilters.length ? `
            <S3Metadata>
              ${metadataFilters.map(rule => `
              <FilterRule>
                <Name>${rule.Name}</Name>
                <Value>${rule.Value}</Value>
              </FilterRule>
              `).join('')}
            </S3Metadata>` : ''}
            ${tagFilters.length ? `
            <S3Tags>
              ${tagFilters.map(rule => `
              <FilterRule>
                <Name>${rule.Name}</Name>
                <Value>${rule.Value}</Value>
              </FilterRule>
              `).join('')}
            </S3Tags>` : ''}
          </Filter>
        </TopicConfiguration>
      `;
      const xmlPayload = `
        <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          ${topicXml}
        </NotificationConfiguration>
      `;
      this.rgwBucketService
        .setNotification(
          this.bucket.bucket,
          xmlPayload, 
          this.bucket.owner
        )
        .subscribe({
          next: () => {
            this.notificationService.show(
              NotificationType.success,
              $localize`Notification lifecycle created successfully`
            );
          },
          error: (error: any) => {
            this.notificationService.show(NotificationType.error, error?.error || 'Unknown error');
            this.notificationForm.setErrors({ cdSubmitButton: true });
          },
          complete: () => {
            this.closeModal();
          }
        });
  
      console.log('XML Payload:\n', xmlPayload); 
    }
  }
  
  goToCreateNotification() {
    this.router.navigate(['rgw/notification/create']);
    this.closeModal();
  }
}
