import { Component, OnDestroy, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { NgbActiveModal, NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { Subscription } from 'rxjs';

import { FeedbackService } from '~/app/shared/api/feedback.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-feedback',
  templateUrl: './feedback.component.html',
  styleUrls: ['./feedback.component.scss']
})
export class FeedbackComponent implements OnInit, OnDestroy {
  action: string;
  title = $localize`Feedback`;
  resource = $localize`Issue`;
  project: any = [
    'dashboard',
    'block',
    'objects',
    'file_system',
    'ceph_manager',
    'orchestrator',
    'ceph_volume',
    'core_ceph'
  ];
  tracker: string[] = ['bug', 'feature'];
  api_key: string;
  keySub: Subscription;

  feedbackForm: CdFormGroup;
  isAPIKeySet = false;
  isFeedbackEnabled = true;

  constructor(
    private feedbackService: FeedbackService,
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    public secondaryModal: NgbModal,
    private notificationService: NotificationService,
    private router: Router
  ) {}

  ngOnInit() {
    this.action = this.actionLabels.CREATE;
    this.createForm();

    this.keySub = this.feedbackService.isKeyExist().subscribe({
      next: (data: boolean) => {
        this.isAPIKeySet = data;
        if (this.isAPIKeySet) {
          this.feedbackForm.get('api_key').clearValidators();
        }
      },
      error: () => {
        this.isFeedbackEnabled = false;
        this.feedbackForm.disable();
      }
    });
  }

  private createForm() {
    this.feedbackForm = new CdFormGroup({
      project: new UntypedFormControl('', Validators.required),
      tracker: new UntypedFormControl('', Validators.required),
      subject: new UntypedFormControl('', Validators.required),
      description: new UntypedFormControl('', Validators.required),
      api_key: new UntypedFormControl('', Validators.required)
    });
  }

  ngOnDestroy() {
    this.keySub.unsubscribe();
  }

  onSubmit() {
    this.feedbackService
      .createIssue(
        this.feedbackForm.controls['project'].value,
        this.feedbackForm.controls['tracker'].value,
        this.feedbackForm.controls['subject'].value,
        this.feedbackForm.controls['description'].value,
        this.feedbackForm.controls['api_key'].value
      )
      .subscribe({
        next: (result) => {
          this.notificationService.show(
            NotificationType.success,
            $localize`Issue successfully created on Ceph Issue tracker`,
            `Go to the tracker: <a href="https://tracker.ceph.com/issues/${result['message']['issue']['id']}" target="_blank"> ${result['message']['issue']['id']} </a>`
          );
        },
        error: () => {
          this.feedbackForm.get('api_key').setErrors({ invalidApiKey: true });
          this.feedbackForm.setErrors({ cdSubmitButton: true });
        },
        complete: () => {
          this.activeModal.close();
        }
      });
  }

  redirect() {
    this.activeModal.close();
    this.router.navigate(['/mgr-modules']);
  }
}
