import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { throwError } from 'rxjs';

import { FeedbackService } from '~/app/shared/api/feedback.service';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { FeedbackComponent } from './feedback.component';

describe('FeedbackComponent', () => {
  let component: FeedbackComponent;
  let fixture: ComponentFixture<FeedbackComponent>;
  let feedbackService: FeedbackService;
  let formHelper: FormHelper;

  configureTestBed({
    imports: [
      ComponentsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ToastrModule.forRoot()
    ],
    declarations: [FeedbackComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FeedbackComponent);
    component = fixture.componentInstance;
    feedbackService = TestBed.inject(FeedbackService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should open the form in a modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cd-modal')).not.toBe(null);
  });

  it('should redirect to mgr-modules if feedback module is not enabled', () => {
    spyOn(feedbackService, 'isKeyExist').and.returnValue(throwError({ status: 400 }));

    component.ngOnInit();

    expect(component.isFeedbackEnabled).toEqual(false);
    expect(component.feedbackForm.disabled).toBeTruthy();
  });

  it('should test invalid api-key', () => {
    component.ngOnInit();
    formHelper = new FormHelper(component.feedbackForm);

    spyOn(feedbackService, 'createIssue').and.returnValue(throwError({ status: 400 }));

    formHelper.setValue('api_key', 'invalidkey');
    formHelper.setValue('project', 'dashboard');
    formHelper.setValue('tracker', 'bug');
    formHelper.setValue('subject', 'foo');
    formHelper.setValue('description', 'foo');
    component.onSubmit();

    formHelper.expectError('api_key', 'invalidApiKey');
  });
});
