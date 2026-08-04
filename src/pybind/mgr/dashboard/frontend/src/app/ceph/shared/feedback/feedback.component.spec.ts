import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { throwError, of as observableOf } from 'rxjs';

import { FeedbackService } from '~/app/shared/api/feedback.service';
import { MgrModuleService } from '~/app/shared/api/mgr-module.service';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { configureTestBed, FormHelper } from '~/testing/unit-test-helper';
import { FeedbackComponent } from './feedback.component';
import { SharedModule } from '~/app/shared/shared.module';
import { SelectModule } from 'carbon-components-angular';

describe('FeedbackComponent', () => {
  let component: FeedbackComponent;
  let fixture: ComponentFixture<FeedbackComponent>;
  let feedbackService: FeedbackService;
  let mgrModuleService: MgrModuleService;
  let formHelper: FormHelper;

  configureTestBed({
    imports: [
      ComponentsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ReactiveFormsModule,
      SharedModule,
      SelectModule
    ],
    declarations: [FeedbackComponent],
    providers: [NgbActiveModal]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(FeedbackComponent);
    component = fixture.componentInstance;
    feedbackService = TestBed.inject(FeedbackService);
    mgrModuleService = TestBed.inject(MgrModuleService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should open the form in a modal', () => {
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cds-modal')).not.toBe(null);
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

  it('should enable feedback module with force', () => {
    spyOn(mgrModuleService, 'updateModuleState');
    spyOn(mgrModuleService.updateCompleted$, 'subscribe').and.callThrough();

    component.enableFeedbackModule();

    expect(mgrModuleService.updateModuleState).toHaveBeenCalledWith(
      'feedback',
      false,
      null,
      null,
      'Enabled Feedback Module',
      false,
      undefined,
      true
    );
    expect(mgrModuleService.updateCompleted$.subscribe).toHaveBeenCalled();
  });

  it('should refresh feedback state after module enablement', () => {
    spyOn(feedbackService, 'isKeyExist').and.returnValues(
      throwError({ status: 404 }),
      observableOf(true)
    );
    spyOn(mgrModuleService, 'updateModuleState');

    component.ngOnInit();
    expect(component.isFeedbackEnabled).toEqual(false);

    component.enableFeedbackModule();
    mgrModuleService.updateCompleted$.next();

    expect(component.isFeedbackEnabled).toEqual(true);
    expect(component.feedbackForm.enabled).toEqual(true);
  });
});
