import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { of, throwError } from 'rxjs';

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
  let formHelper: FormHelper;

  configureTestBed({
    imports: [
      ComponentsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ToastrModule.forRoot(),
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
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should open the form in a modal', () => {
    fixture.detectChanges();
    const nativeEl = fixture.debugElement.nativeElement;
    expect(nativeEl.querySelector('cds-modal')).not.toBe(null);
  });

  it('should redirect to mgr-modules if feedback module is not enabled', () => {
    spyOn(feedbackService, 'isKeyExist').and.returnValue(throwError(() => ({ status: 400 })));

    fixture.detectChanges();

    expect(component.isFeedbackEnabled).toEqual(false);
    expect(component.feedbackForm.disabled).toBeTruthy();
  });

  it('should refresh feedback state after enabling the module', () => {
    const mgrModuleService = TestBed.inject(MgrModuleService);
    spyOn(feedbackService, 'isKeyExist').and.returnValues(
      throwError(() => ({ status: 400 })),
      of(false)
    );
    spyOn(mgrModuleService, 'updateModuleState').and.callFake(() => {
      mgrModuleService.updateCompleted$.next();
    });

    fixture.detectChanges();
    expect(component.isFeedbackEnabled).toBe(false);

    component.enableFeedbackModule();

    expect(component.isFeedbackEnabled).toBe(true);
    expect(component.feedbackForm.enabled).toBe(true);
    expect(component.isAPIKeySet).toBe(false);
  });

  it('should always refresh feedback state after module updates complete', () => {
    const mgrModuleService = TestBed.inject(MgrModuleService);
    const refreshSpy = spyOn<any>(component, 'refreshFeedbackModuleState').and.callThrough();
    spyOn(feedbackService, 'isKeyExist').and.returnValue(of(true));

    fixture.detectChanges();
    refreshSpy.calls.reset();

    mgrModuleService.updateCompleted$.next();

    expect(refreshSpy).toHaveBeenCalledTimes(1);
  });

  it('should retry feedback state refresh after module update until backend is ready', fakeAsync(() => {
    const mgrModuleService = TestBed.inject(MgrModuleService);
    spyOn(feedbackService, 'isKeyExist').and.returnValues(
      throwError(() => ({ status: 400 })), // initial ngOnInit check
      throwError(() => ({ status: 400 })), // first refresh after updateCompleted$
      of(false) // retry succeeds
    );

    fixture.detectChanges();
    expect(component.isFeedbackEnabled).toBe(false);

    mgrModuleService.updateCompleted$.next();
    tick(1000);

    expect(component.isFeedbackEnabled).toBe(true);
    expect(component.feedbackForm.enabled).toBe(true);
    expect(component.isAPIKeySet).toBe(false);
  }));

  it('should stop retry state after all module-refresh attempts fail', fakeAsync(() => {
    const mgrModuleService = TestBed.inject(MgrModuleService);
    spyOn(feedbackService, 'isKeyExist').and.returnValues(
      throwError(() => ({ status: 400 })), // initial ngOnInit check
      throwError(() => ({ status: 400 })), // first refresh after updateCompleted$
      throwError(() => ({ status: 400 })), // retry 1
      throwError(() => ({ status: 400 })), // retry 2
      throwError(() => ({ status: 400 })), // retry 3
      throwError(() => ({ status: 400 })), // retry 4
      throwError(() => ({ status: 400 })) // retry 5
    );

    fixture.detectChanges();
    mgrModuleService.updateCompleted$.next();

    tick(5000);

    expect(component.pendingModuleRefreshRetries).toBe(0);
    expect(component.moduleRefreshTimeoutId).toBeNull();
    expect(component.isFeedbackEnabled).toBe(false);
    expect(component.feedbackForm.disabled).toBe(true);
  }));

  it('should test invalid api-key', () => {
    fixture.detectChanges();
    formHelper = new FormHelper(component.feedbackForm);

    spyOn(feedbackService, 'createIssue').and.returnValue(throwError(() => ({ status: 400 })));

    formHelper.setValue('api_key', 'invalidkey');
    formHelper.setValue('project', 'dashboard');
    formHelper.setValue('tracker', 'bug');
    formHelper.setValue('subject', 'foo');
    formHelper.setValue('description', 'foo');
    component.onSubmit();

    formHelper.expectError('api_key', 'invalidApiKey');
  });
});
