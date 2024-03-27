import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { CephfsVolumeFormComponent } from './cephfs-form.component';
import { FormHelper, configureTestBed } from '~/testing/unit-test-helper';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { ReactiveFormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { of } from 'rxjs';

describe('CephfsVolumeFormComponent', () => {
  let component: CephfsVolumeFormComponent;
  let fixture: ComponentFixture<CephfsVolumeFormComponent>;
  let formHelper: FormHelper;
  let orchService: OrchestratorService;

  configureTestBed({
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ReactiveFormsModule,
      ToastrModule.forRoot()
    ],
    declarations: [CephfsVolumeFormComponent]
  });
  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsVolumeFormComponent);
    component = fixture.componentInstance;
    formHelper = new FormHelper(component.form);
    orchService = TestBed.inject(OrchestratorService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true }));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should validate proper names', fakeAsync(() => {
    const validNames = [
      'test',
      'test1234',
      'test_1234',
      'test-1234',
      'test.1234',
      'test12test',
      '.test'
    ];
    const invalidNames = ['1234', 'test@', 'test)'];

    for (const validName of validNames) {
      formHelper.setValue('name', validName, true);
      tick();
      formHelper.expectValid('name');
    }

    for (const invalidName of invalidNames) {
      formHelper.setValue('name', invalidName, true);
      tick();
      formHelper.expectError('name', 'pattern');
    }
  }));

  it('should show placement when orchestrator is available', () => {
    const placement = fixture.debugElement.query(By.css('#placement'));
    expect(placement).not.toBeNull();
  });

  describe('when editing', () => {
    beforeEach(() => {
      component.editing = true;
      component.ngOnInit();
      fixture.detectChanges();
    });

    it('should not show placement while editing even if orch is available', () => {
      const placement = fixture.debugElement.query(By.css('#placement'));
      const label = fixture.debugElement.query(By.css('#label'));
      const hosts = fixture.debugElement.query(By.css('#hosts'));
      expect(placement).toBeNull();
      expect(label).toBeNull();
      expect(hosts).toBeNull();
    });

    it('should disable renaming and show info alert if disableRename is true', () => {
      component.disableRename = true;
      component.ngOnInit();
      fixture.detectChanges();
      const alertPanel = fixture.debugElement.query(By.css('cd-alert-panel'));
      expect(alertPanel).not.toBeNull();
    });

    it('should not show the alert if disableRename is false', () => {
      component.disableRename = false;
      component.ngOnInit();
      fixture.detectChanges();
      const alertPanel = fixture.debugElement.query(By.css('cd-alert-panel'));
      expect(alertPanel).toBeNull();
    });

    it('should disable the submit button only if disableRename is true', () => {
      component.disableRename = true;
      component.ngOnInit();
      fixture.detectChanges();
      const submitButton = fixture.debugElement.query(By.css('button[type=submit]'));
      expect(submitButton.nativeElement.disabled).toBeTruthy();

      // the submit button should only be disabled when the form is in edit mode
      component.editing = false;
      component.ngOnInit();
      fixture.detectChanges();
      expect(submitButton.nativeElement.disabled).toBeFalsy();

      // submit button should be enabled if disableRename is false
      component.editing = true;
      component.disableRename = false;
      component.ngOnInit();
      fixture.detectChanges();
      expect(submitButton.nativeElement.disabled).toBeFalsy();
    });
  });
});
