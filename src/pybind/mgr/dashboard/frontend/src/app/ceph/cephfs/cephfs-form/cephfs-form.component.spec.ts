import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { CephfsVolumeFormComponent } from './cephfs-form.component';
import { FormHelper, configureTestBed } from '~/testing/unit-test-helper';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { ReactiveFormsModule } from '@angular/forms';
describe('CephfsVolumeFormComponent', () => {
  let component: CephfsVolumeFormComponent;
  let fixture: ComponentFixture<CephfsVolumeFormComponent>;
  let formHelper: FormHelper;

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
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should validate proper names', fakeAsync(() => {
    const validNames = ['test', 'test1234', 'test_1234', 'test-1234', 'test.1234', 'test12test'];
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
});
