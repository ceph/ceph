import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';
import { CephfsVolumeFormComponent } from './cephfs-form.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { ReactiveFormsModule } from '@angular/forms';
describe('CephfsVolumeFormComponent', () => {
  let component: CephfsVolumeFormComponent;
  let fixture: ComponentFixture<CephfsVolumeFormComponent>;
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
    fixture.detectChanges();
  });
  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
