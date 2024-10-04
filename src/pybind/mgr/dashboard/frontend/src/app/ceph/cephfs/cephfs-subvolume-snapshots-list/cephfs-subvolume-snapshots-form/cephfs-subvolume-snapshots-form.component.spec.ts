import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeSnapshotsFormComponent } from './cephfs-subvolume-snapshots-form.component';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { InputModule, ModalModule } from 'carbon-components-angular';

describe('CephfsSubvolumeSnapshotsFormComponent', () => {
  let component: CephfsSubvolumeSnapshotsFormComponent;
  let fixture: ComponentFixture<CephfsSubvolumeSnapshotsFormComponent>;

  configureTestBed({
    declarations: [CephfsSubvolumeSnapshotsFormComponent],
    imports: [
      SharedModule,
      ToastrModule.forRoot(),
      ReactiveFormsModule,
      HttpClientTestingModule,
      RouterTestingModule,
      ModalModule,
      InputModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeSnapshotsFormComponent);
    component = fixture.componentInstance;
    component.fsName = 'test_volume';
    component.subVolumeName = 'test_subvolume';
    component.subVolumeGroupName = 'test_subvolume_group';
    component.ngOnInit();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
