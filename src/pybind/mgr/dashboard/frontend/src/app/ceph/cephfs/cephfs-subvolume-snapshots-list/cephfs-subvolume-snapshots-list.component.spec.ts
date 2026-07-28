import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeSnapshotsListComponent } from './cephfs-subvolume-snapshots-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';

describe('CephfsSubvolumeSnapshotsListComponent', () => {
  let component: CephfsSubvolumeSnapshotsListComponent;
  let fixture: ComponentFixture<CephfsSubvolumeSnapshotsListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsSubvolumeSnapshotsListComponent],
      imports: [HttpClientTestingModule, SharedModule]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsSubvolumeSnapshotsListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show loading when the items are loading', () => {
    component.isLoading = true;
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('cd-loading-panel')).toBeTruthy();
  });

  it('should show the alert panel when there are no subvolumes', () => {
    component.isLoading = false;
    component.subvolumeGroupList = [];
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector('cd-alert-panel')).toBeTruthy();
  });
});
