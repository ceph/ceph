import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeListComponent } from './cephfs-subvolume-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';

describe('CephfsSubvolumeListComponent', () => {
  let component: CephfsSubvolumeListComponent;
  let fixture: ComponentFixture<CephfsSubvolumeListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsSubvolumeListComponent],
      imports: [HttpClientTestingModule, SharedModule]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
