import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeGroupComponent } from './cephfs-subvolume-group.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';

describe('CephfsSubvolumeGroupComponent', () => {
  let component: CephfsSubvolumeGroupComponent;
  let fixture: ComponentFixture<CephfsSubvolumeGroupComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsSubvolumeGroupComponent],
      imports: [HttpClientTestingModule]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeGroupComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
