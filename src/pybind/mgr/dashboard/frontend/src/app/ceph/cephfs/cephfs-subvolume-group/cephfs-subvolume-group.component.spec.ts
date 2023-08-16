import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeGroupComponent } from './cephfs-subvolume-group.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

describe('CephfsSubvolumeGroupComponent', () => {
  let component: CephfsSubvolumeGroupComponent;
  let fixture: ComponentFixture<CephfsSubvolumeGroupComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsSubvolumeGroupComponent],
      imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule]
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
