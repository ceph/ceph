import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeListComponent } from './cephfs-subvolume-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { RouterTestingModule } from '@angular/router/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

describe('CephfsSubvolumeListComponent', () => {
  let component: CephfsSubvolumeListComponent;
  let fixture: ComponentFixture<CephfsSubvolumeListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsSubvolumeListComponent],
      imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot(), RouterTestingModule],
      providers: [NgbActiveModal]
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
