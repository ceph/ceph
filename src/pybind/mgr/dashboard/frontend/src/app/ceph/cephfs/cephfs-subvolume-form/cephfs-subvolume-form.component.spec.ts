import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsSubvolumeFormComponent } from './cephfs-subvolume-form.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';

describe('CephfsSubvolumeFormComponent', () => {
  let component: CephfsSubvolumeFormComponent;
  let fixture: ComponentFixture<CephfsSubvolumeFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsSubvolumeFormComponent],
      providers: [NgbActiveModal],
      imports: [
        SharedModule,
        ToastrModule.forRoot(),
        ReactiveFormsModule,
        HttpClientTestingModule,
        RouterTestingModule
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CephfsSubvolumeFormComponent);
    component = fixture.componentInstance;
    component.pools = [];
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
