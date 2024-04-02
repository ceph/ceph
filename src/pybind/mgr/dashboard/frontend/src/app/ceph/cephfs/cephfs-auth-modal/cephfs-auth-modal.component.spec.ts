import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsAuthModalComponent } from './cephfs-auth-modal.component';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { ReactiveFormsModule } from '@angular/forms';

describe('CephfsAuthModalComponent', () => {
  let component: CephfsAuthModalComponent;
  let fixture: ComponentFixture<CephfsAuthModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsAuthModalComponent],
      imports: [HttpClientTestingModule, SharedModule, ReactiveFormsModule, ToastrModule.forRoot()],
      providers: [NgbActiveModal]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsAuthModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
