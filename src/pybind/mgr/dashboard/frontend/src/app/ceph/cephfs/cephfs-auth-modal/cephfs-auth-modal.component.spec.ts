import { ComponentFixture, TestBed } from '@angular/core/testing';

import { CephfsAuthModalComponent } from './cephfs-auth-modal.component';
import { NgbTypeaheadModule } from '@ng-bootstrap/ng-bootstrap';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '~/app/shared/shared.module';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { CheckboxModule, InputModule, ModalModule } from 'carbon-components-angular';

describe('CephfsAuthModalComponent', () => {
  let component: CephfsAuthModalComponent;
  let fixture: ComponentFixture<CephfsAuthModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsAuthModalComponent],
      imports: [
        HttpClientTestingModule,
        SharedModule,
        ReactiveFormsModule,
        ToastrModule.forRoot(),
        RouterTestingModule,
        NgbTypeaheadModule,
        ModalModule,
        InputModule,
        CheckboxModule
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsAuthModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
