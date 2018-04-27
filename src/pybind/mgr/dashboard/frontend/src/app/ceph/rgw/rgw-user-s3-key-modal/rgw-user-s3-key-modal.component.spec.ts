import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { SharedModule } from '../../../shared/shared.module';
import { RgwUserS3KeyModalComponent } from './rgw-user-s3-key-modal.component';

describe('RgwUserS3KeyModalComponent', () => {
  let component: RgwUserS3KeyModalComponent;
  let fixture: ComponentFixture<RgwUserS3KeyModalComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RgwUserS3KeyModalComponent ],
      imports: [
        ReactiveFormsModule,
        SharedModule
      ],
      providers: [ BsModalRef ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserS3KeyModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
