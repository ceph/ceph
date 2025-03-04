import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BucketTagModalComponent } from './bucket-tag-modal.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { CUSTOM_ELEMENTS_SCHEMA, NO_ERRORS_SCHEMA } from '@angular/core';

describe('BucketTagModalComponent', () => {
  let component: BucketTagModalComponent;
  let fixture: ComponentFixture<BucketTagModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [BucketTagModalComponent],
      imports: [HttpClientTestingModule, ReactiveFormsModule],
      schemas: [NO_ERRORS_SCHEMA, CUSTOM_ELEMENTS_SCHEMA],
      providers: [NgbActiveModal]
    }).compileComponents();

    fixture = TestBed.createComponent(BucketTagModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
