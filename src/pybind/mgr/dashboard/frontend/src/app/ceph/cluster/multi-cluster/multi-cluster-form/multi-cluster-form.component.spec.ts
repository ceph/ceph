import { ComponentFixture, TestBed } from '@angular/core/testing';

import { MultiClusterFormComponent } from './multi-cluster-form.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ToastrModule } from 'ngx-toastr';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { DatePipe } from '@angular/common';
import { ReactiveFormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

describe('MultiClusterFormComponent', () => {
  let component: MultiClusterFormComponent;
  let fixture: ComponentFixture<MultiClusterFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [
        SharedModule,
        ReactiveFormsModule,
        RouterTestingModule,
        HttpClientTestingModule,
        ToastrModule.forRoot()
      ],
      declarations: [MultiClusterFormComponent],
      providers: [NgbActiveModal, NotificationService, CdDatePipe, DatePipe]
    }).compileComponents();

    fixture = TestBed.createComponent(MultiClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
