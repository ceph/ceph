import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteSyncPipeModalComponent } from './rgw-multisite-sync-pipe-modal.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { ReactiveFormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

describe('RgwMultisiteSyncPipeModalComponent', () => {
  let component: RgwMultisiteSyncPipeModalComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPipeModalComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPipeModalComponent],
      imports: [
        HttpClientTestingModule,
        ToastrModule.forRoot(),
        PipesModule,
        ReactiveFormsModule,
        CommonModule
      ],
      providers: [NgbActiveModal]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPipeModalComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
