import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import { ToastModule } from 'ng2-toastr';
import { BsModalRef, BsModalService } from 'ngx-bootstrap';

import { ApiModule } from '../../../shared/api/api.module';
import { ComponentsModule } from '../../../shared/components/components.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ServicesModule } from '../../../shared/services/services.module';
import { RbdSnapshotFormComponent } from './rbd-snapshot-form.component';

describe('RbdSnapshotFormComponent', () => {
  let component: RbdSnapshotFormComponent;
  let fixture: ComponentFixture<RbdSnapshotFormComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      imports: [
        ReactiveFormsModule,
        ComponentsModule,
        HttpClientTestingModule,
        ServicesModule,
        ApiModule,
        ToastModule.forRoot()
      ],
      declarations: [ RbdSnapshotFormComponent ],
      providers: [ BsModalRef, BsModalService, AuthStorageService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdSnapshotFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
