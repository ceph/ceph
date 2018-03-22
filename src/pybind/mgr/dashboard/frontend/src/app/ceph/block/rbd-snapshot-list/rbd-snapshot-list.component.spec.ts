import { HttpClientTestingModule } from '@angular/common/http/testing';
import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ToastModule } from 'ng2-toastr';
import { ModalModule } from 'ngx-bootstrap';

import { ApiModule } from '../../../shared/api/api.module';
import { ComponentsModule } from '../../../shared/components/components.module';
import { DataTableModule } from '../../../shared/datatable/datatable.module';
import { AuthStorageService } from '../../../shared/services/auth-storage.service';
import { ServicesModule } from '../../../shared/services/services.module';
import { RbdSnapshotListComponent } from './rbd-snapshot-list.component';

describe('RbdSnapshotListComponent', () => {
  let component: RbdSnapshotListComponent;
  let fixture: ComponentFixture<RbdSnapshotListComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ RbdSnapshotListComponent ],
      imports: [
        DataTableModule,
        ComponentsModule,
        ModalModule.forRoot(),
        ToastModule.forRoot(),
        ServicesModule,
        ApiModule,
        HttpClientTestingModule
      ],
      providers: [ AuthStorageService ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(RbdSnapshotListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
