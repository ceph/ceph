import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { MultiClusterListComponent } from './multi-cluster-list.component';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '~/app/shared/shared.module';

describe('MultiClusterListComponent', () => {
  let component: MultiClusterListComponent;
  let fixture: ComponentFixture<MultiClusterListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, ToastrModule.forRoot(), NgbNavModule, SharedModule],
      declarations: [MultiClusterListComponent],
      providers: [CdDatePipe, TableActionsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(MultiClusterListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
