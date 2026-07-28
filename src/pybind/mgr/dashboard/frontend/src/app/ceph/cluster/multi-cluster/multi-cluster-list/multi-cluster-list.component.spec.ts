import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { MultiClusterListComponent } from './multi-cluster-list.component';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';
import { TableActionsComponent } from '~/app/shared/datatable/table-actions/table-actions.component';
import { SharedModule } from '~/app/shared/shared.module';
import { ActivatedRoute } from '@angular/router';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('MultiClusterListComponent', () => {
  let component: MultiClusterListComponent;
  let fixture: ComponentFixture<MultiClusterListComponent>;

  configureTestBed({
    imports: [HttpClientTestingModule, NgbNavModule, SharedModule],
    declarations: [MultiClusterListComponent],
    providers: [CdDatePipe, TableActionsComponent, { provide: ActivatedRoute, useValue: {} }]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(MultiClusterListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
