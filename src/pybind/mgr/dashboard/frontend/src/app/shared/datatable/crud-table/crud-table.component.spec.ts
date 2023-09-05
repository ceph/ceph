/* tslint:disable:no-unused-variable */
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbDropdownModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import { NgxPipeFunctionModule } from 'ngx-pipe-function';
import { ToastrModule } from 'ngx-toastr';

import { ComponentsModule } from '~/app/shared/components/components.module';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TableKeyValueComponent } from '../table-key-value/table-key-value.component';
import { TablePaginationComponent } from '../table-pagination/table-pagination.component';
import { TableComponent } from '../table/table.component';
import { CRUDTableComponent } from './crud-table.component';

describe('CRUDTableComponent', () => {
  let component: CRUDTableComponent;
  let fixture: ComponentFixture<CRUDTableComponent>;

  configureTestBed({
    declarations: [
      CRUDTableComponent,
      TableComponent,
      TableKeyValueComponent,
      TablePaginationComponent
    ],
    imports: [
      NgxDatatableModule,
      FormsModule,
      ComponentsModule,
      NgbDropdownModule,
      PipesModule,
      NgbTooltipModule,
      RouterTestingModule,
      NgxPipeFunctionModule,
      HttpClientTestingModule,
      ToastrModule.forRoot()
    ]
  });
  beforeEach(() => {
    fixture = TestBed.createComponent(CRUDTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
