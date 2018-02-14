import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { ComponentsModule } from '../../components/components.module';
import { TableComponent } from '../table/table.component';
import { TableKeyValueComponent } from './table-key-value.component';

describe('TableKeyValueComponent', () => {
  let component: TableKeyValueComponent;
  let fixture: ComponentFixture<TableKeyValueComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ TableComponent, TableKeyValueComponent ],
      imports: [ FormsModule, NgxDatatableModule, ComponentsModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TableKeyValueComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
