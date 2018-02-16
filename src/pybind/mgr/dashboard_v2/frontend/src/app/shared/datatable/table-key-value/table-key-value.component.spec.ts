import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

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
      imports: [ FormsModule, NgxDatatableModule, ComponentsModule, RouterTestingModule ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(TableKeyValueComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should make key value object pairs out of arrays with length two', () => {
    component.data = [
      ['someKey', 0],
      [3, 'something'],
    ];
    component.ngOnInit();
    expect(component.tableData.length).toBe(2);
    expect(component.tableData[0].key).toBe('someKey');
    expect(component.tableData[1].value).toBe('something');
  });

  it('should transform arrays', () => {
    component.data = [
      ['someKey', [1, 2, 3]],
      [3, 'something']
    ];
    component.ngOnInit();
    expect(component.tableData.length).toBe(2);
    expect(component.tableData[0].key).toBe('someKey');
    expect(component.tableData[0].value).toBe('1, 2, 3');
    expect(component.tableData[1].value).toBe('something');
  });

  it('should remove pure object values', () => {
    component.data = [
      [3, 'something'],
      ['will be removed', { a: 3, b: 4, c: 5}]
    ];
    component.ngOnInit();
    expect(component.tableData.length).toBe(1);
    expect(component.tableData[0].value).toBe('something');
  });

  it('should make key value object pairs out of an object', () => {
    component.data = {
      3: 'something',
      someKey: 0
    };
    component.ngOnInit();
    expect(component.tableData.length).toBe(2);
    expect(component.tableData[0].value).toBe('something');
    expect(component.tableData[1].key).toBe('someKey');
  });

  it('should make do nothing if data is correct', () => {
    component.data = [
      {
        key: 3,
        value: 'something'
      },
      {
        key: 'someKey',
        value: 0
      }
    ];
    component.ngOnInit();
    expect(component.tableData.length).toBe(2);
    expect(component.tableData[0].value).toBe('something');
    expect(component.tableData[1].key).toBe('someKey');
  });

  it('should throw error if miss match', () => {
    component.data = 38;
    expect(() => component.ngOnInit()).toThrowError('Wrong data format');
    component.data = [['someKey', 0, 3]];
    expect(() => component.ngOnInit()).toThrowError('Wrong array format');
  });
});
