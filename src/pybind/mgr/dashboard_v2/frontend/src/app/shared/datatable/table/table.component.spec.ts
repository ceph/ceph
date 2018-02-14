import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';

import { NgxDatatableModule, TableColumn } from '@swimlane/ngx-datatable';

import { ComponentsModule } from '../../components/components.module';
import { TableComponent } from './table.component';

describe('TableComponent', () => {
  let component: TableComponent;
  let fixture: ComponentFixture<TableComponent>;
  const columns: TableColumn[] = [];
  const createFakeData = (n) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push({
        a: i,
        b: i * i,
        c: -(i % 10)
      });
    }
    return data;
  };

  beforeEach(
    async(() => {
      TestBed.configureTestingModule({
        declarations: [TableComponent],
        imports: [NgxDatatableModule, FormsModule, ComponentsModule]
      }).compileComponents();
    })
  );

  beforeEach(() => {
    fixture = TestBed.createComponent(TableComponent);
    component = fixture.componentInstance;
  });

  beforeEach(() => {
    component.data = createFakeData(100);
    component.useData();
    component.columns = [
      {prop: 'a'},
      {prop: 'b'},
      {prop: 'c'}
    ];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have rows', () => {
    expect(component.data.length).toBe(100);
    expect(component.rows.length).toBe(component.data.length);
  });

  it('should have an int in setLimit parsing a string', () => {
    expect(component.limit).toBe(10);
    expect(component.limit).toEqual(jasmine.any(Number));

    const e = {target: {value: '1'}};
    component.setLimit(e);
    expect(component.limit).toBe(1);
    expect(component.limit).toEqual(jasmine.any(Number));
    e.target.value = '-20';
    component.setLimit(e);
    expect(component.limit).toBe(1);
  });

  it('should search for 13', () => {
    component.search = '13';
    expect(component.rows.length).toBe(100);
    component.updateFilter(true);
    expect(component.rows[0].a).toBe(13);
    expect(component.rows[1].b).toBe(1369);
    expect(component.rows[2].b).toBe(3136);
    expect(component.rows.length).toBe(3);
  });

  it('should restore full table after search', () => {
    component.search = '13';
    expect(component.rows.length).toBe(100);
    component.updateFilter(true);
    expect(component.rows.length).toBe(3);
    component.updateFilter();
    expect(component.rows.length).toBe(100);
  });
});
