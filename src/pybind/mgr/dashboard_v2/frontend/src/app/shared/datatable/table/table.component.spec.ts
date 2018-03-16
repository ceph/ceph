import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

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
        imports: [NgxDatatableModule, FormsModule, ComponentsModule, RouterTestingModule]
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

  describe('after ngInit', () => {
    const toggleColumn = (prop, checked) => {
      component.toggleColumn({
        target: {
          name: prop,
          checked: checked
        }
      });
    };

    beforeEach(() => {
      component.ngOnInit();
      component.table.sorts = component.sorts;
    });

    it('should have updated the column definitions', () => {
      expect(component.columns[0].flexGrow).toBe(1);
      expect(component.columns[1].flexGrow).toBe(2);
      expect(component.columns[2].flexGrow).toBe(2);
      expect(component.columns[2].resizeable).toBe(false);
    });

    it('should have table columns', () => {
      expect(component.tableColumns.length).toBe(3);
      expect(component.tableColumns).toEqual(component.columns);
    });

    it('should have a unique identifier which is search for', () => {
      expect(component.identifier).toBe('a');
      expect(component.sorts[0].prop).toBe('a');
      expect(component.sorts).toEqual(component.createSortingDefinition('a'));
    });

    it('should remove column "a"', () => {
      toggleColumn('a', false);
      expect(component.table.sorts[0].prop).toBe('b');
      expect(component.tableColumns.length).toBe(2);
    });

    it('should not be able to remove all columns', () => {
      toggleColumn('a', false);
      toggleColumn('b', false);
      toggleColumn('c', false);
      expect(component.table.sorts[0].prop).toBe('c');
      expect(component.tableColumns.length).toBe(1);
    });

    it('should enable column "a" again', () => {
      toggleColumn('a', false);
      toggleColumn('a', true);
      expect(component.table.sorts[0].prop).toBe('b');
      expect(component.tableColumns.length).toBe(3);
    });
  });
});
