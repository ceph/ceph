import { async, ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { ComponentsModule } from '../../components/components.module';
import { TableComponent } from './table.component';

describe('TableComponent', () => {
  let component: TableComponent;
  let fixture: ComponentFixture<TableComponent>;

  const createFakeData = (n) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push({
        a: i,
        b: i * i,
        c: [-(i % 10), 'score' + (i % 16 + 6) ]
      });
    }
    return data;
  };

  const doSearch = (search: string, expectedLength: number, firstObject: object) => {
    component.search = search;
    component.updateFilter(true);
    expect(component.rows.length).toBe(expectedLength);
    expect(component.rows[0]).toEqual(firstObject);
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
      {prop: 'a', name: 'Index'},
      {prop: 'b', name: 'Power ofA'},
      {prop: 'c', name: 'Poker array'}
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
    doSearch('13', 9, {a: 7, b: 49, c: [ -7, 'score13'] });
    expect(component.rows[1].a).toBe(13);
    expect(component.rows[8].a).toBe(87);
  });

  it('should test search manipulation', () => {
    let searchTerms = [];
    spyOn(component, 'subSearch').and.callFake((d, search, c) => {
      expect(search).toEqual(searchTerms);
    });
    const searchTest = (s: string, st: string[]) => {
      component.search = s;
      searchTerms = st;
      component.updateFilter(true);
    };
    searchTest('a b c', [ 'a', 'b', 'c' ]);
    searchTest('a+b c', [ 'a+b', 'c' ]);
    searchTest('a,,,, b,,,     c', [ 'a', 'b', 'c' ]);
    searchTest('a,,,+++b,,,     c', [ 'a+++b', 'c' ]);
    searchTest('"a b c"   "d e  f", "g, h i"', [ 'a+b+c', 'd+e++f', 'g+h+i' ]);
  });

  it('should search for multiple values', () => {
    doSearch('7 5 3', 5, {a: 57, b: 3249, c: [ -7, 'score15']});
  });

  it('should search with column filter', () => {
    doSearch('power:1369', 1, {a: 37, b: 1369, c: [ -7, 'score11']});
    doSearch('ndex:7 ofa:5 poker:3', 3, {a: 71, b: 5041, c: [-1, 'score13']});
  });

  it('should search with through array', () => {
    doSearch('array:score21', 6, {a: 15, b: 225, c: [-5, 'score21']});
  });

  it('should search with spaces', () => {
    doSearch('\'poker array\':score21', 6, {a: 15, b: 225, c: [-5, 'score21']});
    doSearch('"poker array":score21', 6, {a: 15, b: 225, c: [-5, 'score21']});
    doSearch('poker+array:score21', 6, {a: 15, b: 225, c: [-5, 'score21']});
  });

  it('should not search if column name is incomplete', () => {
    doSearch('\'poker array\'', 100, {a: 0, b: 0, c: [-0, 'score6']});
    doSearch('pok', 100, {a: 0, b: 0, c: [-0, 'score6']});
    doSearch('pok:', 100, {a: 0, b: 0, c: [-0, 'score6']});
  });

  it('should restore full table after search', () => {
    expect(component.rows.length).toBe(100);
    component.search = '13';
    component.updateFilter(true);
    expect(component.rows.length).toBe(9);
    component.updateFilter();
    expect(component.rows.length).toBe(100);
  });

  it('should force an identifier', () => {
    component.identifier = 'x';
    component.forceIdentifier = true;
    component.ngOnInit();
    expect(component.identifier).toBe('x');
    expect(component.sorts[0].prop).toBe('a');
    expect(component.sorts).toEqual(component.createSortingDefinition('a'));
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
