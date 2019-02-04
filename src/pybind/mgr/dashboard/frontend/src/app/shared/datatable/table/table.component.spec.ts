import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import * as _ from 'lodash';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../components/components.module';
import { CdTableFetchDataContext } from '../../models/cd-table-fetch-data-context';
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
        c: [-(i % 10), 'score' + ((i % 16) + 6)],
        d: !(i % 2)
      });
    }
    return data;
  };

  const clearLocalStorage = () => {
    component.localStorage.clear();
  };

  configureTestBed({
    declarations: [TableComponent],
    imports: [NgxDatatableModule, FormsModule, ComponentsModule, RouterTestingModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TableComponent);
    component = fixture.componentInstance;
  });

  beforeEach(() => {
    component.data = createFakeData(100);
    component.columns = [
      { prop: 'a', name: 'Index' },
      { prop: 'b', name: 'Power ofA' },
      { prop: 'c', name: 'Poker array' },
      { prop: 'd', name: 'Boolean value' }
    ];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('after useData', () => {
    beforeEach(() => {
      component.useData();
    });

    it('should force an identifier', () => {
      component.identifier = 'x';
      component.forceIdentifier = true;
      component.ngOnInit();
      expect(component.identifier).toBe('x');
      expect(component.sorts[0].prop).toBe('a');
      expect(component.sorts).toEqual(component.createSortingDefinition('a'));
    });

    it('should have rows', () => {
      expect(component.data.length).toBe(100);
      expect(component.rows.length).toBe(component.data.length);
    });

    it('should have an int in setLimit parsing a string', () => {
      expect(component.limit).toBe(10);
      expect(component.limit).toEqual(jasmine.any(Number));

      const e = { target: { value: '1' } };
      component.setLimit(e);
      expect(component.userConfig.limit).toBe(1);
      expect(component.userConfig.limit).toEqual(jasmine.any(Number));
      e.target.value = '-20';
      component.setLimit(e);
      expect(component.userConfig.limit).toBe(1);
    });

    it('should force an identifier', () => {
      clearLocalStorage();
      component.identifier = 'x';
      component.forceIdentifier = true;
      component.ngOnInit();
      expect(component.identifier).toBe('x');
      expect(component.sorts[0].prop).toBe('a');
      expect(component.sorts).toEqual(component.createSortingDefinition('a'));
    });

    describe('test search', () => {
      const doSearch = (search: string, expectedLength: number, firstObject: object) => {
        component.search = search;
        component.updateFilter();
        expect(component.rows.length).toBe(expectedLength);
        expect(component.rows[0]).toEqual(firstObject);
      };

      it('should search for 13', () => {
        doSearch('13', 9, { a: 7, b: 49, c: [-7, 'score13'], d: false });
        expect(component.rows[1].a).toBe(13);
        expect(component.rows[8].a).toBe(87);
      });

      it('should search for true', () => {
        doSearch('true', 50, { a: 0, b: 0, c: [-0, 'score6'], d: true });
        expect(component.rows[0].d).toBe(true);
        expect(component.rows[1].d).toBe(true);
      });

      it('should search for false', () => {
        doSearch('false', 50, { a: 1, b: 1, c: [-1, 'score7'], d: false });
        expect(component.rows[0].d).toBe(false);
        expect(component.rows[1].d).toBe(false);
      });

      it('should test search manipulation', () => {
        let searchTerms = [];
        spyOn(component, 'subSearch').and.callFake((_d, search) => {
          expect(search).toEqual(searchTerms);
        });
        const searchTest = (s: string, st: string[]) => {
          component.search = s;
          searchTerms = st;
          component.updateFilter();
        };
        searchTest('a b c', ['a', 'b', 'c']);
        searchTest('a+b c', ['a+b', 'c']);
        searchTest('a,,,, b,,,     c', ['a', 'b', 'c']);
        searchTest('a,,,+++b,,,     c', ['a+++b', 'c']);
        searchTest('"a b c"   "d e  f", "g, h i"', ['a+b+c', 'd+e++f', 'g+h+i']);
      });

      it('should search for multiple values', () => {
        doSearch('7 5 3', 5, { a: 57, b: 3249, c: [-7, 'score15'], d: false });
      });

      it('should search with column filter', () => {
        doSearch('power:1369', 1, { a: 37, b: 1369, c: [-7, 'score11'], d: false });
        doSearch('ndex:7 ofa:5 poker:3', 3, { a: 71, b: 5041, c: [-1, 'score13'], d: false });
      });

      it('should search with through array', () => {
        doSearch('array:score21', 6, { a: 15, b: 225, c: [-5, 'score21'], d: false });
      });

      it('should search with spaces', () => {
        doSearch(`'poker array':score21`, 6, { a: 15, b: 225, c: [-5, 'score21'], d: false });
        doSearch('"poker array":score21', 6, { a: 15, b: 225, c: [-5, 'score21'], d: false });
        doSearch('poker+array:score21', 6, { a: 15, b: 225, c: [-5, 'score21'], d: false });
      });

      it('should not search if column name is incomplete', () => {
        doSearch(`'poker array'`, 100, { a: 0, b: 0, c: [-0, 'score6'], d: true });
        doSearch('pok', 100, { a: 0, b: 0, c: [-0, 'score6'], d: true });
        doSearch('pok:', 100, { a: 0, b: 0, c: [-0, 'score6'], d: true });
      });

      it('should restore full table after search', () => {
        expect(component.rows.length).toBe(100);
        component.search = '13';
        component.updateFilter();
        expect(component.rows.length).toBe(9);
        component.updateFilter(true);
        expect(component.rows.length).toBe(100);
      });
    });
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

    const equalStorageConfig = () => {
      expect(JSON.stringify(component.userConfig)).toBe(
        component.localStorage.getItem(component.tableName)
      );
    };

    beforeEach(() => {
      component.ngOnInit();
    });

    it('should have updated the column definitions', () => {
      expect(component.columns[0].flexGrow).toBe(1);
      expect(component.columns[1].flexGrow).toBe(2);
      expect(component.columns[2].flexGrow).toBe(2);
      expect(component.columns[2].resizeable).toBe(false);
    });

    it('should have table columns', () => {
      expect(component.tableColumns.length).toBe(4);
      expect(component.tableColumns).toEqual(component.columns);
    });

    it('should have a unique identifier which it searches for', () => {
      expect(component.identifier).toBe('a');
      expect(component.userConfig.sorts[0].prop).toBe('a');
      expect(component.userConfig.sorts).toEqual(component.createSortingDefinition('a'));
      equalStorageConfig();
    });

    it('should remove column "a"', () => {
      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      expect(component.userConfig.sorts[0].prop).toBe('b');
      expect(component.tableColumns.length).toBe(3);
      equalStorageConfig();
    });

    it('should not be able to remove all columns', () => {
      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      toggleColumn('b', false);
      toggleColumn('c', false);
      toggleColumn('d', false);
      expect(component.userConfig.sorts[0].prop).toBe('d');
      expect(component.tableColumns.length).toBe(1);
      equalStorageConfig();
    });

    it('should enable column "a" again', () => {
      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      toggleColumn('a', true);
      expect(component.userConfig.sorts[0].prop).toBe('b');
      expect(component.tableColumns.length).toBe(4);
      equalStorageConfig();
    });

    afterEach(() => {
      clearLocalStorage();
    });
  });

  describe('reload data', () => {
    beforeEach(() => {
      component.ngOnInit();
      component.data = [];
      component['updating'] = false;
    });

    it('should call fetchData callback function', () => {
      component.fetchData.subscribe((context) => {
        expect(context instanceof CdTableFetchDataContext).toBeTruthy();
      });
      component.reloadData();
    });

    it('should call error function', () => {
      component.data = createFakeData(5);
      component.fetchData.subscribe((context) => {
        context.error();
        expect(component.loadingError).toBeTruthy();
        expect(component.data.length).toBe(0);
        expect(component.loadingIndicator).toBeFalsy();
        expect(component['updating']).toBeFalsy();
      });
      component.reloadData();
    });

    it('should call error function with custom config', () => {
      component.data = createFakeData(10);
      component.fetchData.subscribe((context) => {
        context.errorConfig.resetData = false;
        context.errorConfig.displayError = false;
        context.error();
        expect(component.loadingError).toBeFalsy();
        expect(component.data.length).toBe(10);
        expect(component.loadingIndicator).toBeFalsy();
        expect(component['updating']).toBeFalsy();
      });
      component.reloadData();
    });

    it('should update selection on refresh - "onChange"', () => {
      spyOn(component, 'onSelect').and.callThrough();
      component.data = createFakeData(10);
      component.selection.selected = [_.clone(component.data[1])];
      component.updateSelectionOnRefresh = 'onChange';
      component.updateSelected();
      expect(component.onSelect).toHaveBeenCalledTimes(0);
      component.data[1].d = !component.data[1].d;
      component.updateSelected();
      expect(component.onSelect).toHaveBeenCalled();
    });

    it('should update selection on refresh - "always"', () => {
      spyOn(component, 'onSelect').and.callThrough();
      component.data = createFakeData(10);
      component.selection.selected = [_.clone(component.data[1])];
      component.updateSelectionOnRefresh = 'always';
      component.updateSelected();
      expect(component.onSelect).toHaveBeenCalled();
      component.data[1].d = !component.data[1].d;
      component.updateSelected();
      expect(component.onSelect).toHaveBeenCalled();
    });

    it('should update selection on refresh - "never"', () => {
      spyOn(component, 'onSelect').and.callThrough();
      component.data = createFakeData(10);
      component.selection.selected = [_.clone(component.data[1])];
      component.updateSelectionOnRefresh = 'never';
      component.updateSelected();
      expect(component.onSelect).toHaveBeenCalledTimes(0);
      component.data[1].d = !component.data[1].d;
      component.updateSelected();
      expect(component.onSelect).toHaveBeenCalledTimes(0);
    });

    afterEach(() => {
      clearLocalStorage();
    });
  });

  describe('useCustomClass', () => {
    beforeEach(() => {
      component.customCss = {
        'label label-danger': 'active',
        'secret secret-number': 123.456,
        'btn btn-sm': (v) => _.isString(v) && v.startsWith('http'),
        secure: (v) => _.isString(v) && v.startsWith('https')
      };
    });

    it('should throw an error if custom classes are not set', () => {
      component.customCss = undefined;
      expect(() => component.useCustomClass('active')).toThrowError('Custom classes are not set!');
    });

    it('should not return any class', () => {
      ['', 'something', 123, { complex: 1 }, [1, 2, 3]].forEach((value) =>
        expect(component.useCustomClass(value)).toBe(undefined)
      );
    });

    it('should match a string and return the corresponding class', () => {
      expect(component.useCustomClass('active')).toBe('label label-danger');
    });

    it('should match a number and return the corresponding class', () => {
      expect(component.useCustomClass(123.456)).toBe('secret secret-number');
    });

    it('should match against a function and return the corresponding class', () => {
      expect(component.useCustomClass('http://no.ssl')).toBe('btn btn-sm');
    });

    it('should match against multiple functions and return the corresponding classes', () => {
      expect(component.useCustomClass('https://secure.it')).toBe('btn btn-sm secure');
    });
  });
});
