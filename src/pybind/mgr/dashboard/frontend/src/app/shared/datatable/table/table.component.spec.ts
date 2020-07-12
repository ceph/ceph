import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';
import * as _ from 'lodash';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../components/components.module';
import { CdTableColumnFilter } from '../../models/cd-table-column-filter';
import { CdTableFetchDataContext } from '../../models/cd-table-fetch-data-context';
import { CdTableSelection } from '../../models/cd-table-selection';
import { PipesModule } from '../../pipes/pipes.module';
import { TableComponent } from './table.component';

describe('TableComponent', () => {
  let component: TableComponent;
  let fixture: ComponentFixture<TableComponent>;

  const createFakeData = (n: number) => {
    const data = [];
    for (let i = 0; i < n; i++) {
      data.push({
        a: i,
        b: i * 10,
        c: !!(i % 2)
      });
    }
    return data;
  };

  const clearLocalStorage = () => {
    component.localStorage.clear();
  };

  configureTestBed({
    declarations: [TableComponent],
    imports: [
      BrowserAnimationsModule,
      NgxDatatableModule,
      FormsModule,
      ComponentsModule,
      RouterTestingModule,
      NgbDropdownModule,
      PipesModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TableComponent);
    component = fixture.componentInstance;

    component.data = createFakeData(10);
    component.localColumns = component.columns = [
      { prop: 'a', name: 'Index', filterable: true },
      { prop: 'b', name: 'Index times ten' },
      { prop: 'c', name: 'Odd?', filterable: true }
    ];
  });

  it('should create', () => {
    expect(component).toBeTruthy();
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
    component.useData();
    expect(component.data.length).toBe(10);
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

  it('should prevent propagation of mouseenter event', (done) => {
    let wasCalled = false;
    const mouseEvent = new MouseEvent('mouseenter');
    mouseEvent.stopPropagation = () => {
      wasCalled = true;
    };
    spyOn(component.table.element, 'addEventListener').and.callFake((eventName, fn) => {
      fn(mouseEvent);
      expect(eventName).toBe('mouseenter');
      expect(wasCalled).toBe(true);
      done();
    });
    component.ngOnInit();
  });

  it('should call updateSelection on init', () => {
    component.updateSelection.subscribe((selection: CdTableSelection) => {
      expect(selection.hasSelection).toBeFalsy();
      expect(selection.hasSingleSelection).toBeFalsy();
      expect(selection.hasMultiSelection).toBeFalsy();
      expect(selection.selected.length).toBe(0);
    });
    component.ngOnInit();
  });

  describe('test column filtering', () => {
    let filterIndex: CdTableColumnFilter;
    let filterOdd: CdTableColumnFilter;
    let filterCustom: CdTableColumnFilter;

    const expectColumnFilterCreated = (
      filter: CdTableColumnFilter,
      prop: string,
      options: string[],
      value?: { raw: string; formatted: string }
    ) => {
      expect(filter.column.prop).toBe(prop);
      expect(_.map(filter.options, 'raw')).toEqual(options);
      expect(filter.value).toEqual(value);
    };

    const expectColumnFiltered = (
      changes: { filter: CdTableColumnFilter; value?: string }[],
      results: any[],
      search: string = ''
    ) => {
      component.search = search;
      _.forEach(changes, (change) => {
        component.onChangeFilter(
          change.filter,
          change.value ? { raw: change.value, formatted: change.value } : undefined
        );
      });
      expect(component.rows).toEqual(results);
      component.onClearSearch();
      component.onClearFilters();
    };

    describe('with visible columns', () => {
      beforeEach(() => {
        component.initColumnFilters();
        component.updateColumnFilterOptions();
        filterIndex = component.columnFilters[0];
        filterOdd = component.columnFilters[1];
      });

      it('should have filters initialized', () => {
        expect(component.columnFilters.length).toBe(2);
        expectColumnFilterCreated(
          filterIndex,
          'a',
          _.map(component.data, (row) => _.toString(row.a))
        );
        expectColumnFilterCreated(filterOdd, 'c', ['false', 'true']);
      });

      it('should add filters', () => {
        // single
        expectColumnFiltered([{ filter: filterIndex, value: '1' }], [{ a: 1, b: 10, c: true }]);

        // multiple
        expectColumnFiltered(
          [
            { filter: filterOdd, value: 'false' },
            { filter: filterIndex, value: '2' }
          ],
          [{ a: 2, b: 20, c: false }]
        );

        // Clear should work
        expect(component.rows).toEqual(component.data);
      });

      it('should remove filters', () => {
        // single
        expectColumnFiltered(
          [
            { filter: filterOdd, value: 'true' },
            { filter: filterIndex, value: '1' },
            { filter: filterIndex, value: undefined }
          ],
          [
            { a: 1, b: 10, c: true },
            { a: 3, b: 30, c: true },
            { a: 5, b: 50, c: true },
            { a: 7, b: 70, c: true },
            { a: 9, b: 90, c: true }
          ]
        );

        // multiple
        expectColumnFiltered(
          [
            { filter: filterOdd, value: 'true' },
            { filter: filterIndex, value: '1' },
            { filter: filterIndex, value: undefined },
            { filter: filterOdd, value: undefined }
          ],
          component.data
        );

        // a selected filter should be removed if it's selected again
        expectColumnFiltered(
          [
            { filter: filterOdd, value: 'true' },
            { filter: filterIndex, value: '1' },
            { filter: filterIndex, value: '1' }
          ],
          [
            { a: 1, b: 10, c: true },
            { a: 3, b: 30, c: true },
            { a: 5, b: 50, c: true },
            { a: 7, b: 70, c: true },
            { a: 9, b: 90, c: true }
          ]
        );
      });

      it('should search from filtered rows', () => {
        expectColumnFiltered(
          [{ filter: filterOdd, value: 'true' }],
          [{ a: 9, b: 90, c: true }],
          '9'
        );

        // Clear should work
        expect(component.rows).toEqual(component.data);
      });
    });

    describe('with custom columns', () => {
      beforeEach(() => {
        // create a new additional column in data
        for (let i = 0; i < component.data.length; i++) {
          const row = component.data[i];
          row['d'] = row.a;
        }
        // create a custom column filter
        component.extraFilterableColumns = [
          {
            name: 'd less than 5',
            prop: 'd',
            filterOptions: ['yes', 'no'],
            filterInitValue: 'yes',
            filterPredicate: (row, value) => {
              if (value === 'yes') {
                return row.d < 5;
              } else {
                return row.d >= 5;
              }
            }
          }
        ];
        component.initColumnFilters();
        component.updateColumnFilterOptions();
        filterIndex = component.columnFilters[0];
        filterOdd = component.columnFilters[1];
        filterCustom = component.columnFilters[2];
      });

      it('should have filters initialized', () => {
        expect(component.columnFilters.length).toBe(3);
        expectColumnFilterCreated(filterCustom, 'd', ['yes', 'no'], {
          raw: 'yes',
          formatted: 'yes'
        });
        component.useData();
        expect(component.rows).toEqual(_.slice(component.data, 0, 5));
      });

      it('should remove filters', () => {
        expectColumnFiltered([{ filter: filterCustom, value: 'no' }], _.slice(component.data, 5));
      });
    });
  });

  describe('test search', () => {
    const expectSearch = (keyword: string, expectedResult: object[]) => {
      component.search = keyword;
      component.updateFilter();
      expect(component.rows).toEqual(expectedResult);
      component.onClearSearch();
    };

    describe('searchableObjects', () => {
      const testObject = {
        obj: {
          min: 8,
          max: 123
        }
      };

      beforeEach(() => {
        component.data = [testObject];
        component.localColumns = [{ prop: 'obj', name: 'Object' }];
      });

      it('should not search through objects as default case', () => {
        expect(component.searchableObjects).toBe(false);
        expectSearch('8', []);
      });

      it('should search through objects if searchableObjects is set to true', () => {
        component.searchableObjects = true;
        expectSearch('28', []);
        expectSearch('8', [testObject]);
        expectSearch('123', [testObject]);
        expectSearch('max', [testObject]);
      });
    });

    it('should find a particular number', () => {
      expectSearch('5', [{ a: 5, b: 50, c: true }]);
      expectSearch('9', [{ a: 9, b: 90, c: true }]);
    });

    it('should find boolean values', () => {
      expectSearch('true', [
        { a: 1, b: 10, c: true },
        { a: 3, b: 30, c: true },
        { a: 5, b: 50, c: true },
        { a: 7, b: 70, c: true },
        { a: 9, b: 90, c: true }
      ]);
      expectSearch('false', [
        { a: 0, b: 0, c: false },
        { a: 2, b: 20, c: false },
        { a: 4, b: 40, c: false },
        { a: 6, b: 60, c: false },
        { a: 8, b: 80, c: false }
      ]);
    });

    it('should test search keyword preparation', () => {
      const prepare = TableComponent.prepareSearch;
      const expected = ['a', 'b', 'c'];
      expect(prepare('a b c')).toEqual(expected);
      expect(prepare('a,, b,,  c')).toEqual(expected);
      expect(prepare('a,,,, b,,,     c')).toEqual(expected);
      expect(prepare('a+b c')).toEqual(['a+b', 'c']);
      expect(prepare('a,,,+++b,,,     c')).toEqual(['a+++b', 'c']);
      expect(prepare('"a b c"   "d e  f", "g, h i"')).toEqual(['a+b+c', 'd+e++f', 'g+h+i']);
    });

    it('should search for multiple values', () => {
      expectSearch('2 20 false', [{ a: 2, b: 20, c: false }]);
      expectSearch('false 2', [{ a: 2, b: 20, c: false }]);
    });

    it('should filter by column', () => {
      expectSearch('index:5', [{ a: 5, b: 50, c: true }]);
      expectSearch('times:50', [{ a: 5, b: 50, c: true }]);
      expectSearch('times:50 index:5', [{ a: 5, b: 50, c: true }]);
      expectSearch('Odd?:true', [
        { a: 1, b: 10, c: true },
        { a: 3, b: 30, c: true },
        { a: 5, b: 50, c: true },
        { a: 7, b: 70, c: true },
        { a: 9, b: 90, c: true }
      ]);
      component.data = createFakeData(100);
      expectSearch('index:1 odd:true times:110', [{ a: 11, b: 110, c: true }]);
    });

    it('should search through arrays', () => {
      component.localColumns = [
        { prop: 'a', name: 'Index' },
        { prop: 'b', name: 'ArrayColumn' }
      ];

      component.data = [
        { a: 1, b: ['foo', 'bar'] },
        { a: 2, b: ['baz', 'bazinga'] }
      ];
      expectSearch('bar', [{ a: 1, b: ['foo', 'bar'] }]);
      expectSearch('arraycolumn:bar arraycolumn:foo', [{ a: 1, b: ['foo', 'bar'] }]);
      expectSearch('arraycolumn:baz arraycolumn:inga', [{ a: 2, b: ['baz', 'bazinga'] }]);

      component.data = [
        { a: 1, b: [1, 2] },
        { a: 2, b: [3, 4] }
      ];
      expectSearch('arraycolumn:1 arraycolumn:2', [{ a: 1, b: [1, 2] }]);
    });

    it('should search with spaces', () => {
      const expectedResult = [{ a: 2, b: 20, c: false }];
      expectSearch(`'Index times ten':20`, expectedResult);
      expectSearch('index+times+ten:20', expectedResult);
    });

    it('should filter results although column name is incomplete', () => {
      component.data = createFakeData(3);
      expectSearch(`'Index times ten'`, []);
      expectSearch(`'Ind'`, []);
      expectSearch(`'Ind:'`, [
        { a: 0, b: 0, c: false },
        { a: 1, b: 10, c: true },
        { a: 2, b: 20, c: false }
      ]);
    });

    it('should search if column name is incomplete', () => {
      const expectedData = [
        { a: 0, b: 0, c: false },
        { a: 1, b: 10, c: true },
        { a: 2, b: 20, c: false }
      ];
      component.data = _.clone(expectedData);
      expectSearch('inde', []);
      expectSearch('index:', expectedData);
      expectSearch('index times te', []);
    });

    it('should restore full table after search', () => {
      component.useData();
      expect(component.rows.length).toBe(10);
      component.search = '3';
      component.updateFilter();
      expect(component.rows.length).toBe(1);
      component.onClearSearch();
      expect(component.rows.length).toBe(10);
    });

    it('should work with undefined data', () => {
      component.data = undefined;
      component.search = '3';
      component.updateFilter();
      expect(component.rows).toBeUndefined();
    });
  });

  describe('after ngInit', () => {
    const toggleColumn = (prop: string, checked: boolean) => {
      component.toggleColumn({
        prop: prop,
        isHidden: checked
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
      expect(component.localColumns[0].flexGrow).toBe(1);
      expect(component.localColumns[1].flexGrow).toBe(2);
      expect(component.localColumns[2].flexGrow).toBe(2);
      expect(component.localColumns[2].resizeable).toBe(false);
    });

    it('should have table columns', () => {
      expect(component.tableColumns.length).toBe(3);
      expect(component.tableColumns).toEqual(component.localColumns);
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
      expect(component.tableColumns.length).toBe(2);
      equalStorageConfig();
    });

    it('should not be able to remove all columns', () => {
      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      toggleColumn('b', false);
      toggleColumn('c', false);
      expect(component.userConfig.sorts[0].prop).toBe('c');
      expect(component.tableColumns.length).toBe(1);
      equalStorageConfig();
    });

    it('should enable column "a" again', () => {
      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      toggleColumn('a', true);
      expect(component.userConfig.sorts[0].prop).toBe('b');
      expect(component.tableColumns.length).toBe(3);
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
      component.fetchData.subscribe((context: any) => {
        expect(context instanceof CdTableFetchDataContext).toBeTruthy();
      });
      component.reloadData();
    });

    it('should call error function', () => {
      component.data = createFakeData(5);
      component.fetchData.subscribe((context: any) => {
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
      component.fetchData.subscribe((context: any) => {
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
        'badge badge-danger': 'active',
        'secret secret-number': 123.456,
        btn: (v) => _.isString(v) && v.startsWith('http'),
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
      expect(component.useCustomClass('active')).toBe('badge badge-danger');
    });

    it('should match a number and return the corresponding class', () => {
      expect(component.useCustomClass(123.456)).toBe('secret secret-number');
    });

    it('should match against a function and return the corresponding class', () => {
      expect(component.useCustomClass('http://no.ssl')).toBe('btn');
    });

    it('should match against multiple functions and return the corresponding classes', () => {
      expect(component.useCustomClass('https://secure.it')).toBe('btn secure');
    });
  });

  describe('test expand and collapse feature', () => {
    beforeEach(() => {
      spyOn(component.setExpandedRow, 'emit');
      component.table = {
        rowDetail: { collapseAllRows: jest.fn(), toggleExpandRow: jest.fn() }
      } as any;

      // Setup table
      component.identifier = 'a';
      component.data = createFakeData(10);

      // Select item
      component.expanded = _.clone(component.data[1]);
    });

    describe('update expanded on refresh', () => {
      const updateExpendedOnState = (state: 'always' | 'never' | 'onChange') => {
        component.updateExpandedOnRefresh = state;
        component.updateExpanded();
      };

      beforeEach(() => {
        // Mock change
        component.data[1].b = 'test';
      });

      it('refreshes "always"', () => {
        updateExpendedOnState('always');
        expect(component.expanded.b).toBe('test');
        expect(component.setExpandedRow.emit).toHaveBeenCalled();
      });

      it('refreshes "onChange"', () => {
        updateExpendedOnState('onChange');
        expect(component.expanded.b).toBe('test');
        expect(component.setExpandedRow.emit).toHaveBeenCalled();
      });

      it('does not refresh "onChange" if data is equal', () => {
        component.data[1].b = 10; // Reverts change
        updateExpendedOnState('onChange');
        expect(component.expanded.b).toBe(10);
        expect(component.setExpandedRow.emit).not.toHaveBeenCalled();
      });

      it('"never" refreshes', () => {
        updateExpendedOnState('never');
        expect(component.expanded.b).toBe(10);
        expect(component.setExpandedRow.emit).not.toHaveBeenCalled();
      });
    });

    it('should open the table details and close other expanded rows', () => {
      component.toggleExpandRow(component.expanded, false);
      expect(component.expanded).toEqual({ a: 1, b: 10, c: true });
      expect(component.table.rowDetail.collapseAllRows).toHaveBeenCalled();
      expect(component.setExpandedRow.emit).toHaveBeenCalledWith(component.expanded);
      expect(component.table.rowDetail.toggleExpandRow).toHaveBeenCalled();
    });

    it('should close the current table details expansion', () => {
      component.toggleExpandRow(component.expanded, true);
      expect(component.expanded).toBeUndefined();
      expect(component.setExpandedRow.emit).toHaveBeenCalledWith(undefined);
      expect(component.table.rowDetail.toggleExpandRow).toHaveBeenCalled();
    });
  });
});
