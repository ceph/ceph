import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbDropdownModule, NgbTooltipModule } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { ComponentsModule } from '~/app/shared/components/components.module';
import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumnFilter } from '~/app/shared/models/cd-table-column-filter';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { TablePaginationComponent } from '../table-pagination/table-pagination.component';
import { TableComponent } from './table.component';
import { TableModule } from 'carbon-components-angular';
import { NO_ERRORS_SCHEMA } from '@angular/core';

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
    declarations: [TableComponent, TablePaginationComponent],
    imports: [
      BrowserAnimationsModule,
      FormsModule,
      ComponentsModule,
      RouterTestingModule,
      NgbDropdownModule,
      PipesModule,
      TableModule,
      NgbTooltipModule
    ],
    schemas: [NO_ERRORS_SCHEMA]
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
    component.ngAfterViewInit();
    fixture.detectChanges();
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
        component.onSelectFilter(change.filter.column.name);
        component.onChangeFilter(change.value || undefined);
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
      component.useData();
      component.ngAfterViewInit();
      fixture.detectChanges();
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
      component.data = [];
      component.search = '3';
      component.updateFilter();
      expect(component.rows?.length).toBeFalsy();
    });
  });

  describe('after ngInit', () => {
    const toggleColumn = (prop: string, checked: boolean) => {
      component.toggleColumn({
        data: prop,
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
      component.ngAfterViewInit();
      fixture.detectChanges();
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
      const expectedData = [
        { a: 0, b: 0, c: false },
        { a: 1, b: 10, c: true },
        { a: 2, b: 20, c: false }
      ];
      component.data = _.clone(expectedData);
      fixture.detectChanges();

      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      expect(component.userConfig.sorts[0].prop).toBe('b');
      expect(component.visibleColumns.length).toBe(2);
      equalStorageConfig();
    });

    it('should not be able to remove all columns', () => {
      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      toggleColumn('b', false);
      toggleColumn('c', false);
      expect(component.userConfig.sorts[0].prop).toBe('c');
      expect(component.visibleColumns.length).toBe(1);
      equalStorageConfig();
    });

    it('should enable column "a" again', () => {
      expect(component.userConfig.sorts[0].prop).toBe('a');
      toggleColumn('a', false);
      toggleColumn('a', true);
      expect(component.userConfig.sorts[0].prop).toBe('b');
      expect(component.visibleColumns.length).toBe(3);
      equalStorageConfig();
    });

    it('should toggle on off columns', () => {
      for (const column of component.columns) {
        component.toggleColumn(column);
        expect(column.isHidden).toBeTruthy();
        component.toggleColumn(column);
        expect(column.isHidden).toBeFalsy();
      }
    });

    afterEach(() => {
      clearLocalStorage();
    });
  });

  describe('test cell transformations', () => {
    interface ExecutingTemplateConfig {
      valueClass?: string;
      executingClass?: string;
    }

    const testExecutingTemplate = (templateConfig?: ExecutingTemplateConfig) => {
      const state = 'updating';
      const value = component.data[0].a;

      component.autoReload = -1;
      component.columns[0].cellTransformation = CellTemplate.executing;
      if (templateConfig) {
        component.columns[0].customTemplateConfig = templateConfig;
      }

      const data = createFakeData(10);
      const firstRow = {
        ...data[0],
        cdExecuting: state,
        customTemplateConfig: templateConfig || undefined
      };
      component.data = [firstRow, data.filter((x) => x.a !== firstRow.a)];
      component.localColumns = component.columns = [
        { prop: 'a', name: 'Index', filterable: true, cellTransformation: CellTemplate.executing },
        { prop: 'b', name: 'Index times ten' },
        { prop: 'c', name: 'Odd?', filterable: true }
      ];
      component.ngOnInit();
      component.ngAfterViewInit();
      fixture.detectChanges();

      const elements = fixture.debugElement
        .query(By.css('[cdstablerow] [cdstabledata]'))
        .queryAll(By.css('span'));
      expect(elements.length).toBe(2);

      // Value
      const valueElement = elements[0];
      if (templateConfig?.valueClass) {
        templateConfig.valueClass.split(' ').forEach((clz) => {
          expect(valueElement.classes).toHaveProperty(clz);
        });
      }
      expect(valueElement.nativeElement.textContent.trim()).toBe(`${value}`);
      // Executing state
      const executingElement = elements[1];
      if (templateConfig?.executingClass) {
        templateConfig.executingClass.split(' ').forEach((clz) => {
          expect(executingElement.classes).toHaveProperty(clz);
        });
      }
      expect(executingElement.nativeElement.textContent.trim()).toBe(`(${state})`);
    };

    it('should display executing template', () => {
      testExecutingTemplate();
    });

    it('should display executing template with custom classes', () => {
      testExecutingTemplate({ valueClass: 'a b', executingClass: 'c d' });
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
        expect(component.status.type).toBe('danger');
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
        expect(component.status.type).toBe('danger');
        expect(component.data.length).toBe(10);
        expect(component.loadingIndicator).toBeFalsy();
        expect(component['updating']).toBeFalsy();
      });
      component.reloadData();
    });

    it('should update selection on refresh - "onChange"', () => {
      spyOn(component.updateSelection, 'emit');
      component.data = createFakeData(10);
      component.selection.selected = [_.clone(component.data[1])];
      component.updateSelectionOnRefresh = 'onChange';
      component.updateSelected();
      expect(component.updateSelection.emit).toHaveBeenCalledTimes(0);
      component.data[1].d = !component.data[1].d;
      component.updateSelected();
      expect(component.updateSelection.emit).toHaveBeenCalled();
    });

    it('should update selection on refresh - "always"', () => {
      spyOn(component.updateSelection, 'emit');
      component.data = createFakeData(10);
      component.selection.selected = [_.clone(component.data[1])];
      component.updateSelectionOnRefresh = 'always';
      component.updateSelected();
      expect(component.updateSelection.emit).toHaveBeenCalled();
      component.data[1].d = !component.data[1].d;
      component.updateSelected();
      expect(component.updateSelection.emit).toHaveBeenCalled();
    });

    it('should update selection on refresh - "never"', () => {
      spyOn(component.updateSelection, 'emit');
      component.data = createFakeData(10);
      component.selection.selected = [_.clone(component.data[1])];
      component.updateSelectionOnRefresh = 'never';
      component.updateSelected();
      expect(component.updateSelection.emit).toHaveBeenCalledTimes(0);
      component.data[1].d = !component.data[1].d;
      component.updateSelected();
      expect(component.updateSelection.emit).toHaveBeenCalledTimes(0);
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
        // setExpandRow is called to reset the expanded state.
        // Commeting out the line below because this might be reversed on next iteration
        // expect(component.setExpandedRow.emit).not.toHaveBeenCalled();
      });

      it('"never" refreshes', () => {
        updateExpendedOnState('never');
        expect(component.expanded.b).toBe(10);
        // setExpandRow is called to reset the expanded state.
        // Commeting out the line below because this might be reversed on next iteration
        // expect(component.setExpandedRow.emit).not.toHaveBeenCalled();
      });
    });

    it('should open the table details and close other expanded rows', () => {
      component.data = [{ a: 1, b: 10, c: true }];
      component.useData();
      component.ngAfterViewInit();
      fixture.detectChanges();
      component.toggleExpandRow();
      expect(component.expanded).toEqual({ a: 1, b: 10, c: true });
      expect(component.model.rowsExpanded.every((x) => x)).toBeTruthy();
    });

    it('should close the current table details expansion', () => {
      component.useData();
      component.model.rowsExpanded = component.model.rowsIndices.map((_) => false);
      component.model.rowsIndices.forEach((i) => component.model.expandRow(i, false));
      expect(component.expanded).toBeUndefined();
      expect(component.setExpandedRow.emit).toHaveBeenCalledWith(undefined);
      expect(component.model.rowsExpanded.every((x) => x)).toBeFalsy();
    });

    it('should not select the row when the row is expanded', () => {
      expect(component.selection.selected).toEqual([]);
      component.toggleExpandRow();
      expect(component.selection.selected).toEqual([]);
    });

    it('should not change selection when expanding different row', () => {
      component.useData();
      expect(component.selection.selected).toEqual([]);
      expect(component.expanded).toEqual(component.data[1]);
      component.selection.selected = [component.data[2]];
      component.model.rowsExpanded = component.model.rowsIndices.map((i) => i === 3);
      component.model.expandRow(3, true);
      expect(component.selection.selected).toEqual([component.data[2]]);
      expect(component.expanded).toEqual(component.data[3]);
    });
  });
});
