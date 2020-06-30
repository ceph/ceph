import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbDropdownModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../components/components.module';
import { CellTemplate } from '../../enum/cell-template.enum';
import { CdTableColumn } from '../../models/cd-table-column';
import { CdDatePipe } from '../../pipes/cd-date.pipe';
import { PipesModule } from '../../pipes/pipes.module';
import { TableComponent } from '../table/table.component';
import { TableKeyValueComponent } from './table-key-value.component';

describe('TableKeyValueComponent', () => {
  let component: TableKeyValueComponent;
  let fixture: ComponentFixture<TableKeyValueComponent>;

  configureTestBed({
    declarations: [TableComponent, TableKeyValueComponent],
    imports: [
      FormsModule,
      NgxDatatableModule,
      ComponentsModule,
      RouterTestingModule,
      NgbDropdownModule,
      PipesModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TableKeyValueComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should make key value object pairs out of arrays with length two', () => {
    component.data = [
      ['someKey', 0],
      ['arrayKey', [1, 2, 3]],
      [3, 'something']
    ];
    component.ngOnInit();
    const expected: any = [
      { key: 'arrayKey', value: '1, 2, 3' },
      { key: 'someKey', value: 0 },
      { key: 3, value: 'something' }
    ];
    expect(component.tableData).toEqual(expected);
  });

  it('should not show data supposed to be have hidden by key', () => {
    component.data = [
      ['a', 1],
      ['b', 2]
    ];
    component.hideKeys = ['a'];
    component.ngOnInit();
    expect(component.tableData).toEqual([{ key: 'b', value: 2 }]);
  });

  it('should remove items with objects as values', () => {
    component.data = [
      [3, 'something'],
      ['will be removed', { a: 3, b: 4, c: 5 }]
    ];
    component.ngOnInit();
    expect(component.tableData).toEqual(<any>[{ key: 3, value: 'something' }]);
  });

  it('makes key value object pairs out of an object', () => {
    component.data = { 3: 'something', someKey: 0 };
    component.ngOnInit();
    expect(component.tableData).toEqual([
      { key: '3', value: 'something' },
      { key: 'someKey', value: 0 }
    ]);
  });

  it('does nothing if data does not need to be converted', () => {
    component.data = [
      { key: 3, value: 'something' },
      { key: 'someKey', value: 0 }
    ];
    component.ngOnInit();
    expect(component.tableData).toEqual(component.data);
  });

  it('throws errors if data cannot be converted', () => {
    component.data = 38;
    expect(() => component.ngOnInit()).toThrowError('Wrong data format');
    component.data = [['someKey', 0, 3]];
    expect(() => component.ngOnInit()).toThrowError(
      'Array contains too many elements (3). Needs to be of type [string, any][]'
    );
  });

  it('tests makePairs()', () => {
    const makePairs = (data: any) => component['makePairs'](data);
    expect(makePairs([['dash', 'board']])).toEqual([{ key: 'dash', value: 'board' }]);
    const pair = [
      { key: 'dash', value: 'board' },
      { key: 'ceph', value: 'mimic' }
    ];
    const pairInverse = [
      { key: 'ceph', value: 'mimic' },
      { key: 'dash', value: 'board' }
    ];
    expect(makePairs(pair)).toEqual(pairInverse);
    expect(makePairs({ dash: 'board' })).toEqual([{ key: 'dash', value: 'board' }]);
    expect(makePairs({ dash: 'board', ceph: 'mimic' })).toEqual(pairInverse);
  });

  it('tests makePairsFromArray()', () => {
    const makePairsFromArray = (data: any[]) => component['makePairsFromArray'](data);
    expect(makePairsFromArray([['dash', 'board']])).toEqual([{ key: 'dash', value: 'board' }]);
    const pair = [
      { key: 'dash', value: 'board' },
      { key: 'ceph', value: 'mimic' }
    ];
    expect(makePairsFromArray(pair)).toEqual(pair);
  });

  it('tests makePairsFromObject()', () => {
    const makePairsFromObject = (data: object) => component['makePairsFromObject'](data);
    expect(makePairsFromObject({ dash: 'board' })).toEqual([{ key: 'dash', value: 'board' }]);
    expect(makePairsFromObject({ dash: 'board', ceph: 'mimic' })).toEqual([
      { key: 'dash', value: 'board' },
      { key: 'ceph', value: 'mimic' }
    ]);
  });

  describe('tests convertValue()', () => {
    const convertValue = (data: any) => component['convertValue'](data);
    const expectConvertValue = (value: any, expectation: any) =>
      expect(convertValue(value)).toBe(expectation);

    it('should not convert strings', () => {
      expectConvertValue('something', 'something');
    });

    it('should not convert integers', () => {
      expectConvertValue(29, 29);
    });

    it('should convert arrays with any type to strings', () => {
      expectConvertValue([1, 2, 3], '1, 2, 3');
      expectConvertValue([{ sth: 'something' }], '{"sth":"something"}');
      expectConvertValue([1, 'two', { 3: 'three' }], '1, two, {"3":"three"}');
    });

    it('should only convert objects if renderObjects is set to true', () => {
      expect(convertValue({ sth: 'something' })).toBe(null);
      component.renderObjects = true;
      expect(convertValue({ sth: 'something' })).toEqual({ sth: 'something' });
    });
  });

  describe('automatically pipe UTC dates through cdDate', () => {
    let datePipe: CdDatePipe;

    beforeEach(() => {
      datePipe = TestBed.inject(CdDatePipe);
      spyOn(datePipe, 'transform').and.callThrough();
    });

    const expectTimeConversion = (date: string) => {
      component.data = { dateKey: date };
      component.ngOnInit();
      expect(datePipe.transform).toHaveBeenCalledWith(date);
      expect(component.tableData[0].key).not.toBe(date);
    };

    it('converts some date', () => {
      expectTimeConversion('2019-04-15 12:26:52.305285');
    });

    it('converts UTC date', () => {
      expectTimeConversion('2019-04-16T12:35:46.646300974Z');
    });
  });

  describe('render objects', () => {
    beforeEach(() => {
      component.data = {
        options: {
          numberKey: 38,
          stringKey: 'somethingElse',
          objectKey: {
            sub1: 12,
            sub2: 34,
            sub3: 56
          }
        },
        otherOptions: {
          sub1: {
            x: 42
          },
          sub2: {
            y: 555
          }
        },
        additionalKeyContainingObject: { type: 'none' },
        keyWithEmptyObject: {}
      };
      component.renderObjects = true;
    });

    it('with parent key', () => {
      component.ngOnInit();
      expect(component.tableData).toEqual([
        { key: 'additionalKeyContainingObject type', value: 'none' },
        { key: 'keyWithEmptyObject', value: '' },
        { key: 'options numberKey', value: 38 },
        { key: 'options objectKey sub1', value: 12 },
        { key: 'options objectKey sub2', value: 34 },
        { key: 'options objectKey sub3', value: 56 },
        { key: 'options stringKey', value: 'somethingElse' },
        { key: 'otherOptions sub1 x', value: 42 },
        { key: 'otherOptions sub2 y', value: 555 }
      ]);
    });

    it('without parent key', () => {
      component.appendParentKey = false;
      component.ngOnInit();
      expect(component.tableData).toEqual([
        { key: 'keyWithEmptyObject', value: '' },
        { key: 'numberKey', value: 38 },
        { key: 'stringKey', value: 'somethingElse' },
        { key: 'sub1', value: 12 },
        { key: 'sub2', value: 34 },
        { key: 'sub3', value: 56 },
        { key: 'type', value: 'none' },
        { key: 'x', value: 42 },
        { key: 'y', value: 555 }
      ]);
    });
  });

  describe('subscribe fetchData', () => {
    it('should not subscribe fetchData of table', () => {
      component.ngOnInit();
      expect(component.table.fetchData.observers.length).toBe(0);
    });

    it('should call fetchData', () => {
      let called = false;
      component.fetchData.subscribe(() => {
        called = true;
      });
      component.ngOnInit();
      expect(component.table.fetchData.observers.length).toBe(1);
      component.table.fetchData.emit();
      expect(called).toBeTruthy();
    });
  });

  describe('hide empty items', () => {
    beforeEach(() => {
      component.data = {
        booleanFalse: false,
        booleanTrue: true,
        string: '',
        array: [],
        object: {},
        emptyObject: {
          string: '',
          array: [],
          object: {}
        },
        someNumber: 0,
        someDifferentNumber: 1,
        someArray: [0, 1],
        someString: '0',
        someObject: {
          empty: {},
          something: 0.1
        }
      };
      component.renderObjects = true;
    });

    it('should show all items as default', () => {
      expect(component.hideEmpty).toBe(false);
      component.ngOnInit();
      expect(component.tableData).toEqual([
        { key: 'array', value: '' },
        { key: 'booleanFalse', value: false },
        { key: 'booleanTrue', value: true },
        { key: 'emptyObject array', value: '' },
        { key: 'emptyObject object', value: '' },
        { key: 'emptyObject string', value: '' },
        { key: 'object', value: '' },
        { key: 'someArray', value: '0, 1' },
        { key: 'someDifferentNumber', value: 1 },
        { key: 'someNumber', value: 0 },
        { key: 'someObject empty', value: '' },
        { key: 'someObject something', value: 0.1 },
        { key: 'someString', value: '0' },
        { key: 'string', value: '' }
      ]);
    });

    it('should hide all empty items', () => {
      component.hideEmpty = true;
      component.ngOnInit();
      expect(component.tableData).toEqual([
        { key: 'booleanFalse', value: false },
        { key: 'booleanTrue', value: true },
        { key: 'someArray', value: '0, 1' },
        { key: 'someDifferentNumber', value: 1 },
        { key: 'someNumber', value: 0 },
        { key: 'someObject something', value: 0.1 },
        { key: 'someString', value: '0' }
      ]);
    });
  });

  describe('columns set up', () => {
    let columns: CdTableColumn[];

    beforeEach(() => {
      columns = [
        { prop: 'key', flexGrow: 1, cellTransformation: CellTemplate.bold },
        { prop: 'value', flexGrow: 3 }
      ];
    });

    it('should have the following default column set up', () => {
      component.ngOnInit();
      expect(component.columns).toEqual(columns);
    });

    it('should have the following column set up if customCss is defined', () => {
      component.customCss = { 'class-name': 42 };
      component.ngOnInit();
      columns[1].cellTransformation = CellTemplate.classAdding;
      expect(component.columns).toEqual(columns);
    });
  });
});
