import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { RouterTestingModule } from '@angular/router/testing';

import { NgxDatatableModule } from '@swimlane/ngx-datatable';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ComponentsModule } from '../../components/components.module';
import { CellTemplate } from '../../enum/cell-template.enum';
import { CdTableColumn } from '../../models/cd-table-column';
import { TableComponent } from '../table/table.component';
import { TableKeyValueComponent } from './table-key-value.component';

describe('TableKeyValueComponent', () => {
  let component: TableKeyValueComponent;
  let fixture: ComponentFixture<TableKeyValueComponent>;

  configureTestBed({
    declarations: [TableComponent, TableKeyValueComponent],
    imports: [FormsModule, NgxDatatableModule, ComponentsModule, RouterTestingModule]
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
    component.data = [['someKey', 0], [3, 'something']];
    component.ngOnInit();
    expect(component.tableData.length).toBe(2);
    expect(component.tableData[0].key).toBe('someKey');
    expect(component.tableData[1].value).toBe('something');
  });

  it('should transform arrays', () => {
    component.data = [['someKey', [1, 2, 3]], [3, 'something']];
    component.ngOnInit();
    expect(component.tableData.length).toBe(2);
    expect(component.tableData[0].key).toBe('someKey');
    expect(component.tableData[0].value).toBe('1, 2, 3');
    expect(component.tableData[1].value).toBe('something');
  });

  it('should remove pure object values', () => {
    component.data = [[3, 'something'], ['will be removed', { a: 3, b: 4, c: 5 }]];
    component.ngOnInit();
    expect(component.tableData.length).toBe(1);
    expect(component.tableData[0].value).toBe('something');
  });

  it('makes key value object pairs out of an object', () => {
    component.data = {
      3: 'something',
      someKey: 0
    };
    component.ngOnInit();
    expect(component.tableData.length).toBe(2);
    expect(component.tableData[0].value).toBe('something');
    expect(component.tableData[1].key).toBe('someKey');
  });

  it('does nothing if data is correct', () => {
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

  it('throws errors if miss match', () => {
    component.data = 38;
    expect(() => component.ngOnInit()).toThrowError('Wrong data format');
    component.data = [['someKey', 0, 3]];
    expect(() => component.ngOnInit()).toThrowError('Wrong array format: [string, any][]');
    component.data = [{ somekey: 939, somethingElse: 'test' }];
  });

  describe('Class objects equal plain objects', () => {
    class Example {
      sth = 'something';
      deep?: Example;
      constructor(deep: boolean) {
        if (deep) {
          this.deep = new Example(false);
        }
      }
    }

    const classExample = new Example(true);
    const objectExample = {
      sth: 'something',
      deep: {
        sth: 'something'
      }
    };

    const getTableData = (data) => {
      component.data = data;
      expect(() => component.ngOnInit()).not.toThrow();
      return component.tableData;
    };

    const doesClassEqualsObject = (classData, objectData, dataLength) => {
      const classTableData = getTableData(classData);
      expect(classTableData).toEqual(getTableData(objectData));
      expect(classTableData.length).toBe(dataLength);
    };

    it('should convert class objects the same way as plain objects', () => {
      doesClassEqualsObject(classExample, objectExample, 1);
      doesClassEqualsObject([classExample], [objectExample], 1);
      component.renderObjects = true;
      doesClassEqualsObject(classExample, objectExample, 2);
      doesClassEqualsObject([classExample], [objectExample], 2);
    });
  });

  it('tests _makePairs', () => {
    expect(component._makePairs([['dash', 'board']])).toEqual([{ key: 'dash', value: 'board' }]);
    const pair = [{ key: 'dash', value: 'board' }, { key: 'ceph', value: 'mimic' }];
    const pairInverse = [{ key: 'ceph', value: 'mimic' }, { key: 'dash', value: 'board' }];
    expect(component._makePairs(pair)).toEqual(pairInverse);
    expect(component._makePairs({ dash: 'board' })).toEqual([{ key: 'dash', value: 'board' }]);
    expect(component._makePairs({ dash: 'board', ceph: 'mimic' })).toEqual(pairInverse);
  });

  it('tests _makePairsFromArray', () => {
    expect(component._makePairsFromArray([['dash', 'board']])).toEqual([
      { key: 'dash', value: 'board' }
    ]);
    const pair = [{ key: 'dash', value: 'board' }, { key: 'ceph', value: 'mimic' }];
    expect(component._makePairsFromArray(pair)).toEqual(pair);
  });

  it('tests _makePairsFromObject', () => {
    expect(component._makePairsFromObject({ dash: 'board' })).toEqual([
      { key: 'dash', value: 'board' }
    ]);
    expect(component._makePairsFromObject({ dash: 'board', ceph: 'mimic' })).toEqual([
      { key: 'dash', value: 'board' },
      { key: 'ceph', value: 'mimic' }
    ]);
  });

  describe('tests _convertValue', () => {
    const v = (value) => ({ key: 'sth', value: value });
    const testConvertValue = (value, result) =>
      expect(component._convertValue(v(value)).value).toBe(result);

    it('should leave a string as it is', () => {
      testConvertValue('something', 'something');
    });

    it('should leave an int as it is', () => {
      testConvertValue(29, 29);
    });

    it('should convert arrays with any type to string', () => {
      testConvertValue([1, 2, 3], '1, 2, 3');
      testConvertValue([{ sth: 'something' }], '{"sth":"something"}');
      testConvertValue([1, 'two', { 3: 'three' }], '1, two, {"3":"three"}');
    });

    it('should convert only allow objects if renderObjects is set to true', () => {
      expect(component._convertValue(v({ sth: 'something' }))).toBe(undefined);
      component.renderObjects = true;
      expect(component._convertValue(v({ sth: 'something' }))).toEqual(v({ sth: 'something' }));
    });
  });

  describe('render objects', () => {
    beforeEach(() => {
      component.data = {
        options: {
          someSetting1: 38,
          anotherSetting2: 'somethingElse',
          suboptions: {
            sub1: 12,
            sub2: 34,
            sub3: 56
          }
        },
        someKey: 0,
        o2: {
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
        { key: 'o2 sub1 x', value: 42 },
        { key: 'o2 sub2 y', value: 555 },
        { key: 'options anotherSetting2', value: 'somethingElse' },
        { key: 'options someSetting1', value: 38 },
        { key: 'options suboptions sub1', value: 12 },
        { key: 'options suboptions sub2', value: 34 },
        { key: 'options suboptions sub3', value: 56 },
        { key: 'someKey', value: 0 }
      ]);
    });

    it('without parent key', () => {
      component.appendParentKey = false;
      component.ngOnInit();
      expect(component.tableData).toEqual([
        { key: 'anotherSetting2', value: 'somethingElse' },
        { key: 'keyWithEmptyObject', value: '' },
        { key: 'someKey', value: 0 },
        { key: 'someSetting1', value: 38 },
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
        {
          prop: 'key',
          flexGrow: 1,
          cellTransformation: CellTemplate.bold
        },
        {
          prop: 'value',
          flexGrow: 3
        }
      ];
    });

    it('should have the following default column set up', () => {
      component.ngOnInit();
      expect(component.columns).toEqual(columns);
    });

    it('should have the following column set up if customCss is defined', () => {
      component.customCss = {
        'answer-of-everything': 42
      };
      component.ngOnInit();
      columns[1].cellTransformation = CellTemplate.classAdding;
      expect(component.columns).toEqual(columns);
    });
  });
});
