import { FilterPipe } from './filter.pipe';

describe('FilterPipe', () => {
  const pipe = new FilterPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('filter words with "foo"', () => {
    const value = ['foo', 'bar', 'foobar'];
    const filters = [
      {
        value: 'foo',
        applyFilter: (row, val) => {
          return row.indexOf(val) !== -1;
        }
      }
    ];
    expect(pipe.transform(value, filters)).toEqual(['foo', 'foobar']);
  });

  it('filter words with "foo" and "bar"', () => {
    const value = ['foo', 'bar', 'foobar'];
    const filters = [
      {
        value: 'foo',
        applyFilter: (row, val) => {
          return row.indexOf(val) !== -1;
        }
      },
      {
        value: 'bar',
        applyFilter: (row, val) => {
          return row.indexOf(val) !== -1;
        }
      }
    ];
    expect(pipe.transform(value, filters)).toEqual(['foobar']);
  });

  it('filter with no value', () => {
    const value = ['foo', 'bar', 'foobar'];
    const filters = [
      {
        value: '',
        applyFilter: () => {
          return false;
        }
      }
    ];
    expect(pipe.transform(value, filters)).toEqual(['foo', 'bar', 'foobar']);
  });
});
