import { CdHelperClass } from './cd-helper.class';

class MockClass {
  n = 42;
  o = {
    x: 'something',
    y: [1, 2, 3],
    z: true
  };
  b: boolean;
}

describe('CdHelperClass', () => {
  describe('updateChanged', () => {
    let old: MockClass;
    let used: MockClass;
    let structure = {
      n: 42,
      o: {
        x: 'something',
        y: [1, 2, 3],
        z: true
      }
    } as any;

    beforeEach(() => {
      old = new MockClass();
      used = new MockClass();
      structure = {
        n: 42,
        o: {
          x: 'something',
          y: [1, 2, 3],
          z: true
        }
      };
    });

    it('should not update anything', () => {
      CdHelperClass.updateChanged(used, structure);
      expect(used).toEqual(old);
    });

    it('should only change n', () => {
      CdHelperClass.updateChanged(used, { n: 17 });
      expect(used.n).not.toEqual(old.n);
      expect(used.n).toBe(17);
    });

    it('should update o on change of o.y', () => {
      CdHelperClass.updateChanged(used, structure);
      structure.o.y.push(4);
      expect(used.o.y).toEqual(old.o.y);
      CdHelperClass.updateChanged(used, structure);
      expect(used.o.y).toEqual([1, 2, 3, 4]);
    });

    it('should change b, o and n', () => {
      structure.o.x.toUpperCase();
      structure.n++;
      structure.b = true;
      CdHelperClass.updateChanged(used, structure);
      expect(used).toEqual(structure);
    });
  });
});
