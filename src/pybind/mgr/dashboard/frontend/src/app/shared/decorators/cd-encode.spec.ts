import { cdEncode, cdEncodeNot } from './cd-encode';

describe('cdEncode', () => {
  @cdEncode
  class ClassA {
    x2: string;
    y2: string;

    methodA(x1: string, @cdEncodeNot y1: string) {
      this.x2 = x1;
      this.y2 = y1;
    }
  }

  class ClassB {
    x2: string;
    y2: string;

    @cdEncode
    methodB(x1: string, @cdEncodeNot y1: string) {
      this.x2 = x1;
      this.y2 = y1;
    }
  }

  const word = 'a+b/c-d';

  it('should encode all params of ClassA, with exception of y1', () => {
    const a = new ClassA();
    a.methodA(word, word);
    expect(a.x2).toBe('a%2Bb%2Fc-d');
    expect(a.y2).toBe(word);
  });

  it('should encode all params of methodB, with exception of y1', () => {
    const b = new ClassB();
    b.methodB(word, word);
    expect(b.x2).toBe('a%2Bb%2Fc-d');
    expect(b.y2).toBe(word);
  });
});
