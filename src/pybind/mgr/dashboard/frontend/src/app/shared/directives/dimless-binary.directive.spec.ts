import { DimlessBinaryDirective } from './dimless-binary.directive';

export class MockElementRef {
  nativeElement: {};
}

describe('DimlessBinaryDirective', () => {
  it('should create an instance', () => {
    const directive = new DimlessBinaryDirective(new MockElementRef(), null, null, null);
    expect(directive).toBeTruthy();
  });
});
