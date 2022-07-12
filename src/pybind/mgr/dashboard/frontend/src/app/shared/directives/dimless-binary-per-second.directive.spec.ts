import { DimlessBinaryPerSecondDirective } from './dimless-binary-per-second.directive';

export class MockElementRef {
  nativeElement: {};
}

describe('DimlessBinaryPerSecondDirective', () => {
  it('should create an instance', () => {
    const directive = new DimlessBinaryPerSecondDirective(new MockElementRef(), null, null, null);
    expect(directive).toBeTruthy();
  });
});
