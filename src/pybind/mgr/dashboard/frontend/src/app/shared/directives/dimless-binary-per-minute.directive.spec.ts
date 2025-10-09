import { DimlessBinaryPerMinuteDirective } from './dimless-binary-per-minute.directive';

export class MockElementRef {
  nativeElement: {};
}

describe('DimlessBinaryPerMinuteDirective', () => {
  it('should create an instance', () => {
    const directive = new DimlessBinaryPerMinuteDirective(new MockElementRef(), null, null, null);
    expect(directive).toBeTruthy();
  });
});
