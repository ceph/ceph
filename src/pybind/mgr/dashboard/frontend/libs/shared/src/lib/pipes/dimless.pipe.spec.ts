import { FormatterService } from '../services/formatter.service';
import { DimlessPipe } from './dimless.pipe';

describe('DimlessPipe', () => {
  it('create an instance', () => {
    const pipe = new DimlessPipe(new FormatterService());
    expect(pipe).toBeTruthy();
  });
});