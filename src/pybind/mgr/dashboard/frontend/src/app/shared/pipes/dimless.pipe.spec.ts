import { FormatterService } from '../services/formatter.service';
import { DimlessPipe } from './dimless.pipe';

describe('DimlessPipe', () => {
  it('create an instance', () => {
    const formatterService = new FormatterService();
    const pipe = new DimlessPipe(formatterService);
    expect(pipe).toBeTruthy();
  });
});
