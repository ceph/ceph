import { FormatterService } from '../services/formatter.service';
import { DimlessBinaryPipe } from './dimless-binary.pipe';

describe('DimlessBinaryPipe', () => {
  it('create an instance', () => {
    const formatterService = new FormatterService();
    const pipe = new DimlessBinaryPipe(formatterService);
    expect(pipe).toBeTruthy();
  });
});
