import { OverviewGridColumnNumbersPipe } from './overview-grid-column-numbers.pipe';

describe('OverviewGridColumnNumbersPipe', () => {
  const pipe = new OverviewGridColumnNumbersPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('should calculate correct columns for default (3 columns)', () => {
    expect(pipe.transform(3)).toEqual({ sm: 4, md: 4, lg: 5 });
  });

  it('should calculate correct columns for 1 column', () => {
    expect(pipe.transform(1)).toEqual({ sm: 4, md: 8, lg: 16 });
  });

  it('should calculate correct columns for 4 columns', () => {
    expect(pipe.transform(4)).toEqual({ sm: 4, md: 4, lg: 4 });
  });

  it('should clamp column values less than 1 to 1', () => {
    expect(pipe.transform(0)).toEqual({ sm: 4, md: 8, lg: 16 });
    expect(pipe.transform(-5)).toEqual({ sm: 4, md: 8, lg: 16 });
  });

  it('should clamp column values greater than 16 to 16', () => {
    expect(pipe.transform(20)).toEqual({ sm: 4, md: 4, lg: 1 });
  });
});
