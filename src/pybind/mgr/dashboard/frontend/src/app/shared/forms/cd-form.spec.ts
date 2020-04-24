import { CdForm, LoadingStatus } from './cd-form';

describe('CdForm', () => {
  let form: CdForm;

  beforeEach(() => {
    form = new CdForm();
  });

  describe('loading', () => {
    it('should start in loading state', () => {
      expect(form.loading).toBe(LoadingStatus.Loading);
    });

    it('should change to ready when calling loadingReady', () => {
      form.loadingReady();
      expect(form.loading).toBe(LoadingStatus.Ready);
    });

    it('should change to error state calling loadingError', () => {
      form.loadingError();
      expect(form.loading).toBe(LoadingStatus.Error);
    });

    it('should change to loading state calling loadingStart', () => {
      form.loadingError();
      expect(form.loading).toBe(LoadingStatus.Error);
      form.loadingStart();
      expect(form.loading).toBe(LoadingStatus.Loading);
    });
  });
});
