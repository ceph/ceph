import { SilencesPageHelper } from './silences.po';

describe('Silences page', () => {
  let silences: SilencesPageHelper;
  const creator = 'silence-create-expire';

  beforeAll(() => {
    silences = new SilencesPageHelper();
  });

  afterEach(async () => {
    await SilencesPageHelper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await silences.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await silences.waitTextToBePresent(silences.getBreadcrumb(), 'Silences');
    });
  });

  describe('create and expire silence test', () => {
    beforeAll(async () => {
      await silences.navigateTo();
    });

    it('should create silence', async () => {
      await silences.create(creator, 'very interesting silence comment', 'alertname', 'load_0');
    });

    it('should expire silence', async () => {
      await silences.expire(creator);
    });
  });
});
