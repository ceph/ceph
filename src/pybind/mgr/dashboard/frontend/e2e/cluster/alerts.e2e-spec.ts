import { Helper } from '../helper.po';

describe('Alerts page', () => {
  let alerts: Helper['alerts'];

  beforeAll(() => {
    alerts = new Helper().alerts;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await alerts.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await alerts.getBreadcrumbText()).toEqual('Alerts');
    });
  });
});
