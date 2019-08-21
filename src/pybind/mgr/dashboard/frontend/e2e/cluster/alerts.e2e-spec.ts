import { Helper } from '../helper.po';
import { AlertsPageHelper } from './alerts.po';

describe('Alerts page', () => {
  let alerts: AlertsPageHelper;

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
