import { Helper } from '../helper.po';
import { AlertsPageHelper } from './alerts.po';

describe('Alerts page', () => {
  let alerts: AlertsPageHelper;

  beforeAll(() => {
    alerts = new AlertsPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await alerts.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await expect(alerts.getBreadcrumbText()).toEqual('Alerts');
    });
  });
});
