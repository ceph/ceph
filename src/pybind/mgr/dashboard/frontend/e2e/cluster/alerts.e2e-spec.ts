import { AlertsPageHelper } from './alerts.po';

describe('Alerts page', () => {
  let alerts: AlertsPageHelper;

  beforeAll(() => {
    alerts = new AlertsPageHelper();
  });

  afterEach(async () => {
    await AlertsPageHelper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await alerts.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await alerts.waitTextToBePresent(alerts.getBreadcrumb(), 'Alerts');
    });
  });
});
