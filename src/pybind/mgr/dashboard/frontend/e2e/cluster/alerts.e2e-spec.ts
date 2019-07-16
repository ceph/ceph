import { Helper } from '../helper.po';

describe('Alerts page', () => {
  let alerts;

  beforeAll(() => {
    alerts = new Helper().alerts;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      alerts.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(alerts.getBreadcrumbText()).toEqual('Alerts');
    });
  });
});
