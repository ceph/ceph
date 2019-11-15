import { Helper } from '../helper.po';
import { AlertsPage } from './alerts.po';

describe('Alerts page', () => {
  let page: AlertsPage;

  beforeAll(() => {
    page = new AlertsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      page.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      Helper.waitTextToBePresent(Helper.getBreadcrumb(), 'Alerts');
    });
  });
});
