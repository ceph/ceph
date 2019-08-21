import { Helper } from '../helper.po';
import { DaemonsPageHelper } from './daemons.po';

describe('RGW daemons page', () => {
  let daemons: DaemonsPageHelper;

  beforeAll(() => {
    daemons = new Helper().daemons;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await daemons.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await daemons.getBreadcrumbText()).toEqual('Daemons');
    });

    it('should show two tabs', async () => {
      expect(await daemons.getTabsCount()).toEqual(2);
    });

    it('should show daemons list tab at first', async () => {
      expect(await daemons.getTabText(0)).toEqual('Daemons List');
    });

    it('should show overall performance as a second tab', () => {
      expect(daemons.getTabText(1)).toEqual('Overall Performance');
    });
  });

  describe('details and performance counters table tests', async () => {
    beforeAll(async () => {
      await daemons.navigateTo();
    });

    it('should check that details/performance tables are visible when daemon is selected', async () => {
      await daemons.checkTables();
    });
  });
});
