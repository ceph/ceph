import { PoolPageHelper } from '../pools/pools.po';
import { NotificationSidebarPageHelper } from './notification.po';

describe('Notification page', () => {
  let notification: NotificationSidebarPageHelper;
  let pools: PoolPageHelper;

  beforeAll(() => {
    notification = new NotificationSidebarPageHelper();
    pools = new PoolPageHelper();
  });

  afterEach(async () => {
    NotificationSidebarPageHelper.checkConsole();

    if (await notification.getCloseBtn().isPresent()) {
      await notification.waitClickableAndClick(notification.getCloseBtn());
      await notification.waitStaleness(notification.getSidebar());
    }
  });

  it('should open notification sidebar', async () => {
    await notification.waitStaleness(notification.getSidebar());
    await notification.open();
    await notification.waitVisibility(notification.getSidebar());
  });

  it('should display a running task', async () => {
    const poolName = 'e2e_notification_pool';

    await pools.navigateTo('create');
    await pools.create(poolName, 16);
    await pools.edit_pool_pg(poolName, 8, false);

    await notification.open();
    await notification.waitVisibility(notification.getTasks().first());
    await expect((await notification.getTasks()).length).toBeGreaterThan(0);

    await pools.delete(poolName);
  });

  it('should have notifications', async () => {
    await notification.open();
    await expect((await notification.getNotifications()).length).toBeGreaterThan(0);
  });

  it('should clear notifications', async () => {
    await notification.open();
    await expect((await notification.getNotifications()).length).toBeGreaterThan(0);
    await notification.waitClickableAndClick(notification.getClearNotficationsBtn());
    await notification.waitStaleness(notification.getNotifications().first());
    await expect((await notification.getNotifications()).length).toBe(0);
  });
});
