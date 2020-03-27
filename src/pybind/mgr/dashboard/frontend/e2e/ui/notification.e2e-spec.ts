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
    await NotificationSidebarPageHelper.checkConsole();
  });

  it('should open notification sidebar', async () => {
    await notification.waitInvisibility(notification.getSidebar());
    await notification.open();
    await notification.waitVisibility(notification.getSidebar());
  });

  it('should display a running task', async () => {
    const poolName = 'e2e_notification_pool';

    await pools.navigateTo('create');
    await pools.create(poolName, 8);
    await pools.edit_pool_pg(poolName, 4, false);
    await notification.waitStaleness(notification.getToast());

    // Check that running task is shown.
    await notification.open();
    await notification.waitFn(async () => {
      const task = await notification.getTasks().first();
      const text = await task.getText();
      return text.includes(poolName);
    }, 'Timed out verifying task.');

    // Delete pool after task is complete (otherwise we get an error).
    await notification.waitFn(
      async () => {
        const tasks = await notification.getTasks();
        return tasks.length === 0 ? true : !(await tasks[0].getText()).includes(poolName);
      },
      'Timed out waiting for task to complete.',
      40000
    );
    await pools.delete(poolName);
  });

  it('should have notifications', async () => {
    await notification.open();
    await expect((await notification.getNotifications()).length).toBeGreaterThan(0);
  });

  it('should clear notifications', async () => {
    await notification.waitStaleness(notification.getToast());
    await expect((await notification.getNotifications()).length).toBeGreaterThan(0);
    await notification.waitVisibility(notification.getClearNotficationsBtn());

    // It can happen that although notifications are cleared, by the time we check the
    // notifications amount, another notification can appear, so we check it more than once (if needed).
    await notification.waitClickableAndClick(notification.getClearNotficationsBtn());
    await notification.waitFn(async () => {
      const notifications = await notification.getNotifications();
      if (notifications.length > 0) {
        await notification.waitClickableAndClick(notification.getClearNotficationsBtn());
        return false;
      }
      return true;
    }, 'Timed out checking that notifications are cleared.');
  });
});
