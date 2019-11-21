import { PoolPageHelper } from '../pools/pools.po';
import { ImagesPageHelper } from './images.po';

describe('Images page', () => {
  let pools: PoolPageHelper;
  let images: ImagesPageHelper;

  beforeAll(() => {
    images = new ImagesPageHelper();
    pools = new PoolPageHelper();
  });

  afterEach(async () => {
    await ImagesPageHelper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await images.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await images.waitTextToBePresent(images.getBreadcrumb(), 'Images');
    });

    it('should show four tabs', async () => {
      await expect(images.getTabsCount()).toEqual(4);
    });

    it('should show text for all tabs', async () => {
      await expect(images.getTabText(0)).toEqual('Images');
      await expect(images.getTabText(1)).toEqual('Namespaces');
      await expect(images.getTabText(2)).toEqual('Trash');
      await expect(images.getTabText(3)).toEqual('Overall Performance');
    });
  });

  describe('create, edit & delete image test', () => {
    const poolName = 'e2e_images_pool';
    const imageName = 'e2e_images#image';
    const newImageName = 'e2e_images#image_new';

    beforeAll(async () => {
      await pools.navigateTo('create'); // Need pool for image testing
      await pools.create(poolName, 8, 'rbd');
      await pools.navigateTo();
      await pools.exist(poolName, true);
      await images.navigateTo();
    });

    it('should create image', async () => {
      await images.createImage(imageName, poolName, '1');
      await expect(images.getFirstTableCellWithText(imageName).isPresent()).toBe(true);
    });

    it('should edit image', async () => {
      await images.editImage(imageName, poolName, newImageName, '2');
      await expect(images.getFirstTableCellWithText(newImageName).isPresent()).toBe(true);
    });

    it('should delete image', async () => {
      await images.navigateTo();
      await images.delete(newImageName);
    });

    afterAll(async () => {
      await pools.navigateTo();
      await pools.delete(poolName);
    });
  });

  describe('move to trash, restore and purge image tests', () => {
    const poolName = 'trash_pool';
    const imageName = 'trash#image';
    const newImageName = 'newtrash#image';

    beforeAll(async () => {
      await pools.navigateTo('create'); // Need pool for image testing
      await pools.create(poolName, 8, 'rbd');
      await pools.navigateTo();
      await pools.exist(poolName, true);

      await images.navigateTo(); // Need image for trash testing
      await images.createImage(imageName, poolName, '1');
      await expect(images.getFirstTableCellWithText(imageName).isPresent()).toBe(true);
    });

    it('should move the image to the trash', async () => {
      await images.moveToTrash(imageName);
      await expect(images.getFirstTableCellWithText(imageName).isPresent()).toBe(true);
    });

    it('should restore image to images table', async () => {
      await images.restoreImage(imageName, newImageName);
      await expect(images.getFirstTableCellWithText(newImageName).isPresent()).toBe(true);
    });

    it('should purge trash in images trash tab', async () => {
      await images.navigateTo();
      // Have had issues with image not restoring fast enough, thus these tests/waits are here
      await images.waitPresence(
        images.getFirstTableCellWithText(newImageName),
        'Timed out waiting for image to restore'
      );
      await images.moveToTrash(newImageName);
      await images.purgeTrash(newImageName, poolName);
    });

    afterAll(async () => {
      await pools.navigateTo();
      await pools.delete(poolName); // Deletes images test pool
      await pools.navigateTo();
      await pools.exist(poolName, false);
    });
  });
});
