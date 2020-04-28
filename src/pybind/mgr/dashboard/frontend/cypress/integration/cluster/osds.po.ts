import { PageHelper } from '../page-helper.po';

export class OSDsPageHelper extends PageHelper {
  pages = { index: { url: '#/osd', id: 'cd-osd-list' } };
}
