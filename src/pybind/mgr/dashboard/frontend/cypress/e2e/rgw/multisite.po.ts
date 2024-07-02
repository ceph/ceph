import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/rgw/multisite', id: 'cd-rgw-multisite-details' }
};
export class MultisitePageHelper extends PageHelper {
  pages = pages;
}
