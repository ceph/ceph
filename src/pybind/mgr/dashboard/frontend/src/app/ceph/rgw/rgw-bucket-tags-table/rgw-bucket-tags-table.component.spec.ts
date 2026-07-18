import { RgwBucketTagsTableComponent } from './rgw-bucket-tags-table.component';
import { XmlService } from '~/app/shared/services/xml.service';

describe('RgwBucketTagsTableComponent', () => {
  let component: RgwBucketTagsTableComponent;

  beforeEach(() => {
    component = new RgwBucketTagsTableComponent({} as any, {} as any, {} as any, new XmlService());
  });

  it('should escape xml special characters in tags payload', () => {
    expect(component['tagsToXML']([{ key: 'k<&>"\'', value: 'v<&>"\'' }])).toBe(
      '<Tagging><TagSet><Tag><Key>k&lt;&amp;&gt;&quot;&apos;</Key><Value>v&lt;&amp;&gt;&quot;&apos;</Value></Tag></TagSet></Tagging>'
    );
  });
});
