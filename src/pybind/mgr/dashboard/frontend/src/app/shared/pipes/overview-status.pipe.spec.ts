import { describe, expect, it } from '@jest/globals';

import { ICON_TYPE } from '~/app/shared/enum/icons.enum';
import { OverviewStatusPipe } from './overview-status.pipe';

describe('OverviewStatusPipe', () => {
  const pipe = new OverviewStatusPipe();

  it('create an instance', () => {
    expect(pipe).toBeTruthy();
  });

  it('maps success status to correct icon and text class', () => {
    expect(pipe.transform('success')).toEqual({
      icon: ICON_TYPE.success,
      textClass: 'cd-status-text--success'
    });
  });

  it('maps warning status to correct icon and text class', () => {
    expect(pipe.transform('warning')).toEqual({
      icon: ICON_TYPE.warning,
      textClass: 'cd-status-text--warning'
    });
  });

  it('maps danger status to correct icon and text class', () => {
    expect(pipe.transform('danger')).toEqual({
      icon: ICON_TYPE.danger,
      textClass: 'cd-status-text--danger'
    });
  });

  it('falls back to info mappings for info, undefined, null, and unknown statuses', () => {
    const expectedInfoMeta = {
      icon: ICON_TYPE.infoCircle,
      textClass: 'cd-status-text--info'
    };

    expect(pipe.transform('info')).toEqual(expectedInfoMeta);
    expect(pipe.transform(undefined)).toEqual(expectedInfoMeta);
    expect(pipe.transform(null)).toEqual(expectedInfoMeta);

    // Using 'as any' here to intentionally test bad inputs that bypass TypeScript
    expect(pipe.transform('invalid_status' as any)).toEqual(expectedInfoMeta);
  });
});
