import { Component, Input } from '@angular/core';

import * as _ from 'lodash';

import { OSDPreview } from '../../../../shared/models/osd-preview.interface';

@Component({
  selector: 'cd-osd-creation-preview-details',
  templateUrl: './osd-creation-preview-details.component.html',
  styleUrls: ['./osd-creation-preview-details.component.scss']
})
export class OsdCreationPreviewDetailsComponent {
  @Input()
  osd: OSDPreview;

  isEmpty(value: any) {
    return _.isEmpty(value);
  }
}
