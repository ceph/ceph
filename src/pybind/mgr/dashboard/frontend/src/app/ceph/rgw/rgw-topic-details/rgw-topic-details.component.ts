import { Component, Input, SimpleChanges, OnChanges } from '@angular/core';

import { TopicDetails } from '~/app/shared/models/topic.model';
import * as _ from 'lodash';

@Component({
  selector: 'cd-rgw-topic-details',
  templateUrl: './rgw-topic-details.component.html',
  styleUrls: ['./rgw-topic-details.component.scss']
})
export class RgwTopicDetailsComponent implements OnChanges {
  @Input()
  selection: TopicDetails;
  policy: string;
  constructor() {}
  ngOnChanges(changes: SimpleChanges): void {
    if (changes['selection'] && this.selection) {
      if (_.isString(this.selection.policy)) {
        try {
          this.policy = JSON.parse(this.selection.policy);
        } catch (e) {
          this.policy = '{}';
        }
      } else {
        this.policy = this.selection.policy || {};
      }
    }
  }
}
