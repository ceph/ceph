import { Component, Input, SimpleChanges, OnChanges } from '@angular/core';
import { TopicDetailModel } from './topic-details.model';

@Component({
  selector: 'cd-rgw-topic-details',
  templateUrl: './rgw-topic-details.component.html',
  styleUrls: ['./rgw-topic-details.component.scss']
})
export class RgwTopicDetailsComponent implements OnChanges {
  @Input()
  selection: TopicDetailModel;
  policy: {};
  constructor() {}
  ngOnChanges(changes: SimpleChanges): void {
    if (changes['selection'] && this.selection) {
      if (typeof this.selection.policy === 'string') {
        this.policy = JSON.parse(this.selection.policy);
      } else {
        this.policy = {};
      }
    }
  }
}
