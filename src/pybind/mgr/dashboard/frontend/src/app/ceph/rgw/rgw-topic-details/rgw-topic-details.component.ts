import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-rgw-topic-details',
  templateUrl: './rgw-topic-details.component.html',
  styleUrls: ['./rgw-topic-details.component.scss']
})
export class RgwTopicDetailsComponent {
  @Input()
  selection: any;
  tenant: string;
  topicname: string;
  constructor() {}
}
