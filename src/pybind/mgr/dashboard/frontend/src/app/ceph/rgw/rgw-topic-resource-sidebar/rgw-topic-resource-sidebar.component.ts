import { Component, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import { Subscription } from 'rxjs';

import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { SidebarItem } from '~/app/shared/components/sidebar-layout/sidebar-layout.component';
import { Topic } from '~/app/shared/models/topic.model';
import { RgwTopicKeyService } from '~/app/shared/services/rgw-topic-key.service';

@Component({
  selector: 'cd-rgw-topic-resource-sidebar',
  templateUrl: './rgw-topic-resource-sidebar.component.html',
  styleUrls: ['./rgw-topic-resource-sidebar.component.scss'],
  encapsulation: ViewEncapsulation.None,
  standalone: false
})
export class RgwTopicResourceSidebarComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  topicKey = '';
  topicName = '';
  selection: Topic | undefined;
  sidebarItems: SidebarItem[] = [];

  constructor(
    private route: ActivatedRoute,
    private rgwTopicService: RgwTopicService,
    private rgwTopicKeyService: RgwTopicKeyService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.paramMap.subscribe((pm: ParamMap) => {
        this.topicKey = pm.get('name') ?? '';
        this.buildSidebarItems();
        this.loadTopic();
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private buildSidebarItems(): void {
    this.sidebarItems = [
      {
        label: $localize`Overview`,
        route: ['/rgw/destination', this.topicKey, 'overview'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Policies`,
        route: ['/rgw/destination', this.topicKey, 'policies'],
        routerLinkActiveOptions: { exact: true }
      },
      {
        label: $localize`Subscribed buckets`,
        route: ['/rgw/destination', this.topicKey, 'subscribed-buckets'],
        routerLinkActiveOptions: { exact: true }
      }
    ];
  }

  private loadTopic(): void {
    if (!this.topicKey) {
      this.selection = undefined;
      this.topicName = '';
      return;
    }

    const key = this.rgwTopicKeyService.decodeTopicKey(this.topicKey);

    this.sub.add(
      this.rgwTopicService.getTopic(key).subscribe({
        next: (topic: Topic) => {
          this.selection = topic;
          this.topicName = topic?.name || this.rgwTopicKeyService.extractTopicName(key);
        },
        error: () => {
          this.selection = undefined;
          this.topicName = this.rgwTopicKeyService.extractTopicName(key);
        }
      })
    );
  }
}
