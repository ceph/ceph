import { Component, OnDestroy, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';
import _ from 'lodash';
import { Subscription } from 'rxjs';

import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { OverviewField } from '~/app/shared/components/resource-overview-card/resource-overview-card.component';
import { Topic } from '~/app/shared/models/topic.model';
import { RgwTopicKeyService } from '~/app/shared/services/rgw-topic-key.service';

@Component({
  selector: 'cd-rgw-topic-resource-page',
  templateUrl: './rgw-topic-resource-page.component.html',
  standalone: false
})
export class RgwTopicResourcePageComponent implements OnInit, OnDestroy {
  private sub = new Subscription();

  section = '';
  topic!: Topic;
  hasTopic = false;
  loading = false;
  loadError = false;
  topicOverviewFields: OverviewField[] = [];
  policy: string | object = '{}';

  constructor(
    private route: ActivatedRoute,
    private rgwTopicService: RgwTopicService,
    private rgwTopicKeyService: RgwTopicKeyService
  ) {}

  ngOnInit(): void {
    this.sub.add(
      this.route.data.subscribe((data) => {
        this.section = data['section'] ?? '';
      })
    );

    this.sub.add(
      this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
        this.loadTopic(pm.get('name') ?? '');
      })
    );
  }

  ngOnDestroy(): void {
    this.sub.unsubscribe();
  }

  private loadTopic(topicKeyParam: string): void {
    if (!topicKeyParam) {
      this.hasTopic = false;
      this.loading = false;
      this.loadError = false;
      this.topicOverviewFields = [];
      this.policy = {};
      return;
    }

    const topicKey = this.rgwTopicKeyService.decodeTopicKey(topicKeyParam);

    this.loading = true;
    this.loadError = false;

    this.sub.add(
      this.rgwTopicService.getTopic(topicKey).subscribe({
        next: (topic: Topic) => {
          this.topic = topic;
          this.hasTopic = true;
          this.topicOverviewFields = this.buildOverviewSections(topic);
          this.policy = this.parsePolicy(topic?.policy);
          this.loading = false;
        },
        error: () => {
          this.hasTopic = false;
          this.topicOverviewFields = [];
          this.policy = {};
          this.loading = false;
          this.loadError = true;
        }
      })
    );
  }

  private buildOverviewSections(topic?: Topic): OverviewField[] {
    const details = topic ?? ({} as Topic);

    return [
      {
        label: $localize`Name`,
        value: details?.name
      },
      {
        label: $localize`Owner`,
        value: details?.owner
      },
      {
        label: $localize`Amazon resource name`,
        value: details?.arn
      },
      {
        label: $localize`Push endpoint`,
        value: details?.dest?.push_endpoint
      },
      {
        label: $localize`Push endpoint arguments`,
        value: details?.dest?.push_endpoint_args
      },
      {
        label: $localize`Stored secret`,
        value: details?.dest?.stored_secret
      },
      {
        label: $localize`Persistent`,
        value: details?.dest?.persistent
      },
      {
        label: $localize`Persistent queue`,
        value: details?.dest?.persistent_queue
      },
      {
        label: $localize`Time to live`,
        value: details?.dest?.time_to_live
      },
      {
        label: $localize`Max retries`,
        value: details?.dest?.max_retries
      },
      {
        label: $localize`Retry sleep duration`,
        value: details?.dest?.retry_sleep_duration
      },
      {
        label: $localize`Opaque data`,
        value: details?.opaqueData
      }
    ];
  }

  private parsePolicy(policy: string | object): string | object {
    if (_.isString(policy)) {
      try {
        return JSON.parse(policy);
      } catch {
        return '{}';
      }
    }

    return policy || {};
  }
}
