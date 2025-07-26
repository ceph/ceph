import { AfterViewChecked, Component, ElementRef, OnInit, ViewChild } from '@angular/core';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { UntypedFormControl, Validators } from '@angular/forms';
import * as _ from 'lodash';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ActivatedRoute, Router } from '@angular/router';
import { NotificationService } from '~/app/shared/services/notification.service';

import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import {
  AMQP_ACK_LEVEL,
  TopicRequest,
  END_POINT_TYPE,
  KAFKA_ACK_LEVEL,
  KAFKA_MECHANISM,
  Topic,
  URLPort,
  HostURLProtocol,
  URL_FORMAT_PLACEHOLDERS,
  UrlProtocol,
  Endpoint
} from '~/app/shared/models/topic.model';

const BASE_URL = 'rgw/topic';
@Component({
  selector: 'cd-rgw-topic-form',
  templateUrl: './rgw-topic-form.component.html',
  styleUrls: ['./rgw-topic-form.component.scss']
})
export class RgwTopicFormComponent extends CdForm implements OnInit, AfterViewChecked {
  @ViewChild('topicPolicyTextArea')
  public topicPolicyTextArea: ElementRef<any>;
  topicForm: CdFormGroup;
  action: string;
  resource: string;
  endpointType: string[] = [];
  ackLevels: string[] = [];
  selectedOption: string;
  port: string;
  owners: string[];
  vhost: string;
  kafkaMechanism: string[] = [];
  editing: boolean = false;
  topicId: string;
  hostProtocols: typeof Endpoint = Endpoint;
  key: string = '';
  protocolPlaceholders: Record<string, string> = {
    HTTP: URL_FORMAT_PLACEHOLDERS.http,
    AMQP: URL_FORMAT_PLACEHOLDERS.amqp,
    KAFKA: URL_FORMAT_PLACEHOLDERS.kafka
  };

  constructor(
    public actionLabels: ActionLabelsI18n,
    private textAreaJsonFormatterService: TextAreaJsonFormatterService,
    public rgwTopicService: RgwTopicService,
    private rgwUserService: RgwUserService,
    public notificationService: NotificationService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    super();
    this.editing = this.router.url.startsWith(`/rgw/topic/${URLVerbs.EDIT}`);
    this.action = this.editing ? this.actionLabels.EDIT : this.actionLabels.CREATE;
    this.resource = $localize`topic`;
  }

  ngAfterViewChecked(): void {
    this.textAreaOnChange(this.topicPolicyTextArea);
  }

  ngOnInit(): void {
    this.endpointType = Object.values(END_POINT_TYPE);
    this.createForm();
    this.rgwUserService.enumerate().subscribe((data: string[]) => {
      this.owners = data.sort();
      if (this.editing) {
        this.topicId = this.route.snapshot.paramMap.get('name');
        this.topicId = decodeURIComponent(this.topicId);
        this.loadTopicData(this.topicId);
      } else {
        this.loadingReady();
      }
    });

    this.topicForm.get('user')?.valueChanges.subscribe(() => this.setMechanism());
    this.topicForm.get('password')?.valueChanges.subscribe(() => this.setMechanism());
    this.topicForm.get('endpointType')?.valueChanges.subscribe((option: string) => {
      this.ackLevels = Object.values(option === Endpoint.AMQP ? AMQP_ACK_LEVEL : KAFKA_ACK_LEVEL);
    });
    this.kafkaMechanism = Object.values(KAFKA_MECHANISM);

    this.setMechanism();

    this.topicForm.get('enable_ssl')!.valueChanges.subscribe((enabled: boolean) => {
      const verifySSLControl = this.topicForm.get('verify_ssl');
      const useSSLControl = this.topicForm.get('use_ssl');
      if (enabled) {
        verifySSLControl!.enable();
        useSSLControl!.enable();
      } else {
        verifySSLControl!.disable();
        useSSLControl!.disable();
        useSSLControl!.setValue(false);
        verifySSLControl!.setValue(false);
      }
    });
  }

  loadTopicData(topicId: string) {
    this.rgwTopicService.getTopic(topicId).subscribe((topic: Topic) => {
      this.topicForm.get('name')?.disable();
      let url = topic.dest.push_endpoint;
      let hostname = url.split('://')[0];
      let endpointType: string;
      if (hostname === HostURLProtocol.amqp || hostname === HostURLProtocol.amqps) {
        endpointType = Endpoint.AMQP;
      } else if (hostname === HostURLProtocol.https || hostname === HostURLProtocol.http) {
        endpointType = Endpoint.HTTP;
      } else {
        endpointType = Endpoint.KAFKA;
      }
      this.selectedOption = endpointType;
      this.getPushUrlQueryValues(topic);
      this.topicForm.get('owner').disable();
      this.loadingReady();
    });
  }

  get pushEndpointPlaceholder(): string {
    return (
      this.protocolPlaceholders[this.topicForm.get('endpointType')?.value] ||
      $localize`Enter endpoint URL`
    );
  }

  createForm() {
    this.topicForm = new CdFormGroup({
      owner: new UntypedFormControl('', { validators: [Validators.required] }),
      name: new UntypedFormControl(
        '',
        [Validators.required],
        CdValidators.unique(this.rgwTopicService.exists, this.rgwTopicService)
      ),
      push_endpoint: new UntypedFormControl(
        { value: '', disabled: true },
        { validators: [Validators.required] }
      ),
      OpaqueData: new UntypedFormControl(''),
      persistent: new UntypedFormControl('0'),
      max_retries: new UntypedFormControl('0'),
      time_to_live: new UntypedFormControl('0'),
      retry_sleep_duration: new UntypedFormControl('0'),
      policy: new UntypedFormControl('{}', CdValidators.json()),
      endpointType: new UntypedFormControl('', { validators: [Validators.required] }),
      port: new UntypedFormControl('', {
        validators: [Validators.required, Validators.pattern('^[0-9]+$')]
      }),
      verify_ssl: new UntypedFormControl(true),
      enable_ssl: new UntypedFormControl(true),
      cloud_events: new UntypedFormControl(),
      user: new UntypedFormControl(),
      password: new UntypedFormControl(),
      vhost: new UntypedFormControl(),
      ca_location: new UntypedFormControl(),
      amqp_exchange: new UntypedFormControl(),
      ack_level: new UntypedFormControl(),
      use_ssl: new UntypedFormControl(false),
      kafka_brokers: new UntypedFormControl(),
      mechanism: new UntypedFormControl(),
      fqdn: new UntypedFormControl('', { validators: [Validators.required] })
    });
  }

  onEndpointTypeChange() {
    this.selectedOption = this.topicForm.get('endpointType').value;
    const secureSslChecked = this.topicForm.get('enable_ssl')?.value;
    this.vhost = '/';
    this.setDefaultPort(secureSslChecked, this.selectedOption);
    this.generatePushEndpoint(secureSslChecked);
    this.reset();
  }

  setDefaultPort(enableSSL: boolean, selectedValue: string) {
    this.port = this.getPort(selectedValue as HostURLProtocol, enableSSL).toString();
    this.topicForm.patchValue({ port: this.port });
  }

  onSecureSSLChange(event: any) {
    const ssl = !!event;
    this.port = this.getPort(this.selectedOption as HostURLProtocol, ssl).toString();
    this.topicForm.patchValue({ port: this.port });
    this.generatePushEndpoint(ssl);
  }

  private getPort(protocol: HostURLProtocol, ssl: boolean): number {
    const map = {
      [Endpoint.HTTP]: [URLPort.HTTP, URLPort.HTTPS],
      [Endpoint.AMQP]: [URLPort.AMQP, URLPort.AMQPS],
      [Endpoint.KAFKA]: [URLPort.KAFKA, URLPort.KAFKA_SSL]
    };
    return ssl ? map[protocol][1] : map[protocol][0];
  }

  textAreaOnChange(textArea: ElementRef<any>) {
    this.textAreaJsonFormatterService.format(textArea);
  }
  setMechanism(): void {
    const user = this.topicForm.get('user')?.value;
    const password = this.topicForm.get('password')?.value;
    const mechanismControl = this.topicForm.get('mechanism');
    let defaultMechanism = '';
    if (user && password) {
      defaultMechanism = KAFKA_MECHANISM.PLAIN;
    }
    mechanismControl?.setValue(defaultMechanism);
  }

  generatePushEndpoint(secureSsl?: boolean) {
    if (!this.selectedOption) {
      return;
    }
    let generatedEndpoint = '';
    const ssl = secureSsl !== undefined ? secureSsl : this.topicForm.get('enable_ssl')?.value;
    const fqdn = this.topicForm.get('fqdn')?.value || '<fqdn>';
    const port = this.topicForm.get('port')?.value || '[:port]';
    switch (this.selectedOption) {
      case Endpoint.HTTP:
        generatedEndpoint = `http${ssl ? 's' : ''}://${fqdn}:${port}`;
        break;
      case Endpoint.AMQP:
        generatedEndpoint = this.generateAMQPEndpoint(port, fqdn, ssl ? 's' : '');

        break;
      case Endpoint.KAFKA:
        generatedEndpoint = this.generateKafkaEndpoint(fqdn, port);
        break;
      default:
        generatedEndpoint = '';
    }
    if (generatedEndpoint) {
      this.topicForm.patchValue({ push_endpoint: generatedEndpoint });
    }
  }

  generateAMQPEndpoint(port: string, fqdn: string, ssl: string): string {
    let generatedEndpoint;
    const userAmqp = this.topicForm.get('user')?.value;
    const passwordAmqp = this.topicForm.get('password')?.value;
    const vhostAmqp = this.topicForm.get('vhost')?.value || '/';
    generatedEndpoint = `amqp${ssl ? 's' : ''}://${fqdn}:${port}${vhostAmqp}`;
    if (userAmqp && passwordAmqp) {
      generatedEndpoint = `amqp${
        ssl ? 's' : ''
      }://${userAmqp}:${passwordAmqp}@${fqdn}:${port}${vhostAmqp}`;
    }
    return generatedEndpoint;
  }

  generateKafkaEndpoint(fqdn: string, port: string): string {
    let generatedEndpoint;
    const kafkaProtocol = HostURLProtocol.kafka;
    const userKafka = this.topicForm.get('user')?.value;
    const passwordKafka = this.topicForm.get('password')?.value;
    const kafkaBrokers = this.topicForm.get('kafka_brokers')?.value;
    generatedEndpoint = `${kafkaProtocol}://${fqdn}:${port}`;
    if (userKafka && passwordKafka) {
      generatedEndpoint = `${kafkaProtocol}://${userKafka}:${passwordKafka}@${fqdn}:${port}`;
    } else if (kafkaBrokers) {
      generatedEndpoint = `kafka://${kafkaBrokers}`;
    } else {
      generatedEndpoint = `kafka://${fqdn}:${port}`;
    }

    return generatedEndpoint;
  }

  getTopicPolicy() {
    return this.topicForm.getValue('policy') || '{}';
  }

  getPushUrlQueryValues(topic: Topic) {
    let url = topic.dest.push_endpoint;
    let pushEndpointUrl = this.convertUrlToObject(url);
    let pushendpointArg = topic.dest.push_endpoint_args;
    const pushendpointAddarg = this.extractAdditionalValues(pushendpointArg);
    const protocol = pushEndpointUrl.protocol?.toLowerCase();
    switch (protocol) {
      case UrlProtocol.AMQP:
      case UrlProtocol.AMQPS:
        this.selectedOption = Endpoint.AMQP;
        break;

      case UrlProtocol.HTTP:
      case UrlProtocol.HTTPS:
        this.selectedOption = Endpoint.HTTP;
        break;

      default:
        this.selectedOption = Endpoint.KAFKA;
        break;
    }

    const defaults: typeof this.topicForm.value = _.clone(this.topicForm.value);
    const keys = Object.keys(this.topicForm.value) as (keyof typeof topic)[];
    let value: Pick<typeof topic, typeof keys[number]> = _.pick(topic, keys);

    value = _.merge(defaults, value);
    if (!this.owners.includes(value['owner'])) {
      this.owners.push(value['owner']);
      this.topicForm.get('owner').disable();
    }
    this.topicForm.patchValue({ endpointType: this.selectedOption });
    this.topicForm.patchValue({
      name: topic.name,
      owner: topic.owner,
      push_endpoint: topic.dest.push_endpoint,
      OpaqueData: topic.opaqueData,
      persistent: topic.dest.persistent,
      max_retries: topic.dest.max_retries,
      time_to_live: topic.dest.time_to_live,
      retry_sleep_duration: topic.dest.retry_sleep_duration,
      policy: topic.policy,
      port: pushEndpointUrl.port,
      fqdn: pushEndpointUrl.hostname,
      vhost: pushEndpointUrl.pathname,
      user: pushEndpointUrl.username,
      password: pushEndpointUrl.password,
      ca_location: pushendpointAddarg.ca_location,
      mechanism: pushendpointAddarg.mechanism,
      enable_ssl:
        pushEndpointUrl.protocol === UrlProtocol.HTTPS ||
        pushEndpointUrl.protocol == UrlProtocol.AMQPS ||
        pushEndpointUrl.protocol === UrlProtocol.KAFKA
          ? true
          : false,
      verify_ssl: pushendpointAddarg.verify_ssl,
      cloud_events: pushendpointAddarg.cloud_events,
      amqp_exchange: pushendpointAddarg.amqp_exchange,
      ack_level: pushendpointAddarg.ack_level,
      use_ssl: pushendpointAddarg.use_ssl,
      kafka_brokers: pushendpointAddarg.kafka_brokers
    });
  }

  convertUrlToObject(url: string) {
    const urlObj = new URL(url);

    return {
      protocol: urlObj.protocol,
      hostname: urlObj.hostname,
      pathname: urlObj.pathname,
      hash: urlObj.hash,
      port: this.getPortFromUrl(url),
      username: urlObj.username,
      password: urlObj.password
    };
  }

  getPortFromUrl(url: string): string {
    const urlObj = new URL(url);
    let port = urlObj.port;
    if (!port) {
      port =
        urlObj.protocol === UrlProtocol.HTTPS
          ? URLPort.HTTPS
          : urlObj.protocol === UrlProtocol.HTTP
          ? URLPort.HTTP
          : '';
    }
    return port;
  }

  extractAdditionalValues(str: string): { [key: string]: string } {
    let obj: { [key: string]: string } = {};
    let pairs = str.split('&');
    pairs.forEach((pair) => {
      let [key, value] = pair.split('=');
      if (key && value) {
        obj[key] = value;
      }
    });
    return obj;
  }

  openUrl(url: string) {
    window.open(url, '_blank');
  }

  submitAction() {
    if (this.topicForm.invalid || this.topicForm.pending) {
      return this.topicForm.setErrors({ cdSubmitButton: true });
    }
    const notificationTitle = this.editing
      ? $localize`Topic updated successfully`
      : $localize`Topic created successfully`;
    const formValue = this.topicForm.getRawValue();
    const topicPolicy = this.getTopicPolicy();
    const payload = this.generatePayload(formValue, topicPolicy);

    const action = this.rgwTopicService.create(payload);

    action.subscribe({
      next: () => {
        this.notificationService.show(NotificationType.success, notificationTitle);
        this.goToListView();
      },
      error: () => this.topicForm.setErrors({ cdSubmitButton: true })
    });
  }

  generatePayload(formValue: any, topicPolicy: any): TopicRequest {
    const basePayload: TopicRequest = {
      name: formValue.name,
      owner: formValue.owner,
      push_endpoint: formValue.push_endpoint,
      opaque_data: formValue.OpaqueData,
      persistent: formValue.persistent,
      time_to_live: formValue.time_to_live,
      max_retries: formValue.max_retries,
      retry_sleep_duration: formValue.retry_sleep_duration,
      policy: topicPolicy
    };

    const topicType = formValue.endpointType;

    const additionalFieldsMap: Record<string, Partial<TopicRequest>> = {
      [Endpoint.KAFKA]: {
        use_ssl: formValue.use_ssl,
        ack_level: formValue.ack_level,
        kafka_brokers: formValue.kafka_brokers,
        ca_location: formValue.ca_location,
        mechanism: formValue.mechanism
      },
      [Endpoint.AMQP]: {
        verify_ssl: formValue.verify_ssl,
        amqp_exchange: formValue.amqp_exchange,
        ack_level: formValue.ack_level,
        ca_location: formValue.ca_location
      },
      [Endpoint.HTTP]: {
        verify_ssl: formValue.verify_ssl,
        cloud_events: formValue.cloud_events
      }
    };

    if (additionalFieldsMap[topicType]) {
      Object.assign(basePayload, additionalFieldsMap[topicType]);
    }

    return basePayload;
  }

  get attributeHelpText(): string {
    return this.selectedOption === this.hostProtocols.AMQP
      ? $localize`Choose the configuration settings for the AMQP connection`
      : $localize`Choose the configuration settings for the KAFKA connection`;
  }

  goToListView() {
    this.router.navigate([BASE_URL]);
  }

  clearTextArea(field: string, defaultValue: string = '') {
    this.topicForm.get(field)?.setValue(defaultValue);
    this.topicForm.markAsDirty();
    this.topicForm.updateValueAndValidity();
  }

  reset() {
    this.topicForm.patchValue({ enable_ssl: true });
    this.topicForm.patchValue({ port: this.port });
    this.topicForm.patchValue({ vhost: this.vhost });
  }
}
