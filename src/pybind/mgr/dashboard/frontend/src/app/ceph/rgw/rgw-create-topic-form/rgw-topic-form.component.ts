import { AfterViewChecked, Component, ElementRef, OnInit, ViewChild } from '@angular/core';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { UntypedFormControl, Validators } from '@angular/forms';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ActivatedRoute, Router } from '@angular/router';
import { NotificationService } from '~/app/shared/services/notification.service';

import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { CdValidators } from '~/app/shared/forms/cd-validators';
import {
  AMQP_ACK_LEVEL,
  CreateTopic,
  END_POINT_TYPE,
  KAFKA_ACK_LEVEL,
  KAFKA_MECHANISM,
  Topic,
  URLPort,
  HostURLProtocol,
  URL_FORMAT_PLACEHOLDERS
} from '~/app/shared/models/topic.model';
import * as _ from 'lodash';

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
  hostProtocols = HostURLProtocol;
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
    this.rgwUserService.enumerate().subscribe((data: any) => {
      this.owners = (<string[]>data).sort();
      if (this.editing) {
        this.topicId = this.route.snapshot.paramMap.get('name');
        const decodedKey = decodeURIComponent(this.topicId);
        this.topicId = decodedKey;
        this.loadTopicData(this.topicId);
      } else {
        this.loadingReady();
      }
    });

    this.topicForm.get('user')?.valueChanges.subscribe(() => this.setMechanism());
    this.topicForm.get('password')?.valueChanges.subscribe(() => this.setMechanism());
    this.topicForm.get('endpointType')?.valueChanges.subscribe((option) => {
      this.ackLevels = Object.values(option === 'AMQP' ? AMQP_ACK_LEVEL : KAFKA_ACK_LEVEL);
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
        endpointType = HostURLProtocol.AMQP;
      } else if (hostname === HostURLProtocol.https || hostname === HostURLProtocol.http) {
        endpointType = HostURLProtocol.HTTP;
      } else {
        endpointType = HostURLProtocol.KAFKA;
      }
      this.selectedOption = endpointType;
      this.extractValues(topic);
      this.topicForm.get('owner').disable();
      this.loadingReady();
    });
  }

  get pushEndpointPlaceholder(): string {
    return (
      this.protocolPlaceholders[this.topicForm.get('endpointType')?.value] || 'Enter endpoint URL'
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
      persistent: new UntypedFormControl(''),
      max_retries: new UntypedFormControl(''),
      time_to_live: new UntypedFormControl(''),
      retry_sleep_duration: new UntypedFormControl(''),
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
  onSecureSslChange(event: any) {
    const ssl = !!event;
    this.port = this.getPort(this.selectedOption as HostURLProtocol, ssl).toString();
    this.topicForm.patchValue({ port: this.port });
    this.generatePushEndpoint(ssl);
  }
  private getPort(protocol: HostURLProtocol, ssl: boolean): number {
    const map = {
      [HostURLProtocol.HTTP]: [URLPort.HTTP, URLPort.HTTPS],
      [HostURLProtocol.AMQP]: [URLPort.AMQP, URLPort.AMQPS],
      [HostURLProtocol.KAFKA]: [URLPort.KAFKA, URLPort.KAFKA_SSL]
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
      defaultMechanism = 'PLAIN';
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
      case HostURLProtocol.HTTP:
        generatedEndpoint = `http${ssl ? 's' : ''}://${fqdn}:${port}`;
        break;
      case HostURLProtocol.AMQP:
        const userAmqp = this.topicForm.get('user')?.value;
        const passwordAmqp = this.topicForm.get('password')?.value;
        const vhostAmqp = this.topicForm.get('vhost')?.value || '/';
        generatedEndpoint = `amqp${ssl ? 's' : ''}://${fqdn}:${port}${vhostAmqp}`;
        if (userAmqp && passwordAmqp) {
          generatedEndpoint = `amqp${
            ssl ? 's' : ''
          }://${userAmqp}:${passwordAmqp}@${fqdn}:${port}${vhostAmqp}`;
        }
        break;
      case HostURLProtocol.KAFKA:
        const kafkaProtocol = 'kafka';
        const userKafka = this.topicForm.get('user')?.value;
        const passwordKafka = this.topicForm.get('password')?.value;
        const kafkaBrokers = this.topicForm.get('kafka_brokers')?.value;
        generatedEndpoint = `${kafkaProtocol}://${fqdn}:${port}`;
        if (userKafka && passwordKafka) {
          generatedEndpoint = `${kafkaProtocol}://${userKafka}:${passwordKafka}@${fqdn}:${port}`;
        }
        if (kafkaBrokers) {
          generatedEndpoint += kafkaBrokers;
        }
        break;
    }
    if (generatedEndpoint) {
      this.topicForm.patchValue({ push_endpoint: generatedEndpoint });
    }
  }

  getTopicPolicy() {
    return this.topicForm.getValue('policy') || '{}';
  }
  extractValues(topic: Topic) {
    let url = topic.dest.push_endpoint;
    let pushEndpointUrl = this.convertFullUrlToObject(url);
    if (pushEndpointUrl.protocol === 'amqp:' || pushEndpointUrl.protocol === 'amqps:') {
      this.selectedOption = HostURLProtocol.AMQP;
    } else if (pushEndpointUrl.protocol === 'https:' || pushEndpointUrl.protocol === 'http:') {
      this.selectedOption = HostURLProtocol.HTTP;
    } else {
      this.selectedOption = HostURLProtocol.KAFKA;
    }

    const defaults = _.clone(this.topicForm.value);
    let value: any = _.pick(topic, _.keys(this.topicForm.value));
    value = _.merge(defaults, value);
    if (!this.owners.includes(value['owner'])) {
      this.owners.push(value['owner']);
      this.topicForm.get('owner').disable();
    }
    this.topicForm.patchValue({ endpointType: this.selectedOption });
    let pushendpointArg = topic.dest.push_endpoint_args;
    const pushendpointAddarg = this.extractAdditionalValues(pushendpointArg);
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
        pushEndpointUrl.protocol === 'https:' ||
        pushEndpointUrl.protocol == 'amqps:' ||
        pushEndpointUrl.protocol === 'kafka'
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

  convertFullUrlToObject(url: string): any {
    const urlObj = new URL(url);
    let port = urlObj.port;
    if (!port) {
      port =
        urlObj.protocol === 'https:'
          ? URLPort.HTTPS
          : urlObj.protocol === 'http:'
          ? URLPort.HTTP
          : '';
    }
    return {
      protocol: urlObj.protocol,
      hostname: urlObj.hostname,
      pathname: urlObj.pathname,
      hash: urlObj.hash,
      port: port,
      username: urlObj.username,
      password: urlObj.password
    };
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

    const formValue = this.topicForm.getRawValue(),
      topicType = formValue.endpointType,
      topicPolicy = this.getTopicPolicy();
    let payload: CreateTopic = {
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

    if (topicType === HostURLProtocol.KAFKA) {
      Object.assign(payload, {
        use_ssl: formValue.use_ssl,
        ack_level: formValue.ack_level,
        kafka_brokers: formValue.kafka_brokers,
        ca_location: formValue.ca_location,
        mechanism: formValue.mechanism
      });
    } else if (topicType === HostURLProtocol.AMQP) {
      Object.assign(payload, {
        verify_ssl: formValue.verify_ssl,
        amqp_exchange: formValue.amqp_exchange,
        ack_level: formValue.ack_level,
        ca_location: formValue.ca_location
      });
    } else if (topicType === HostURLProtocol.HTTP) {
      Object.assign(payload, {
        verify_ssl: formValue.verify_ssl,
        cloud_events: formValue.cloud_events
      });
    }

    const notificationTitle = this.editing
      ? $localize`Topic updated successfully`
      : $localize`Topic created successfully`;

    const action = this.rgwTopicService.create(payload);

    action.subscribe({
      next: () => {
        this.notificationService.show(NotificationType.success, notificationTitle);
        this.goToListView();
      },
      error: () => this.topicForm.setErrors({ cdSubmitButton: true })
    });
  }

  get attributehelpText(): string {
    return this.selectedOption === this.hostProtocols.AMQP
      ? $localize`Choose the configuration settings for the AMQP connection`
      : $localize`Choose the configuration settings for the KAFKA connection`;
  }

  goToListView() {
    this.router.navigate([`rgw/topic`]);
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
