import {
  AfterViewChecked,
  ChangeDetectorRef,
  Component,
  ElementRef,
  OnInit,
  ViewChild
} from '@angular/core';
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
  HostURLProtocol
} from '~/app/shared/models/topic.model';

@Component({
  selector: 'cd-rgw-create-topic-form',
  templateUrl: './rgw-create-topic-form.component.html',
  styleUrls: ['./rgw-create-topic-form.component.scss']
})
export class RgwCreateTopicFormComponent extends CdForm implements OnInit, AfterViewChecked {
  @ViewChild('topicPolicyTextArea')
  public topicPolicyTextArea: ElementRef<any>;

  topicForm: CdFormGroup;
  action: string;
  connect: string;
  resource: string;
  endpointType: string[] = [];
  amqpAckLevel: string[] = [];
  kafkaAckLevel: string[] = [];
  selectedOption: string;
  port: string;
  owners: string[];
  vhost: string;
  selectOwner: string;
  kafkaMechanism: string[] = [];
  editing: boolean = false;
  topicId: string;
  fqdn: string;
  constructor(
    public actionLabels: ActionLabelsI18n,
    private textAreaJsonFormatterService: TextAreaJsonFormatterService,
    private readonly changeDetectorRef: ChangeDetectorRef,
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
    this.changeDetectorRef.detectChanges();
    this.textAreaOnChange(this.topicPolicyTextArea);
  }

  ngOnInit(): void {
    this.connect = this.actionLabels.HEADER;
    this.endpointType = Object.values(END_POINT_TYPE);
    this.amqpAckLevel = Object.values(AMQP_ACK_LEVEL);
    this.kafkaAckLevel = Object.values(KAFKA_ACK_LEVEL);
    this.createForm();
    this.topicId = this.editing ? this.router.url.split('/').pop() : '';
    this.topicForm.get('user')?.valueChanges.subscribe(() => this.setMechanism());
    this.topicForm.get('password')?.valueChanges.subscribe(() => this.setMechanism());
    this.kafkaMechanism= Object.values(KAFKA_MECHANISM);
    if (this.editing) {
      this.topicId = this.route.snapshot.paramMap.get('name');
      this.loadTopicData(this.topicId);
    } else {
      this.loadingReady();
    }
    this.rgwUserService.enumerate().subscribe((data: any) => {
      this.owners = (<string[]>data).sort();
    });
    this.setMechanism();
  }
  loadTopicData(topicId: string) {
    this.rgwTopicService.getTopic(topicId).subscribe((topic: Topic) => {
      this.topicForm.get('name')?.disable();
      let url = topic.dest.push_endpoint;
      let hostname = url.split('://')[0];
      let endpointType: string;
      if (hostname === 'amqp' || 'amqps') {
        endpointType = HostURLProtocol.AMQP;
      } else if (hostname === 'https' || 'http') {
        endpointType = HostURLProtocol.HTTP;
      } else {
        endpointType = HostURLProtocol.KAFKA;
      }
      this.selectedOption = endpointType;
      this.extractValues(topic);
      this.loadingReady();
    });
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
      enable_ssl: new UntypedFormControl(true), // Default to SSL true for HTTP/AMQP
      cloud_events: new UntypedFormControl(),
      user: new UntypedFormControl(),
      password: new UntypedFormControl(),
      vhost: new UntypedFormControl(),
      ca_location: new UntypedFormControl(),
      amqp_exchange: new UntypedFormControl(),
      amqp_ack_level: new UntypedFormControl(),
      use_ssl: new UntypedFormControl(false), // Used for Kafka SSL option
      kafka_ack_level: new UntypedFormControl(),
      kafka_brokers: new UntypedFormControl(),
      mechanism: new UntypedFormControl(),
      fqdn: new UntypedFormControl('', { validators: [Validators.required] })
    });
  }

  // Handle select change to update endpoint
  onSelectChange(event: Event) {
    const select = event.target as HTMLSelectElement;
    this.selectedOption = select.value;
    const secureSslChecked = this.topicForm.get('enable_ssl')?.value;
    this.vhost = '/';
    this.setDefaultValue(secureSslChecked, this.selectedOption);
    this.generatePushEndpoint(secureSslChecked);
    this.reset();
  }

  setDefaultValue(enableSSL: boolean, selectedValue: string) {
    if (selectedValue === HostURLProtocol.HTTP) {
      this.port = enableSSL ? URLPort.HTTPS : URLPort.HTTP;
    } else if (selectedValue === HostURLProtocol.AMQP) {
      this.port = enableSSL ? URLPort.AMQPS : URLPort.AMQP;
    } else if (selectedValue === HostURLProtocol.KAFKA) {
      this.port = URLPort.KAFKA;
    }

    this.topicForm.patchValue({ port: this.port });
  }
  onSecureSslChange(event: any) {
    const secureSslChecked = event;
    if (this.selectedOption === HostURLProtocol.HTTP) {
      this.port = secureSslChecked === true ? URLPort.HTTPS : URLPort.HTTP;
    } else if (this.selectedOption === HostURLProtocol.AMQP) {
      this.port = secureSslChecked === true ? URLPort.AMQPS : URLPort.AMQP;
    } else if (this.selectedOption === HostURLProtocol.KAFKA) {
      this.port = URLPort.KAFKA;
    }
    this.topicForm.patchValue({ port: this.port });
    this.generatePushEndpoint(secureSslChecked);
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
    const secureSslValue =
      secureSsl !== undefined ? secureSsl : this.topicForm.get('enable_ssl')?.value;
    switch (this.selectedOption) {
      case HostURLProtocol.HTTP: // HTTP Endpoint
        const protocol = secureSslValue ? HostURLProtocol.https : HostURLProtocol.http;
        const fqdnHttp = this.topicForm.get('fqdn')?.value;
        const portHttp = this.topicForm.get('port')?.value;

        if (fqdnHttp && portHttp) {
          generatedEndpoint = `${protocol}://${fqdnHttp}:${portHttp}`;
        }
        break;

      case HostURLProtocol.AMQP: // AMQP Endpoint
        const amqpProtocol = secureSslValue ? HostURLProtocol.amqps : HostURLProtocol.amqp;
        const userAmqp = this.topicForm.get('user')?.value;
        const passwordAmqp = this.topicForm.get('password')?.value;
        const fqdnAmqp = this.topicForm.get('fqdn')?.value;
        const portAmqp = this.topicForm.get('port')?.value;
        const vhostAmqp = this.topicForm.get('vhost')?.value;

        if (fqdnAmqp && portAmqp && vhostAmqp) {
          generatedEndpoint = `${amqpProtocol}://${fqdnAmqp}:${portAmqp}${vhostAmqp}`;
        }
        if (userAmqp && passwordAmqp && fqdnAmqp && portAmqp && vhostAmqp) {
          generatedEndpoint = `${amqpProtocol}://${userAmqp}:${passwordAmqp}@${fqdnAmqp}:${portAmqp}${vhostAmqp}`;
        }
        break;

      case HostURLProtocol.KAFKA: // Kafka Endpoint
        const userKafka = this.topicForm.get('user')?.value;
        const passwordKafka = this.topicForm.get('password')?.value;
        const fqdnKafka = this.topicForm.get('fqdn')?.value;
        const portKafka = this.topicForm.get('port')?.value;
        const kafkaBrokers = this.topicForm.get('kafka_brokers')?.value;
        if (userKafka && passwordKafka && fqdnKafka && portKafka) {
          generatedEndpoint = `kafka://${userKafka}:${passwordKafka}@${fqdnKafka}:${portKafka}`;
        } else if (kafkaBrokers) {
          generatedEndpoint = `kafka://${kafkaBrokers}`;
        } else {
          generatedEndpoint = `kafka://${fqdnKafka}:${portKafka}`;
        }

        break;

      default:
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
      amqp_ack_level: pushendpointAddarg.amqp_ack_level,
      use_ssl: pushendpointAddarg.use_ssl,
      kafka_ack_level: pushendpointAddarg.kafka_ack_level,
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

    if (topicType === 'KAFKA') {
      Object.assign(payload, {
        use_ssl: formValue.use_ssl,
        kafka_ack_level: formValue.kafka_ack_level,
        kafka_brokers: formValue.kafka_brokers,
        ca_location: formValue.ca_location,
        mechanism: formValue.mechanism
      });
    } else if (topicType === 'AMQP') {
      Object.assign(payload, {
        verify_ssl: formValue.verify_ssl,
        amqp_exchange: formValue.amqp_exchange,
        amqp_ack_level: formValue.amqp_ack_level,
        ca_location: formValue.ca_location
      });
    } else if (topicType === 'HTTP') {
      Object.assign(payload, {
        verify_ssl: formValue.verify_ssl,
        cloud_events: formValue.cloud_events
      });
    }

    const notificationTitle = $localize`${
      this.editing
        ? `Topic ${this.topicId} updated successfully`
        : `Topic ${this.topicId} created successfully`
    }`;
    const action = this.editing
      ? this.rgwTopicService.update(payload)
      : this.rgwTopicService.create(payload);

    action.subscribe({
      next: () => {
        this.notificationService.show(NotificationType.success, notificationTitle);
        this.goToListView();
      },
      error: () => this.topicForm.setErrors({ cdSubmitButton: true })
    });
  }

  goToListView() {
    this.router.navigate([`rgw/topic`]);
  }

  clearTextArea(field: string, defaultValue: string = '') {
    this.topicForm.get(field)?.setValue(defaultValue);
    this.topicForm.markAsDirty();
    this.topicForm.updateValueAndValidity();
  }

  // Reset form values while maintaining the endpoint type
  reset() {
    this.topicForm.patchValue({ enable_ssl: true });
    this.topicForm.patchValue({ port: this.port });
    this.topicForm.patchValue({ vhost: this.vhost });
  }
}