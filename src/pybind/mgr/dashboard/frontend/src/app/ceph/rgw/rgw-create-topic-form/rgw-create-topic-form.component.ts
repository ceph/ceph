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
import {
  AMQPACKLEVEL,
  ENDPOINTTYPE,
  KAFKAACKLEVEL,
  TopicModel
} from '../rgw-topic-list/topic.model';
import { UntypedFormControl, Validators } from '@angular/forms';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { ActivatedRoute, Router } from '@angular/router';
import { NotificationService } from '~/app/shared/services/notification.service';
import { CreateTopicModel, KAFKAMECHANISM } from './create-topic.model';
import { RgwUserService } from '~/app/shared/api/rgw-user.service';
import { forkJoin } from 'rxjs/internal/observable/forkJoin';
import { CdValidators } from '~/app/shared/forms/cd-validators';

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
  connect: string = 'a';
  resource: string;
  endpointType: string[] = [];
  amqp_ack_level: string[] = [];
  kafka_ack_level: string[] = [];
  selectedOption: string;
  verify_sslflag: boolean = true;
  secure_sslflag: boolean = true;
  port: string;
  useSslFlag: boolean = false;
  enable_ssl: boolean = false;
  levelFlag: any;
  owners: string[];
  vhost: string;
  passwordhelperText: string;
  selectOwner: string;
  kafka_mechanism: string[] = [];
  editing: boolean = false;
  topicId: string;
  fqdn: string;
  topicArn: string;
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
    this.endpointType = Object.values(ENDPOINTTYPE);
    this.amqp_ack_level = Object.values(AMQPACKLEVEL);
    this.kafka_ack_level = Object.values(KAFKAACKLEVEL);
    this.createForm();
    this.topicId = this.editing ? this.router.url.split('/').pop() : '';
    this.passwordhelperText = 'Defalut password is guest';
    this.topicForm.get('user')?.valueChanges.subscribe(() => this.setMechanism());
    this.topicForm.get('password')?.valueChanges.subscribe(() => this.setMechanism());
    this.kafka_mechanism = Object.values(KAFKAMECHANISM);
    const promises = {
      owners: this.rgwUserService.enumerate()
    };
    forkJoin(promises).subscribe((data: any) => {
      this.owners = (<string[]>data.owners).sort();
      this.loadingReady();
    });
    if (this.editing) {
      this.topicId = this.route.snapshot.paramMap.get('name');
      this.loadTopicData(this.topicId);
    } else {
      this.loadingReady();
    }
    this.setMechanism(); //
  }
  loadTopicData(_topicId: string) {
    this.rgwTopicService.get(_topicId).subscribe((topic: TopicModel) => {
      this.topicForm.get('name')?.disable();
      let url = topic.dest.push_endpoint;
      let endpointType = url.split('://')[0].toUpperCase();
      if (endpointType === 'amqp' || 'amqps') {
        endpointType = 'AMQP';
      } else if (endpointType === 'https' || 'http') {
        endpointType = 'HTTP';
      } else {
        endpointType = 'KAFKA';
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
        CdValidators.unique(this.rgwTopicService.validatetopicName, this.rgwTopicService)
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
      port: new UntypedFormControl('', { validators: [Validators.required] }),
      verify_ssl: new UntypedFormControl(true),
      enable_ssl: new UntypedFormControl(this.enable_ssl), // Default to SSL true for HTTP/AMQP
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

  onSelectOwner(event: Event) {
    const selectOwnervalue = event.target as HTMLSelectElement;
    this.selectOwner = selectOwnervalue.value;
  }

  // Handle select change to update endpoint
  onSelectChange(event: Event) {
    const select = event.target as HTMLSelectElement;
    this.selectedOption = select.value;
    const secureSslChecked = this.topicForm.get('enable_ssl')?.value;
    this.enable_ssl = true;
    this.vhost = '/';
    this.setDefaultValue(this.enable_ssl, this.selectedOption);
    this.generatePushEndpoint(secureSslChecked);
    this.reset();
  }

  setDefaultValue(enableSSL: boolean, selectedValue: string) {
    if (selectedValue === 'HTTP') {
      this.port = enableSSL ? '443' : '80';
    } else if (selectedValue === 'AMQP') {
      this.port = enableSSL ? '5671' : '5672';
    } else if (selectedValue === 'KAFKA') {
      this.port = '9092';
    }

    this.topicForm.patchValue({ port: this.port });
  }
  onSecureSslChange(event: any) {
    const secureSslChecked = event;
    this.secure_sslflag = secureSslChecked;
    if (this.selectedOption === 'HTTP') {
      this.port = secureSslChecked === true ? '443' : '80';
    } else if (this.selectedOption === 'AMQP') {
      this.port = secureSslChecked === true ? '5671' : '5672';
    } else if (this.selectedOption === 'KAFKA') {
      this.port = '9092';
    }
    this.topicForm.patchValue({ port: this.port });
    this.generatePushEndpoint(secureSslChecked);
  }

  onVerifySslChange(event: any) {
    const verifySslChecked = event;
    this.verify_sslflag = verifySslChecked;
  }

  onUseSslChange(event: any) {
    const useSslChecked = event;
    this.useSslFlag = useSslChecked;
  }

  onLevelChange(event: any) {
    const levelChecked = event;
    this.levelFlag = levelChecked;
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
      case 'HTTP': // HTTP Endpoint
        const protocol = secureSslValue ? 'https' : 'http';
        const fqdnHttp = this.topicForm.get('fqdn')?.value;
        const portHttp = this.topicForm.get('port')?.value;

        if (fqdnHttp && portHttp) {
          generatedEndpoint = `${protocol}://${fqdnHttp}:${portHttp}`;
        }
        break;

      case 'AMQP': // AMQP Endpoint
        const amqpProtocol = secureSslValue ? 'amqps' : 'amqp';
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

      case 'KAFKA': // Kafka Endpoint
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
  extractValues(topic: TopicModel) {
    let url = topic.dest.push_endpoint;
    let pushendpointUrl = this.convertFullUrlToObject(url);
    if (pushendpointUrl.protocol === 'amqp:' || pushendpointUrl.protocol === 'amqps:') {
      this.selectedOption = 'AMQP';
    } else if (pushendpointUrl.protocol === 'https:' || pushendpointUrl.protocol === 'http:') {
      this.selectedOption = 'HTTP';
    } else {
      this.selectedOption = 'KAFKA';
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
      port: pushendpointUrl.port,
      fqdn: pushendpointUrl.hostname,
      vhost: pushendpointUrl.pathname,
      user: pushendpointUrl.username,
      password: pushendpointUrl.password,
      ca_location: pushendpointAddarg.ca_location,
      mechanism: pushendpointAddarg.mechanism,
      enable_ssl: pushendpointUrl.protocol === 'https:' ? true : false,
      verify_ssl: pushendpointAddarg.verify_ssl,
      cloud_events: pushendpointAddarg.cloud_events,
      amqp_exchange: pushendpointAddarg.amqp_exchange,
      amqp_ack_level: pushendpointAddarg.amqp_ack_level,
      use_ssl: pushendpointAddarg.use_ssl,
      kafka_ack_level: pushendpointAddarg.kafka_ack_level,
      kafka_brokers: pushendpointAddarg.kafka_b
    });
  }

  convertFullUrlToObject(url: string): any {
    const urlObj = new URL(url);
    let port = urlObj.port;
    if (!port) {
      port = urlObj.protocol === 'https:' ? '443' : urlObj.protocol === 'http:' ? '80' : '';
    }
    return {
      protocol: urlObj.protocol, // e.g., 'https:'
      hostname: urlObj.hostname, // e.g., 'example.com'
      pathname: urlObj.pathname, // e.g., '/path/to/resource'
      hash: urlObj.hash, // e.g., '#section1'
      port: port, // e.g., '443' for HTTPS or '80' for HTTP
      username: urlObj.username, // e.g., 'user'
      password: urlObj.password // e.g., 'password'
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

  submitAction() {
    if (this.topicForm.invalid || this.topicForm.pending) {
      return this.topicForm.setErrors({ cdSubmitButton: true });
    }

    const formValue = this.topicForm.getRawValue(),
      topicType = formValue.endpointType,
      topicPolicy = this.getTopicPolicy();
    let payload: CreateTopicModel = {
      name: formValue.name,
      owner: formValue.owner,
      push_endpoint: formValue.push_endpoint,
      OpaqueData: formValue.OpaqueData,
      persistent: formValue.persistent,
      time_to_live: formValue.time_to_live,
      max_retries: formValue.max_retries,
      retry_sleep_duration: formValue.retry_sleep_duration,
      policy: topicPolicy,
      ca_location: formValue.ca_location,
      mechanism: formValue.mechanism
    };

    if (topicType === 'KAFKA') {
      Object.assign(payload, {
        use_ssl: formValue.use_ssl,
        kafka_ack_level: formValue.kafka_ack_level,
        kafka_brokers: formValue.kafka_brokers
      });
    } else if (topicType === 'AMQP') {
      Object.assign(payload, {
        verify_ssl: formValue.verify_ssl,
        amqp_exchange: formValue.amqp_exchange,
        amqp_ack_level: formValue.amqp_ack_level
      });
    } else if (topicType === 'HTTP') {
      Object.assign(payload, {
        verify_ssl: formValue.verify_ssl,
        cloud_events: formValue.cloud_events
      });
    }

    const notificationTitle = $localize`${
      this.editing ? 'Topic updated successfully' : 'Topic created successfully'
    }`;
    const action = this.editing
      ? this.rgwTopicService.create(payload)
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

  // Reset form values while maintaining the endpoint type
  reset() {
    this.topicForm.patchValue({ enable_ssl: true });
    this.topicForm.patchValue({ port: this.port });
    this.topicForm.patchValue({ vhost: this.vhost });
  }
}
