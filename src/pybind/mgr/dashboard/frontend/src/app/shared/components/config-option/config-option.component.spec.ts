import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';

import * as _ from 'lodash';
import { PopoverModule } from 'ngx-bootstrap/popover';
import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ConfigurationService } from '../../api/configuration.service';
import { CdFormGroup } from '../../forms/cd-form-group';
import { HelperComponent } from '../helper/helper.component';
import { ConfigOptionComponent } from './config-option.component';

describe('ConfigOptionComponent', () => {
  let component: ConfigOptionComponent;
  let fixture: ComponentFixture<ConfigOptionComponent>;
  let configurationService: ConfigurationService;
  let oNames: Array<string>;

  configureTestBed({
    declarations: [ConfigOptionComponent, HelperComponent],
    imports: [PopoverModule.forRoot(), ReactiveFormsModule, HttpClientTestingModule],
    providers: [ConfigurationService]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigOptionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
    configurationService = TestBed.inject(ConfigurationService);

    const configOptions: Record<string, any> = [
      {
        name: 'osd_scrub_auto_repair_num_errors',
        type: 'uint',
        level: 'advanced',
        desc: 'Maximum number of detected errors to automatically repair',
        long_desc: '',
        default: 5,
        daemon_default: '',
        tags: [],
        services: [],
        see_also: ['osd_scrub_auto_repair'],
        min: '',
        max: '',
        can_update_at_runtime: true,
        flags: []
      },
      {
        name: 'osd_debug_deep_scrub_sleep',
        type: 'float',
        level: 'dev',
        desc:
          'Inject an expensive sleep during deep scrub IO to make it easier to induce preemption',
        long_desc: '',
        default: 0,
        daemon_default: '',
        tags: [],
        services: [],
        see_also: [],
        min: '',
        max: '',
        can_update_at_runtime: true,
        flags: []
      },
      {
        name: 'osd_heartbeat_interval',
        type: 'int',
        level: 'advanced',
        desc: 'Interval (in seconds) between peer pings',
        long_desc: '',
        default: 6,
        daemon_default: '',
        tags: [],
        services: [],
        see_also: [],
        min: 1,
        max: 86400,
        can_update_at_runtime: true,
        flags: [],
        value: [
          {
            section: 'osd',
            value: 6
          }
        ]
      },
      {
        name: 'bluestore_compression_algorithm',
        type: 'str',
        level: 'advanced',
        desc: 'Default compression algorithm to use when writing object data',
        long_desc:
          'This controls the default compressor to use (if any) if the ' +
          'per-pool property is not set.  Note that zstd is *not* recommended for ' +
          'bluestore due to high CPU overhead when compressing small amounts of data.',
        default: 'snappy',
        daemon_default: '',
        tags: [],
        services: [],
        see_also: [],
        enum_values: ['', 'snappy', 'zlib', 'zstd', 'lz4'],
        min: '',
        max: '',
        can_update_at_runtime: true,
        flags: ['runtime']
      },
      {
        name: 'rbd_discard_on_zeroed_write_same',
        type: 'bool',
        level: 'advanced',
        desc: 'discard data on zeroed write same instead of writing zero',
        long_desc: '',
        default: true,
        daemon_default: '',
        tags: [],
        services: ['rbd'],
        see_also: [],
        min: '',
        max: '',
        can_update_at_runtime: true,
        flags: []
      },
      {
        name: 'rbd_journal_max_payload_bytes',
        type: 'size',
        level: 'advanced',
        desc: 'maximum journal payload size before splitting',
        long_desc: '',
        daemon_default: '',
        tags: [],
        services: ['rbd'],
        see_also: [],
        min: '',
        max: '',
        can_update_at_runtime: true,
        flags: [],
        default: '16384'
      },
      {
        name: 'cluster_addr',
        type: 'addr',
        level: 'basic',
        desc: 'cluster-facing address to bind to',
        long_desc: '',
        daemon_default: '',
        tags: ['network'],
        services: ['osd'],
        see_also: [],
        min: '',
        max: '',
        can_update_at_runtime: false,
        flags: [],
        default: '-'
      },
      {
        name: 'fsid',
        type: 'uuid',
        level: 'basic',
        desc: 'cluster fsid (uuid)',
        long_desc: '',
        daemon_default: '',
        tags: ['service'],
        services: ['common'],
        see_also: [],
        min: '',
        max: '',
        can_update_at_runtime: false,
        flags: ['no_mon_update'],
        default: '00000000-0000-0000-0000-000000000000'
      },
      {
        name: 'mgr_tick_period',
        type: 'secs',
        level: 'advanced',
        desc: 'Period in seconds of beacon messages to monitor',
        long_desc: '',
        daemon_default: '',
        tags: [],
        services: ['mgr'],
        see_also: [],
        min: '',
        max: '',
        can_update_at_runtime: true,
        flags: [],
        default: '2'
      }
    ];

    spyOn(configurationService, 'filter').and.returnValue(observableOf(configOptions));
    oNames = _.map(configOptions, 'name');
    component.optionNames = oNames;
    component.optionsForm = new CdFormGroup({});
    component.optionsFormGroupName = 'testFormGroupName';
    component.ngOnInit();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  describe('optionNameToText', () => {
    it('should format config option names correctly', () => {
      const configOptionNames = {
        osd_scrub_auto_repair_num_errors: 'Scrub Auto Repair Num Errors',
        osd_debug_deep_scrub_sleep: 'Debug Deep Scrub Sleep',
        osd_heartbeat_interval: 'Heartbeat Interval',
        bluestore_compression_algorithm: 'Bluestore Compression Algorithm',
        rbd_discard_on_zeroed_write_same: 'Rbd Discard On Zeroed Write Same',
        rbd_journal_max_payload_bytes: 'Rbd Journal Max Payload Bytes',
        cluster_addr: 'Cluster Addr',
        fsid: 'Fsid',
        mgr_tick_period: 'Tick Period'
      };

      component.options.forEach((option) => {
        expect(option.text).toEqual(configOptionNames[option.name]);
      });
    });
  });

  describe('createForm', () => {
    it('should set the optionsFormGroupName correctly', () => {
      expect(component.optionsFormGroupName).toEqual('testFormGroupName');
    });

    it('should create a FormControl for every config option', () => {
      component.options.forEach((option) => {
        expect(Object.keys(component.optionsFormGroup.controls)).toContain(option.name);
      });
    });
  });

  describe('loadStorageData', () => {
    it('should create a list of config options by names', () => {
      expect(component.options.length).toEqual(9);

      component.options.forEach((option) => {
        expect(oNames).toContain(option.name);
      });
    });

    it('should add all needed attributes to every config option', () => {
      component.options.forEach((option) => {
        const optionKeys = Object.keys(option);
        expect(optionKeys).toContain('text');
        expect(optionKeys).toContain('additionalTypeInfo');
        expect(optionKeys).toContain('value');

        if (option.type !== 'bool' && option.type !== 'str') {
          expect(optionKeys).toContain('patternHelpText');
        }

        if (option.name === 'osd_heartbeat_interval') {
          expect(optionKeys).toContain('maxValue');
          expect(optionKeys).toContain('minValue');
        }
      });
    });

    it('should set minValue and maxValue correctly', () => {
      component.options.forEach((option) => {
        if (option.name === 'osd_heartbeat_interval') {
          expect(option.minValue).toEqual(1);
          expect(option.maxValue).toEqual(86400);
        }
      });
    });

    it('should set the value attribute correctly', () => {
      component.options.forEach((option) => {
        if (option.name === 'osd_heartbeat_interval') {
          const value = option.value;
          expect(value).toBeDefined();
          expect(value).toEqual({ section: 'osd', value: 6 });
        } else {
          expect(option.value).toBeUndefined();
        }
      });
    });

    it('should set the FormControl value correctly', () => {
      component.options.forEach((option) => {
        const value = component.optionsFormGroup.getValue(option.name);
        if (option.name === 'osd_heartbeat_interval') {
          expect(value).toBeDefined();
          expect(value).toEqual(6);
        } else {
          expect(value).toBeNull();
        }
      });
    });
  });
});
