import json
import unittest
from predictor import RHDiskFailurePredictor, PSDiskFailurePredictor


class TestModels(unittest.TestCase):
    def setUp(self):
        """Load sample data point for testing
        """
        # possible outputs from predictors
        self.valid_outputs = ['Good', 'Warning', 'Bad', 'Unknown'] 

        # sample input, taken from backblaze q1 2018 csv
        # and converted to match smartctl json format
        with open('tests/sample_input.json', 'r') as f:
            health_data = json.load(f)
        
        # NOTE: this is copied as-is from module.py
        # ideally i'd want to init the module and pass this smartctl json to it
        # but the module only takes device id as input. and since we dont have
        # a test device, this is a workaround
        self.predict_datas = []
        o_keys = sorted(health_data.keys(), reverse=True)
        for o_key in o_keys:
            # get values for current day (?)
            dev_smart = {}
            s_val = health_data[o_key]

            # add all smart attributes
            ata_smart = s_val.get('ata_smart_attributes', {})
            for attr in ata_smart.get('table', []):
                # get raw smart values
                if attr.get('raw', {}).get('string') is not None:
                    if str(attr.get('raw', {}).get('string', '0')).isdigit():
                        dev_smart['smart_%s_raw' % attr.get('id')] = \
                            int(attr.get('raw', {}).get('string', '0'))
                    else:
                        if str(attr.get('raw', {}).get('string', '0')).split(' ')[0].isdigit():
                            dev_smart['smart_%s_raw' % attr.get('id')] = \
                                int(attr.get('raw', {}).get('string',
                                                            '0').split(' ')[0])
                        else:
                            dev_smart['smart_%s_raw' % attr.get('id')] = \
                                attr.get('raw', {}).get('value', 0)
                # get normalized smart values
                if attr.get('value') is not None:
                    dev_smart['smart_%s_normalized' % attr.get('id')] = \
                                attr.get('value')
            # add power on hours manually if not available in smart attributes
            if s_val.get('power_on_time', {}).get('hours') is not None:
                dev_smart['smart_9_raw'] = int(s_val['power_on_time']['hours'])
            # add device capacity
            if s_val.get('user_capacity') is not None:
                if s_val.get('user_capacity').get('bytes') is not None:
                    dev_smart['user_capacity'] = s_val.get('user_capacity').get('bytes')
                else:
                    self.log.debug('user_capacity not found in smart attributes list')
            # add device model
            if s_val.get('model_name') is not None:
                dev_smart['model_name'] = s_val.get('model_name')
            # add vendor
            if s_val.get('vendor') is not None:
                dev_smart['vendor'] = s_val.get('vendor')
            # if smart data was found, then add that to list
            if dev_smart:
                self.predict_datas.append(dev_smart)
            if len(self.predict_datas) >= 12:
                break


    def test_setup(self):
        """Tests if setup was done correctly 
        """
        # ensure test pt is a list of dicts, as it will be for the module
        self.assertIsInstance(self.predict_datas, list)
        for day_data in self.predict_datas:
            self.assertIsInstance(day_data, dict)

        # input to module will have <=12 days and >=6 days of data
        # ensure that test pt reflects that
        self.assertLessEqual(len(self.predict_datas), 12)
        self.assertGreaterEqual(len(self.predict_datas), 6)


    def test_red_hat_predictor(self):
        """Tests if Red Hat created model works correctly
        """
        # check if red hat models initialize properly
        rh_predictor = RHDiskFailurePredictor()
        ret = rh_predictor.initialize('models/redhat')
        self.assertEqual(ret, None)

        # predict and check if value is a valid output
        pred = rh_predictor.predict(self.predict_datas)
        self.assertIn(pred, self.valid_outputs)


    def test_prophetstor_predictor(self):
        """Tests if Prophetstor created model works correctly
        """
        # check if red hat models initialize properly
        ps_predictor = PSDiskFailurePredictor()
        ret = ps_predictor.initialize('models/prophetstor')
        self.assertEqual(ret, None)

        # predict and check if value is a valid output
        pred = ps_predictor.predict(self.predict_datas)
        self.assertIn(pred, self.valid_outputs)


if __name__ == "__main__":
    unittest.main()
