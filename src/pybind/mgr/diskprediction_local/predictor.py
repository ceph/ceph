"""Machine learning model for disk failure prediction.

This class provides serves the disk failure prediction module. It uses the
models developed at the AICoE in the Office of the CTO at Red Hat.

An instance of the predictor is initialized by providing the path to trained
models. Then, to predict hard drive health and deduce time to failure, the
predict function is called with 6 days worth of SMART data from the hard drive.
It will return a string to indicate disk failure status: "Good", "Warning",
"Bad", or "Unknown".

An example code is as follows:

>>> model = disk_failure_predictor.DiskFailurePredictor()
>>> status = model.initialize("./models")
>>> if status:
>>>     model.predict(disk_days)
'Bad'
"""
import os
import json
import joblib
import logging

import numpy as np
import pandas as pd
from scipy import stats


def get_diskfailurepredictor_path():
    path = os.path.abspath(__file__)
    dir_path = os.path.dirname(path)
    return dir_path


class DiskFailurePredictor(object):
    """Disk failure prediction

    This class implements a disk failure prediction module.
    """
    # json with manufacturer names as keys
    # and features used for prediction as values
    CONFIG_FILE = "config.json"
    PREDICTION_CLASSES = {-1: "Unknown",
                          0: "Good",
                          1: "Warning",
                          2: "Bad"}


    def __init__(self):
        """
        This function may throw exception due to wrong file operation.
        """
        self.model_dirpath = ""
        self.model_context = {}


    def initialize(self, model_dirpath):
        """Initialize all models. Save paths of all trained model files to list

        Arguments:
            model_dirpath {str} -- path to directory of trained models

        Returns:
            str -- Error message. If all goes well, return None
        """
        # read config file as json, if it exists
        config_path = os.path.join(model_dirpath, self.CONFIG_FILE)
        if not os.path.isfile(config_path):
            return "Missing config file: " + config_path
        else:
            with open(config_path) as f_conf:
                self.model_context = json.load(f_conf)

        # ensure all manufacturers whose context is defined in config file
        # have models and preprocessors saved inside model_dirpath
        for manufacturer in self.model_context:
            preprocessor_path = os.path.join(model_dirpath, manufacturer + '_preprocessor.joblib')
            if not os.path.isfile(preprocessor_path):
                return "Missing preprocessor file: {}".format(preprocessor_path)
            model_path = os.path.join(model_dirpath, manufacturer + '_predictor.joblib')
            if not os.path.isfile(model_path):
                return "Missing model file: {}".format(model_path)

        self.model_dirpath = model_dirpath


    def __format_raw_data(self, disk_days):
        """Massages the input raw data into a form that can be used by the
        predictor for preprocessing, feeding to model, etc. Specifically,
        converts list of dictionaries to a pandas.DataFrame.

        Arguments:
            disk_days {list} -- list of n dictionaries representing SMART data
                                from the past n days. Value of n depends on the
                                Module defined in module.py

        Returns:
            pandas.DataFrame -- df where each row holds SMART attributes and
                                possibly other data for the drive from one day.
        """
        # list of dictionaries to dataframe
        df = pd.DataFrame(disk_days)

        # change from dict type {'bytes': 123} to just float64 type 123
        df['user_capacity'] = df['user_capacity'].apply(lambda x: x['bytes'])

        # change from dict type {'table': [{}, {}, {}]}  to list type [{}, {}, {}]
        df['ata_smart_attributes'] = df['ata_smart_attributes'].apply(lambda x: x['table'])

        # make a separate column for raw and normalized values of each smart id
        for day_idx in range(len(disk_days)):
            for attr_dict in df.iloc[0]['ata_smart_attributes']:
                smart_id = attr_dict['id']
                df.at[day_idx, 'smart_{}_raw'.format(smart_id)] = int(attr_dict['raw']['value'])
                df.at[day_idx, 'smart_{}_normalized'.format(smart_id)] = int(attr_dict['value'])

        # drop the now-redundant column
        df = df.drop('ata_smart_attributes', axis=1)
        return df


    def __preprocess(self, disk_days_df):
        """Scales and transforms input dataframe to feed it to prediction model

        Arguments:
            disk_days_df {pandas.DataFrame} -- df where each row holds drive
                                                features from one day.

        Returns:
            numpy.ndarray -- (n, d) shaped array of n days worth of data and d
                                features, scaled
        """
        # preprocessing may vary across manufactueres. so get manufacturer
        manufacturer = DiskFailurePredictor.__get_manufacturer(disk_days_df['model_name'].iloc[0]).lower()

        # keep only the features used for prediction for current manufacturer
        try:
            disk_days_df = disk_days_df[self.model_context[manufacturer]]
        except KeyError as e:
            # TODO: change to log.error
            print("Either SMART attributes mismatch for hard drive and prediction model,\
                 or 'model_name' not available in input data")
            print(e)
            return None

        # scale raw data
        preprocessor_path = os.path.join(self.model_dirpath, manufacturer + '_preprocessor.joblib')
        preprocessor = joblib.load(preprocessor_path)
        disk_days_df = preprocessor.transform(disk_days_df)
        return disk_days_df


    @staticmethod
    def __get_manufacturer(model_name):
        """Returns the manufacturer name for a given hard drive model name

        Arguments:
            model_name {str} -- hard drive model name

        Returns:
            str -- manufacturer name
        """
        if model_name.startswith("W"):
            return "WDC"
        elif model_name.startswith("T"):
            return "Toshiba"
        elif model_name.startswith("S"):
            return "Seagate"
        elif model_name.startswith("Hi"):
            return "Hitachi"
        else:
            return "HGST"


    def predict(self, disk_days):
        # massage data into a format that can be fed to models
        raw_df = self.__format_raw_data(disk_days)

        # preprocess
        preprocessed_data = self.__preprocess(raw_df)
        if preprocessed_data is None:
            return DiskFailurePredictor.PREDICTION_CLASSES[-1]

        # get model for current manufacturer
        manufacturer = self.__get_manufacturer(raw_df['model_name'].iloc[0]).lower()
        model_path = os.path.join(self.model_dirpath, manufacturer + '_predictor.joblib')
        model = joblib.load(model_path)

        # predictions for each day
        preds = model.predict(preprocessed_data)

        # use majority vote to decide class. raise if a nan prediction exists
        pred_class_id = stats.mode(preds, nan_policy='raise').mode[0]
        return DiskFailurePredictor.PREDICTION_CLASSES[pred_class_id]
