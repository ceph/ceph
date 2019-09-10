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
    PREDICTION_CLASSES = {-1: "Unknown", 0: "Good", 1: "Warning", 2: "Bad"}

    # model name prefixes to identify vendor
    MANUFACTURER_MODELNAME_PREFIXES = {
        "WDC": "WDC",
        "Toshiba": "Toshiba",  # for cases like "Toshiba xxx"
        "TOSHIBA": "Toshiba",  # for cases like "TOSHIBA xxx"
        "toshiba": "Toshiba",  # for cases like "toshiba xxx"
        "S": "Seagate",        # for cases like "STxxxx" and "Seagate BarraCuda ZAxxx"
        "ZA": "Seagate",       # for cases like "ZAxxxx"
        "Hitachi": "Hitachi",
        "HGST": "HGST",
    }

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
        # have models and scalers saved inside model_dirpath
        for manufacturer in self.model_context:
            scaler_path = os.path.join(model_dirpath, manufacturer + "_scaler.joblib")
            if not os.path.isfile(scaler_path):
                return "Missing scaler file: {}".format(scaler_path)
            model_path = os.path.join(model_dirpath, manufacturer + "_predictor.joblib")
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
        df["user_capacity"] = df["user_capacity"].apply(lambda x: x["bytes"])

        # change from dict type {'table': [{}, {}, {}]}  to list type [{}, {}, {}]
        df["ata_smart_attributes"] = df["ata_smart_attributes"].apply(
            lambda x: x["table"]
        )

        # make a separate column for raw and normalized values of each smart id
        for day_idx in range(len(disk_days)):
            for attr_dict in df.iloc[0]["ata_smart_attributes"]:
                smart_id = attr_dict["id"]
                df.at[day_idx, "smart_{}_raw".format(smart_id)] = int(
                    attr_dict["raw"]["value"]
                )
                df.at[day_idx, "smart_{}_normalized".format(smart_id)] = int(
                    attr_dict["value"]
                )

        # drop the now-redundant column
        df = df.drop("ata_smart_attributes", axis=1)
        return df

    def __preprocess(self, disk_days_df, manufacturer):
        """Scales and transforms input dataframe to feed it to prediction model

        Arguments:
            disk_days_df {pandas.DataFrame} -- df where each row holds drive
                                                features from one day.
            manufacturer {str} -- manufacturer of the hard drive

        Returns:
            numpy.ndarray -- (n, d) shaped array of n days worth of data and d
                                features, scaled
        """
        # get the attributes that were used to train model for current manufacturer
        try:
            model_smart_attr = self.model_context[manufacturer]
        except KeyError as e:
            print("No context (SMART attributes on which model has been trained) found for manufacturer: {}"\
                .format(manufacturer)
            )
            return None

        # keep only the required features
        try:
            disk_days_df = disk_days_df[model_smart_attr]
        except KeyError as e:
            print("Mismatch in SMART attributes used to train model and SMART attributes available")
            return None

        # featurize n (6 to 12) days data - mean,std,coefficient of variation
        # current model is trained on 6 days of data because that is what will be
        # available at runtime
        # NOTE: ensure unique indices so that features can be merged w/ pandas errors
        disk_days_df = disk_days_df.reset_index(drop=True)
        means = disk_days_df.drop("user_capacity", axis=1).rolling(6).mean()
        stds = disk_days_df.drop("user_capacity", axis=1).rolling(6).std()
        cvs = stds.divide(means, fill_value=0)

        # rename and combine features into one df
        means = means.rename(columns={col: "mean_" + col for col in means.columns})
        stds = stds.rename(columns={col: "std_" + col for col in stds.columns})
        cvs = cvs.rename(columns={col: "cv_" + col for col in cvs.columns})
        featurized_df = means.merge(stds, left_index=True, right_index=True)
        featurized_df = featurized_df.merge(cvs, left_index=True, right_index=True)

        # drop rows where all features (mean,std,cv) are nans
        featurized_df = featurized_df.dropna(how="all")

        # fill nans created by cv calculation
        featurized_df = featurized_df.fillna(0)

        # capacity is not a feature that varies over time
        # FIXME: will this values roll over
        featurized_df["user_capacity"] = disk_days_df["user_capacity"]

        # scale features
        scaler_path = os.path.join(self.model_dirpath, manufacturer + "_scaler.joblib")
        scaler = joblib.load(scaler_path)
        featurized_df = scaler.transform(featurized_df)
        return featurized_df

    @staticmethod
    def __get_manufacturer(model_name):
        """Returns the manufacturer name for a given hard drive model name

        Arguments:
            model_name {str} -- hard drive model name

        Returns:
            str -- manufacturer name
        """
        for prefix, manufacturer in DiskFailurePredictor.MANUFACTURER_MODELNAME_PREFIXES.items():
            if model_name.startswith(prefix):
                return manufacturer
        # print error message
        print("Could not infer manufacturer from model name {}".format(model_name))

    def predict(self, disk_days):
        # massage data into a format that can be fed to models
        raw_df = self.__format_raw_data(disk_days)

        # get manufacturer preferably as a smartctl attribute
        # if not available then infer using model name
        try:
            manufacturer = raw_df["vendor"].iloc[0]
        except KeyError as e:
            print('"vendor" field not found in smartctl output. Will try to infer manufacturer from model name.')
            manufacturer = DiskFailurePredictor.__get_manufacturer(raw_df["model_name"].iloc[0]).lower()

        # print error message, return Unknown, and continue execution
        if manufacturer is None:
            print(
                "Manufacturer could not be determiend. This may be because \
                DiskPredictor has never encountered this manufacturer before, \
                    or the model name is not according to the manufacturer's \
                        naming conventions known to DiskPredictor"
            )
            return DiskFailurePredictor.PREDICTION_CLASSES[-1]

        # preprocess for feeding to model
        preprocessed_data = self.__preprocess(raw_df, manufacturer)
        if preprocessed_data is None:
            return DiskFailurePredictor.PREDICTION_CLASSES[-1]

        # get model for current manufacturer
        model_path = os.path.join(
            self.model_dirpath, manufacturer + "_predictor.joblib"
        )
        model = joblib.load(model_path)

        # use prediction for last day
        pred_class_id = model.predict(preprocessed_data)[-1]
        return DiskFailurePredictor.PREDICTION_CLASSES[pred_class_id]
