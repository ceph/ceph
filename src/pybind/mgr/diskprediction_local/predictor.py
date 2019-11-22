"""Machine learning model for disk failure prediction.

This classes defined here provide the disk failure prediction module.
RHDiskFailurePredictor uses the models developed at the AICoE in the
Office of the CTO at Red Hat. These models were built using the open
source Backblaze SMART metrics dataset.
PSDiskFailurePredictor uses the models developed by ProphetStor as an
example.

An instance of the predictor is initialized by providing the path to trained
models. Then, to predict hard drive health and deduce time to failure, the
predict function is called with 6 days worth of SMART data from the hard drive.
It will return a string to indicate disk failure status: "Good", "Warning",
"Bad", or "Unknown".

An example code is as follows:

>>> model = disk_failure_predictor.RHDiskFailurePredictor()
>>> status = model.initialize("./models")
>>> if status:
>>>     model.predict(disk_days)
'Bad'
"""
import os
import json
import pickle
import logging

import numpy as np
from scipy import stats


def get_diskfailurepredictor_path():
    path = os.path.abspath(__file__)
    dir_path = os.path.dirname(path)
    return dir_path


class RHDiskFailurePredictor(object):
    """Disk failure prediction module developed at Red Hat

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

    LOGGER = logging.getLogger()

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
            scaler_path = os.path.join(model_dirpath, manufacturer + "_scaler.pkl")
            if not os.path.isfile(scaler_path):
                return "Missing scaler file: {}".format(scaler_path)
            model_path = os.path.join(model_dirpath, manufacturer + "_predictor.pkl")
            if not os.path.isfile(model_path):
                return "Missing model file: {}".format(model_path)

        self.model_dirpath = model_dirpath

    def __preprocess(self, disk_days, manufacturer):
        """Scales and transforms input dataframe to feed it to prediction model

        Arguments:
            disk_days {list} -- list in which each element is a dictionary with key,val
                                as feature name,value respectively.
                                e.g.[{'smart_1_raw': 0, 'user_capacity': 512 ...}, ...]
            manufacturer {str} -- manufacturer of the hard drive

        Returns:
            numpy.ndarray -- (n, d) shaped array of n days worth of data and d
                                features, scaled
        """
        # get the attributes that were used to train model for current manufacturer
        try:
            model_smart_attr = self.model_context[manufacturer]
        except KeyError as e:
            RHDiskFailurePredictor.LOGGER.debug(
                "No context (SMART attributes on which model has been trained) found for manufacturer: {}".format(
                    manufacturer
                )
            )
            return None

        # convert to structured array, keeping only the required features
        # assumes all data is in float64 dtype
        try:
            struc_dtypes = [(attr, np.float64) for attr in model_smart_attr]
            values = [tuple(day[attr] for attr in model_smart_attr) for day in disk_days]
            disk_days_sa = np.array(values, dtype=struc_dtypes)
        except KeyError as e:
            RHDiskFailurePredictor.LOGGER.debug(
                "Mismatch in SMART attributes used to train model and SMART attributes available"
            )
            return None

        # view structured array as 2d array for applying rolling window transforms
        # do not include capacity_bytes in this. only use smart_attrs
        disk_days_attrs = disk_days_sa[[attr for attr in model_smart_attr if 'smart_' in attr]]\
                            .view(np.float64).reshape(disk_days_sa.shape + (-1,))

        # featurize n (6 to 12) days data - mean,std,coefficient of variation
        # current model is trained on 6 days of data because that is what will be
        # available at runtime

        # rolling time window interval size in days
        roll_window_size = 6

        # rolling means generator
        gen = (disk_days_attrs[i: i + roll_window_size, ...].mean(axis=0) \
                for i in range(0, disk_days_attrs.shape[0] - roll_window_size + 1))
        means = np.vstack(gen)

        # rolling stds generator
        gen = (disk_days_attrs[i: i + roll_window_size, ...].std(axis=0, ddof=1) \
                for i in range(0, disk_days_attrs.shape[0] - roll_window_size + 1))
        stds = np.vstack(gen)

        # coefficient of variation
        cvs = stds / means
        cvs[np.isnan(cvs)] = 0
        featurized = np.hstack((
                                means,
                                stds,
                                cvs,
                                disk_days_sa['user_capacity'][: disk_days_attrs.shape[0] - roll_window_size + 1].reshape(-1, 1)
                                ))

        # scale features
        scaler_path = os.path.join(self.model_dirpath, manufacturer + "_scaler.pkl")
        with open(scaler_path, 'rb') as f:
            scaler = pickle.load(f)
        featurized = scaler.transform(featurized)
        return featurized

    @staticmethod
    def __get_manufacturer(model_name):
        """Returns the manufacturer name for a given hard drive model name

        Arguments:
            model_name {str} -- hard drive model name

        Returns:
            str -- manufacturer name
        """
        for prefix, manufacturer in RHDiskFailurePredictor.MANUFACTURER_MODELNAME_PREFIXES.items():
            if model_name.startswith(prefix):
                return manufacturer
        # print error message
        RHDiskFailurePredictor.LOGGER.debug(
            "Could not infer manufacturer from model name {}".format(model_name)
        )

    def predict(self, disk_days):
        # get manufacturer preferably as a smartctl attribute
        # if not available then infer using model name
        manufacturer = disk_days[0].get("vendor")
        if manufacturer is None:
            RHDiskFailurePredictor.LOGGER.debug(
                '"vendor" field not found in smartctl output. Will try to infer manufacturer from model name.'
            )
            manufacturer = RHDiskFailurePredictor.__get_manufacturer(
                disk_days[0].get("model_name", "")
            ).lower()

        # print error message, return Unknown, and continue execution
        if manufacturer is None:
            RHDiskFailurePredictor.LOGGER.debug(
                "Manufacturer could not be determiend. This may be because \
                DiskPredictor has never encountered this manufacturer before, \
                    or the model name is not according to the manufacturer's \
                        naming conventions known to DiskPredictor"
            )
            return RHDiskFailurePredictor.PREDICTION_CLASSES[-1]

        # preprocess for feeding to model
        preprocessed_data = self.__preprocess(disk_days, manufacturer)
        if preprocessed_data is None:
            return RHDiskFailurePredictor.PREDICTION_CLASSES[-1]

        # get model for current manufacturer
        model_path = os.path.join(
            self.model_dirpath, manufacturer + "_predictor.pkl"
        )
        with open(model_path, 'rb') as f:
            model = pickle.load(f)

        # use prediction for most recent day
        # TODO: ensure that most recent day is last element and most previous day
        # is first element in input disk_days
        pred_class_id = model.predict(preprocessed_data)[-1]
        return RHDiskFailurePredictor.PREDICTION_CLASSES[pred_class_id]


class PSDiskFailurePredictor(object):
    """Disk failure prediction developed at ProphetStor

    This class implements a disk failure prediction module.
    """

    CONFIG_FILE = "config.json"
    EXCLUDED_ATTRS = ["smart_9_raw", "smart_241_raw", "smart_242_raw"]

    def __init__(self):
        """
        This function may throw exception due to wrong file operation.
        """

        self.model_dirpath = ""
        self.model_context = {}

    def initialize(self, model_dirpath):
        """
        Initialize all models.

        Args: None

        Returns:
            Error message. If all goes well, return an empty string.

        Raises:
        """

        config_path = os.path.join(model_dirpath, self.CONFIG_FILE)
        if not os.path.isfile(config_path):
            return "Missing config file: " + config_path
        else:
            with open(config_path) as f_conf:
                self.model_context = json.load(f_conf)

        for model_name in self.model_context:
            model_path = os.path.join(model_dirpath, model_name)

            if not os.path.isfile(model_path):
                return "Missing model file: " + model_path

        self.model_dirpath = model_dirpath

    def __preprocess(self, disk_days):
        """
        Preprocess disk attributes.

        Args:
            disk_days: Refer to function predict(...).

        Returns:
            new_disk_days: Processed disk days.
        """

        req_attrs = []
        new_disk_days = []

        attr_list = set.intersection(*[set(disk_day.keys()) for disk_day in disk_days])
        for attr in attr_list:
            if (
                attr.startswith("smart_") and attr.endswith("_raw")
            ) and attr not in self.EXCLUDED_ATTRS:
                req_attrs.append(attr)

        for disk_day in disk_days:
            new_disk_day = {}
            for attr in req_attrs:
                if float(disk_day[attr]) >= 0.0:
                    new_disk_day[attr] = disk_day[attr]

            new_disk_days.append(new_disk_day)

        return new_disk_days

    @staticmethod
    def __get_diff_attrs(disk_days):
        """
        Get 5 days differential attributes.

        Args:
            disk_days: Refer to function predict(...).

        Returns:
            attr_list: All S.M.A.R.T. attributes used in given disk. Here we
                       use intersection set of all disk days.

            diff_disk_days: A list struct comprises 5 dictionaries, each
                            dictionary contains differential attributes.

        Raises:
            Exceptions of wrong list/dict operations.
        """

        all_attrs = [set(disk_day.keys()) for disk_day in disk_days]
        attr_list = list(set.intersection(*all_attrs))
        attr_list = disk_days[0].keys()
        prev_days = disk_days[:-1]
        curr_days = disk_days[1:]
        diff_disk_days = []
        # TODO: ensure that this ordering is correct
        for prev, cur in zip(prev_days, curr_days):
            diff_disk_days.append(
                {attr: (int(cur[attr]) - int(prev[attr])) for attr in attr_list}
            )

        return attr_list, diff_disk_days

    def __get_best_models(self, attr_list):
        """
        Find the best model from model list according to given attribute list.

        Args:
            attr_list: All S.M.A.R.T. attributes used in given disk.

        Returns:
            modelpath: The best model for the given attribute list.
            model_attrlist: 'Ordered' attribute list of the returned model.
                            Must be aware that SMART attributes is in order.

        Raises:
        """

        models = self.model_context.keys()

        scores = []
        for model_name in models:
            scores.append(
                sum(attr in attr_list for attr in self.model_context[model_name])
            )
        max_score = max(scores)

        # Skip if too few matched attributes.
        if max_score < 3:
            print("Too few matched attributes")
            return None

        best_models = {}
        best_model_indices = [
            idx for idx, score in enumerate(scores) if score > max_score - 2
        ]
        for model_idx in best_model_indices:
            model_name = list(models)[model_idx]
            model_path = os.path.join(self.model_dirpath, model_name)
            model_attrlist = self.model_context[model_name]
            best_models[model_path] = model_attrlist

        return best_models
        # return os.path.join(self.model_dirpath, model_name), model_attrlist

    @staticmethod
    def __get_ordered_attrs(disk_days, model_attrlist):
        """
        Return ordered attributes of given disk days.

        Args:
            disk_days: Unordered disk days.
            model_attrlist: Model's ordered attribute list.

        Returns:
            ordered_attrs: Ordered disk days.

        Raises: None
        """

        ordered_attrs = []

        for one_day in disk_days:
            one_day_attrs = []

            for attr in model_attrlist:
                if attr in one_day:
                    one_day_attrs.append(one_day[attr])
                else:
                    one_day_attrs.append(0)

            ordered_attrs.append(one_day_attrs)

        return ordered_attrs

    def predict(self, disk_days):
        """
        Predict using given 6-days disk S.M.A.R.T. attributes.

        Args:
            disk_days: A list struct comprises 6 dictionaries. These
                       dictionaries store 'consecutive' days of disk SMART
                       attributes.
        Returns:
            A string indicates prediction result. One of following four strings
            will be returned according to disk failure status:
            (1) Good : Disk is health
            (2) Warning : Disk has some symptoms but may not fail immediately
            (3) Bad : Disk is in danger and data backup is highly recommended
            (4) Unknown : Not enough data for prediction.

        Raises:
            Pickle exceptions
        """

        all_pred = []

        proc_disk_days = self.__preprocess(disk_days)
        attr_list, diff_data = PSDiskFailurePredictor.__get_diff_attrs(proc_disk_days)
        modellist = self.__get_best_models(attr_list)
        if modellist is None:
            return "Unknown"

        for modelpath in modellist:
            model_attrlist = modellist[modelpath]
            ordered_data = PSDiskFailurePredictor.__get_ordered_attrs(
                diff_data, model_attrlist
            )

            try:
                with open(modelpath, "rb") as f_model:
                    clf = pickle.load(f_model)

            except UnicodeDecodeError:
                # Compatibility for python3
                with open(modelpath, "rb") as f_model:
                    clf = pickle.load(f_model, encoding="latin1")

            pred = clf.predict(ordered_data)

            all_pred.append(1 if any(pred) else 0)

        score = 2 ** sum(all_pred) - len(modellist)
        if score > 10:
            return "Bad"
        if score > 4:
            return "Warning"
        return "Good"
