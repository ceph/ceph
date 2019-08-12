# first test that CSV files are created for each ruleset 
$ crushtool -i five-devices.crushmap --test --num-rep 1 --min-x 0 --max-x 9 --output-csv
$ if [ ! -f data-absolute_weights.csv ]; then echo FAIL; fi
$ if [ ! -f data-batch_device_expected_utilization_all.csv ]; then echo FAIL; fi 
$ if [ ! -f data-batch_device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f data-device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f data-device_utilization.csv ]; then echo FAIL; fi
$ if [ ! -f data-placement_information.csv ]; then echo FAIL; fi
$ if [ ! -f data-proportional_weights_all.csv ]; then echo FAIL; fi
$ if [ ! -f data-proportional_weights.csv ]; then echo FAIL; fi
$ if [ ! -f metadata-absolute_weights.csv ]; then echo FAIL; fi
$ if [ ! -f metadata-batch_device_expected_utilization_all.csv ]; then echo FAIL; fi 
$ if [ ! -f metadata-batch_device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f metadata-device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f metadata-device_utilization.csv ]; then echo FAIL; fi
$ if [ ! -f metadata-placement_information.csv ]; then echo FAIL; fi
$ if [ ! -f metadata-proportional_weights_all.csv ]; then echo FAIL; fi
$ if [ ! -f metadata-proportional_weights.csv ]; then echo FAIL; fi
$ if [ ! -f rbd-absolute_weights.csv ]; then echo FAIL; fi
$ if [ ! -f rbd-batch_device_expected_utilization_all.csv ]; then echo FAIL; fi 
$ if [ ! -f rbd-batch_device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f rbd-device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f rbd-device_utilization.csv ]; then echo FAIL; fi
$ if [ ! -f rbd-placement_information.csv ]; then echo FAIL; fi
$ if [ ! -f rbd-proportional_weights_all.csv ]; then echo FAIL; fi
$ if [ ! -f rbd-proportional_weights.csv ]; then echo FAIL; fi
$ rm data*csv
$ rm metadata*csv
$ rm rbd*csv
# now check that the CSV files are made to the proper length
$ crushtool -i five-devices.crushmap --test --rule 0 --num-rep 1 --min-x 0 --max-x 9 --output-csv
$ if [ $(wc -l data-absolute_weights.csv | awk '{print $1}') != "5" ]; then echo FAIL; fi
$ if [ $(wc -l data-batch_device_expected_utilization_all.csv | awk '{print $1}') != "5" ]; then echo FAIL; fi
$ if [ $(wc -l data-batch_device_utilization_all.csv | awk '{print $1}') != "5" ]; then echo FAIL; fi
$ if [ $(wc -l data-device_utilization_all.csv | awk '{print $1}') != "5" ]; then echo FAIL; fi
$ if [ $(wc -l data-device_utilization.csv | awk '{print $1}') != "5" ]; then echo FAIL; fi
$ if [ $(wc -l data-placement_information.csv | awk '{print $1}') != "10" ]; then echo FAIL; fi
$ if [ $(wc -l data-proportional_weights_all.csv | awk '{print $1}') != "5" ]; then echo FAIL; fi
$ if [ $(wc -l data-proportional_weights.csv | awk '{print $1}') != "5" ]; then echo FAIL; fi
$ rm data*csv
# finally check that user supplied tags are prepended correctly
$ crushtool -i five-devices.crushmap --test --rule 0 --num-rep 1 --min-x 0 --max-x 9 --output-name "test-tag" --output-csv
$ if [ ! -f test-tag-data-absolute_weights.csv ]; then echo FAIL; fi
$ if [ ! -f test-tag-data-batch_device_expected_utilization_all.csv ]; then echo FAIL; fi 
$ if [ ! -f test-tag-data-batch_device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f test-tag-data-device_utilization_all.csv ]; then echo FAIL; fi
$ if [ ! -f test-tag-data-device_utilization.csv ]; then echo FAIL; fi
$ if [ ! -f test-tag-data-placement_information.csv ]; then echo FAIL; fi
$ if [ ! -f test-tag-data-proportional_weights_all.csv ]; then echo FAIL; fi
$ if [ ! -f test-tag-data-proportional_weights.csv ]; then echo FAIL; fi
$ rm test-tag*csv
