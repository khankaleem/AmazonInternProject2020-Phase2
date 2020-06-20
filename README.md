# AmazonInternProject2020-Phase2
Phase2 of Internship


Command:

hadoop-streaming -files s3://test-emr-bucket-kaleem/emr/mapper.py,s3://test-emr-bucket-kaleem/emr/reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py" -input s3://test-emr-bucket-kaleem/emr/input/ -output s3://test-emr-bucket-kaleem/emr/output/
