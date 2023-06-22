# Testing

## Create the test data

Run `create_test_data.py` in python interactive mode


## Run the tests

To test the glue-based functions:

```
WORKSPACE_LOCATION=$PWD/hudi_vs_iceberg

docker run -it -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
    -e AWS_DEFAULT_REGION=eu-west-1 \
    -e AWS_REGION=eu-west-1 \
    -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e DISABLE_SSL=true \
    -e DATALAKE_FORMATS=hudi \
    --rm -p 4040:4040 -p 18080:18080 \
    --name glue_pytest amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    -c "python3 -m pytest tests/test_glue_hudi.py"

docker run -it -v $WORKSPACE_LOCATION:/home/glue_user/workspace/ \
    -e AWS_DEFAULT_REGION=eu-west-1 \
    -e AWS_REGION=eu-west-1 \
    -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e DISABLE_SSL=true \
    -e DATALAKE_FORMATS=iceberg \
    --rm -p 4040:4040 -p 18080:18080 \
    --name glue_pytest amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    -c "python3 -m pytest tests/test_glue_iceberg.py"
...

```

Use the python environment created by `requirements-python.txt` to test the athena-based functions:

```
pytest hudi_vs_iceberg/tests/test_athena_hudi.py --log-cli-level=INFO -vv
```