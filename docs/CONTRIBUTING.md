# Contributing

## Python environment

1. Create a new virtual environment with Python 3.9 (this is the latest python version which is compatible with Glue Interactive sessions)

        python -m venv env

2.  Activate the virtual environment

        source env/bin/activate

3.  Install the python dependencies

        pip install -r requirements-python.txt

This will allow you to run the Athena and pandas-based examples.

## AWS Glue Interactive session

[AWS Glue interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-overview.html) provide an on-demand, highly-scalable, serverless Spark backend to Jupyter notebooks. This enables you to author code in your local environment and run it seamlessly on the interactive sessions backend.

To install, follow these [instructions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html#interative-sessions-windows-instructions). You will need to create a separate environment from the python environment. The python dependencies are saved in `requirements-spark.txt`.

For more detailed instructions and a demo see [introducing-aws-glue-interactive-sessions-for-jupyter](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-interactive-sessions-for-jupyter/).

Warning: Remove any `default` profiles in your ~/.aws/config or ~/.aws/credentials otherwise the notebook will fail to provision a glue session. 

## Using Jupyter extension for VSCode

[Jupyter extension for VSCode](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) provides Jupyter notebook support, allowing you to make use of Visual Studio's features such Intellisense. For more features and benefits of using the extension please refer to this [link](https://code.visualstudio.com/docs/datascience/jupyter-notebooks).

In order to be able to use the PySpark kernel, you might need to update the kernel specs following [these instructions](https://stackoverflow.com/questions/73994337/cannot-find-pyspark-kernel-aws-glue-interactive-sessions-with-vscode).

To authenticate you will first need to obtain the AWS session tokens using aws-vault and save it to your .env file from the terminal:

```
aws-vault exec sso-sandbox
env | grep AWS > .env
```

You then set these as environment variables in your jupyter notebook using the [dotenv](https://github.com/theskumar/python-dotenv) python module:

```
%load_ext dotenv
%dotenv
```

## Using the AWS Glue docker image

Some parts of this repository have been developed using the AWS Glue docker image (e.g. the [dummy database creator](../hudi_vs_iceberg/helpers/dummy_database_creator/)). AWS guidance on how to use this image can be found [here](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image) and detailed instructions on its use in this repo can be found in each relevant subdirectory.

Please note that when using AWS Vault you do not need to mount your local `.aws` directory. Rather you can pass your AWS environment variables when executing `docker run`.

For example to run the docker image with JupyterLab:

```
WORKSPACE_LOCATION=$PWD/hudi_vs_iceberg

docker run -it -v $WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/ \
    -e AWS_DEFAULT_REGION=eu-west-1 \
    -e AWS_REGION=eu-west-1 \
    -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN \
    -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    -e DISABLE_SSL=true \
    --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 \
    --name glue_pyspark amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
    /home/glue_user/jupyter/jupyter_start.sh
```

Open http://127.0.0.1:8888/lab in your web browser in your local machine to see the Jupyter lab UI.