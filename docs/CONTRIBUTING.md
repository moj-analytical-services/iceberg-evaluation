# Contributing

## Python environment

1. Create a new virtual environment with Python 3.9 (this is the latest python version which is compatible with Glue Interactive sessions)

        python -m venv env

2.  Activate the virtual environment

        source env/bin/activate

3.  Install the python dependencies

        pip install -r requirements.txt

## Running Jupyter notebooks locally

### Using a PySpark kernel

[AWS Glue interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-overview.html) provide an on-demand, highly-scalable, serverless Spark backend to Jupyter notebooks. This enables you to author code in your local environment and run it seamlessly on the interactive sessions backend.

To install, follow these [instructions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html#interative-sessions-windows-instructions). 

For more detailed instructions and a demo see [introducing-aws-glue-interactive-sessions-for-jupyter](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-interactive-sessions-for-jupyter/).

### Using Jupyter extension for VSCode

[Jupyter extension for VSCode](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter) provides basic Jupyter notebook support.

For features and benefits of using the extension please refer to this [link](https://code.visualstudio.com/docs/datascience/jupyter-notebooks)

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
