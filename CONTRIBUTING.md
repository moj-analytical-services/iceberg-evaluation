# Contributing

## Python environment

1. Create a new virtual environment with Python 3.9 (this is the latest python version which is compatible with Glue Interactive sessions)

        python -m venv env

2.  Activate the virtual environment

        source env/bin/activate

3.  Install the python dependencies

        pip install -r requirements.txt

## Running Jupyter notebooks locally

### Using a python kernel

To populate

### Using a PySpark kernel

[AWS Glue interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions-overview.html) provide an on-demand, highly-scalable, serverless Spark backend to Jupyter notebooks. This enables you to author code in your local environment and run it seamlessly on the interactive sessions backend.

To install, follow these [instructions](https://docs.aws.amazon.com/glue/latest/dg/interactive-sessions.html#interative-sessions-windows-instructions).

### Using Jupyter extension for VSCode with aws-vault

You can use [py-aws-vault-auth](https://github.com/achimgaedke/py-aws-vault-auth) to update the running process environment and authenticate for AWS services with [aws-vault](https://github.com/99designs/aws-vault) when using the Jupyter extension for VScode:

```
import py_aws_vault_auth
import os
environ_auth = py_aws_vault_auth.authenticate("sso-data-prod", prompt="python", return_as="environ")
os.environ.update(environ_auth)
```
