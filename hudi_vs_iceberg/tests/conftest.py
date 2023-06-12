def pytest_addoption(parser):
    parser.addoption(
        '--compute', action='store', help='options are glue_iceberg, glue_hudi or athena_iceberg'
    )

