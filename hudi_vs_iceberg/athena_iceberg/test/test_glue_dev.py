import moto
import pytest
import glue_dev

def test_save_glue_script():
    glue_dev.save_glue_script()

def test_run_glue_script():
    glue_dev.run_glue_script()