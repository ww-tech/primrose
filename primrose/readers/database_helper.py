"""Helper function for get os.environ values

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import os

def get_env_val(key):
    """get environmental variable

    Returns:
        env variable (str)

    """
    val = os.environ.get(key)
    if val is None:
        raise Exception("Did not find env variable for " + str(key))
    return val

