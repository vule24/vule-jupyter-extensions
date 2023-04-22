from .sparksql import SparkSQL

def load_ipython_extension(ipython):
    ipython.register_magics(SparkSQL)