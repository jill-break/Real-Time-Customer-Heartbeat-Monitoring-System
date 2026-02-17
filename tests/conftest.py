
import pytest
import sys
import os

# Ensure src is in python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture(scope="session")
def spark_session():
    """
    Creates a SparkSession for testing.
    Ensures HADOOP_HOME is set for Windows.
    """
    import os
    # Set up Hadoop environment for Windows
    hadoop_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'hadoop'))
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['hadoop.home.dir'] = hadoop_home
    os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')
    os.environ['PYSPARK_PYTHON'] = "python"
    os.environ['PYSPARK_DRIVER_PYTHON'] = "python"

    import sys
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    from pyspark.sql import SparkSession
    import tempfile
    
    warehouse_dir = tempfile.mkdtemp()
    
    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[1]") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()
    import shutil
    try:
        shutil.rmtree(warehouse_dir)
    except OSError:
        pass
