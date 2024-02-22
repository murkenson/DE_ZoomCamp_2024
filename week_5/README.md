# Test Installation

Open a terminal window and run the following command to ensure Spark is installed correctly:

`spark-shell`

This will launch the Spark shell, and you should see the Spark logo and a Scala prompt (scala>). You can type `:q` to exit the shell.

To run `PySpark`, we first need to add it to `PYTHONPATH`:

```shell
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```