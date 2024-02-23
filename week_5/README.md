# Installing Java

Download OpenJDK 11 or Oracle JDK 11 (It's important that the version is 11 - spark requires 8 or 11)

We'll use [OpenJDK](https://jdk.java.net/archive/)

Download it (e.g. to `~/spark`):

```bash
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
```

Unpack it:

```bash
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz && rm openjdk-11.0.2_linux-x64_bin.tar.gz
```

After installation, you need to set the JAVA_HOME environment variable to point to the JDK installation directory in your `.bash_profile` using comand `nano .bash_profile`: 

```bash
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
```

Then, reload the shell configuration:

```bash
source .bash_profile
```

# Test Installation

Open a terminal window and run the following command to ensure Spark is installed correctly:

`spark-shell`

This will launch the Spark shell, and you should see the Spark logo and a Scala prompt (scala>). You can type `:q` to exit the shell.

To run `PySpark`, we first need to add it to `PYTHONPATH`:

```bash
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```