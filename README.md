databricks-unittest-sample
---

Setup apache-spark

```
mkdir -p ~/.apache-spark
cd ~/.apache-spark
wget https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz
tar zxf spark-2.4.3-bin-hadoop2.7.tgz
```

Setup environment

```
python -m venv venv
. venv/bin/activate
pip install -r requirements.txt
pip install -r test-requirements.txt
```

Run unit test

```
export SPARK_HOME=~/.apache-spark/spark-2.4.3-bin-hadoop2.7
flake8 .
mypy .
pytest .
```

