# hive_compared_bq

hive_compared_bq is a Python program that compares 2 (SQL like) tables, and graphically shows the rows/columns that are different if any are found.

Currently, there is no solution available on the internet that allows to compare the full content of some tables in a scalable way.

hive_compared_bq tackes this problem by:

* Not moving the data, avoiding long data transfer typically needed for a "Join compared" approach. Instead, only the aggregated data is moved.

* Working on the full datasets and all the columns, so that we are 100% sure that the 2 tables are the same

* Leveraging the tools (currently: Hive, BigQuery) to be as scalable as possible

* Showing the developer in a graphical way the differences found, so that the developer can easily understands its mistakes and fix them

### Features

* Engine supported: Hive, BigQuery. (in theory, it is easy to extend it to other SQL backends such as Spanner, CloudSQL, Oracle... Help is welcomed :) ! )

* Possibility to only select specific columns or to remove some of them (useful if the schema between the tables is not exactly the same, or if we know that some columns are different and we don't want them to "pollute" the results)

* Possibility to just do a quick check (counting the rows in an advanced way) instead of complete checksum verification

* Detection of skew


### TODO explain a bit algorithm + show images of results + explain how to use it

### Installation

Make sure the following software is available:

* Python = 2.7 or 2.6 (see restrictions for 2.6)

* pyhs2 (just for Hive)

* google.cloud.bigquery (just for BigQuery)

#### Note Python version 2.6
BigQuery is not supported by this version of Python (Google Cloud SDK only supports Python 2.7)

It is needed to install some backports for Python's collection. For instance, for a Redhat server it would be:
```bash
yum install python-backport_collections.noarch
```

#### Installing Python 2.7
On RHEL6, Python 2.6 was installed by default. And the RPM version in EPEL for Python2.7 had no RPM available, so I compiled it this way:
```bash
su -c "yum install gcc gcc-c++ zlib-devel sqlite-devel openssl-devel"
wget https://www.python.org/ftp/python/2.7.13/Python-2.7.13.tgz
tar xvfz Python-2.7.13.tgz
cd Python-2.7.13
./configure --with-ensurepip=install
make
su -c "make altinstall"
su -c "echo 'export CLOUDSDK_PYTHON=/usr/local/bin/python2.7' >> /etc/bashrc"
export CLOUDSDK_PYTHON=/usr/local/bin/python2.7
```

#### For Hive:
Execute this as "root":
```bash
yum install cyrus-sasl-gssapi cyrus-sasl-devel
pip install pyhs2
```

#### For Big Query:    (steps extracted from https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)
To execute below commands, make sure that you already have a Google Cloud account with a BigQuery project created.

```bash
mkdir -p ~/bin/googleSdk
cd ~/bin/googleSdk
su -c "pip install --upgrade google-cloud-bigquery"

wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-170.0.0-linux-x86_64.tar.gz
tar xvfz google-cloud-sdk-170.0.0-linux-x86_64.tar.gz 

./google-cloud-sdk/bin/gcloud init
./google-cloud-sdk/bin/gcloud auth application-default login
./google-cloud-sdk/install.sh
```

This last command tells you to source 2 files in every session. So include them in your bash profile. For instance in my case it would be:
```bash
echo 'source /home/sluangsay/bin/googlesdk/google-cloud-sdk/path.bash.inc' >> ~/.bash_profile
echo 'source /home/sluangsay/bin/googlesdk/google-cloud-sdk/completion.bash.inc' >> ~/.bash_profile
```
Open a new bash session to activate those changes


# TODO: explain the installation of Jar + give source code



