# hive_compared_bq

hive_compared_bq is a Python program that compares 2 (SQL like) tables, and graphically shows the rows/columns that are different if any are found.

Currently, there is no solution available on the internet that allows to compare the full content of some tables in a scalable way.

hive_compared_bq tackles this problem by:
* Not moving the data, avoiding long data transfer typically needed for a "Join compared" approach. Instead, only the aggregated data is moved.
* Working on the full datasets and all the columns, so that we are 100% sure that the 2 tables are the same
* Leveraging the tools (currently: Hive, BigQuery) to be as scalable as possible
* Showing the developer in a graphical way the differences found, so that the developer can easily understands its mistakes and fix them

# Table of contents
  * [Features](#features)
  * [Installation](#installation)
    + [Note Python version 2.6](#note-python-version-26)
    + [Installing Python 2.7](#installing-python-27)
    + [For Hive:](#for-hive-)
      - [TODO: explain the installation of Jar + give source code](#todo--explain-the-installation-of-jar---give-source-code)
    + [For Big Query:    (steps extracted from https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)](#for-big-query------steps-extracted-from-https---cloudgooglecom-bigquery-docs-reference-libraries-client-libraries-install-python-)
  * [Usage](#usage)
    + [Get Help](#get-help)
    + [Basic execution](#basic-execution)
    + [Explanation of results](#explanation-of-results)
      - [Case of identical tables](#case-of-identical-tables)
      - [Case of number of rows not matching](#case-of-number-of-rows-not-matching)
      - [Case of differences inside the rows](#case-of-differences-inside-the-rows)
    + [Advanced executions](#advanced-executions)
      - [Faster executions](#faster-executions)
      - [Skewing problem](#skewing-problem)
      - [Schema not matching](#schema-not-matching)
      - [Problems in the selection of the GroupBy column](#problems-in-the-selection-of-the-groupby-column)
  * [Algorithm](#algorithm)

## Features

* Engine supported: Hive, BigQuery. (in theory, it is easy to extend it to other SQL backends such as Spanner, CloudSQL, Oracle... Help is welcomed :) ! )
* Possibility to only select specific columns or to remove some of them (useful if the schema between the tables is not exactly the same, or if we know that some columns are different and we don't want them to "pollute" the results)
* Possibility to just do a quick check (counting the rows in an advanced way) instead of complete checksum verification
* Detection of skew

## Installation

Make sure the following software is available:
* Python = 2.7 or 2.6 (see restrictions for 2.6)
* pyhs2 (just for Hive)
* google.cloud.bigquery (just for BigQuery)

### Note Python version 2.6
BigQuery is not supported by this version of Python (Google Cloud SDK only supports Python 2.7)

It is needed to install some backports for Python's collection. For instance, for a Redhat server it would be:
```bash
yum install python-backport_collections.noarch
```

### Installing Python 2.7
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

### For Hive:
Execute this as "root":
```bash
yum install cyrus-sasl-gssapi cyrus-sasl-devel
pip install pyhs2
```


#### TODO: explain the installation of Jar + give source code

### For Big Query:    (steps extracted from https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python)
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

## Usage

### Get Help

To see all the quick options of hive_compared_bq.py, just execute it without any arguments:
```bash
python hive_compared_bq.py
```

When calling the help, you get more detailled on the different options and also some examples:
```bash
python hive_compared_bq.py --help
```

### Basic execution

You must indicate the 2 tables to compare as arguments (the first table will be considered as the "source" table, and the second one as the "destination" table).

Each table must have the following format: "type"/"database"."table"
where:
* "type" is the technology of your database (currently, only 'hive' and 'bq' (BigQuery) are supported)
* "database" is the name of your database (also called "dataset" in BigQuery)
* "table" is of course the name of your table

About the location of those databases:
* In the case of BigQuery, the default Google Cloud project configured in your environment is selected
* In the case of Hive, you must specify the hostname of the HiveServer2, using the 'hs2' option.

Another note for Hive: you need to pass the HDFS direction of the jar of the required UDF (see installation of Hive above), using the 'jar' option.

To clarify all the above, let's consider that we want to compare the following 2 tables:
* A Hive table called "hive_compared_bq_table", inside the database "sluangsay". With those parameters, the argument to give is: "hive/sluangsay.hive_compared_bq_table". Let's suppose also that the hostname of HiveServer2 is 'master-003.bol.net', and that we installed the required Jar in the HDFS path 'hdfs://hdp/user/sluangsay/lib/sha1.jar'. Then, since this table is the first one on our command line, it is the source table and we need to define the option: -s "{'jar': 'hdfs://hdp/user/sluangsay/lib/sha1.jar'
* A BigQuery table also alled "hive_compared_bq_table", inside the dataset "bidwh2".

To compare above 2 tables, you need to execute:
```bash
python hive_compared_bq.py -s "{'jar': 'hdfs://hdp/user/sluangsay/lib/sha1.jar', 'hs2': 'master-003.bol.net'}" hive/sluangsay.hive_compared_bq_table bq/bidwh2.hive_compared_bq_table
```

### Explanation of results

#### Case of identical tables

When executing above command, if the 2 tables have exactly the same data, then we would get an output similar to:

```
[INFO]  [2017-09-15 10:59:20,851]  (MainThread) Analyzing the columns ['rowkey', 'calc_timestamp', 'categorization_step', 'dts_modified', 'global_id', 'product_category', 'product_group', 'product_subgroup', 'product_subsubgroup', 'unit'] with a sample of 10000 values
[INFO]  [2017-09-15 10:59:22,285]  (MainThread) Best column to do a GROUP BY is rowkey (apparitions of most frequent value: 1 / the 50 most frequentvalues sum up 50 apparitions)
[INFO]  [2017-09-15 10:59:22,286]  (MainThread) Executing the 'Group By' Count queries for sluangsay.hive_compared_bq_table (hive) and bidwh2.hive_compared_bq_table (bigQuery) to do first comparison
No differences where found when doing a Count on the tables sluangsay.hive_compared_bq_table and bidwh2.hive_compared_bq_table and grouping by on the column rowkey
[INFO]  [2017-09-15 10:59:48,727]  (MainThread) Executing the 'shas' queries for hive_sluangsay.hive_compared_bq_table and bigQuery_bidwh2.hive_compared_bq_table to do final comparison
Sha queries were done and no differences where found: the tables hive_sluangsay.hive_compared_bq_table and bigQuery_bidwh2.hive_compared_bq_table are equal!
```

The first 2 lines describe the first step of the algorithm (see the explanation of the algorithm later), and we see that 'rowkey' is the column that is here used to perform the "Group By" operations.
Then, the next 2 lines show the second step: we count the number of rows for each value of 'rowkey'. In that case, those values match for the 2 tables.
Finally, we do the extensive computation of the SHAs for all the columns and rows of the 2 tables. At the end, the script tells us that our 2 tables are identical.
We can also check the returns value of the script: it is 0, which means no differences.

#### Case of number of rows not matching

The first difference that can be observed is that the number of rows associated to (at least) one GroupBy value does not match between the 2 tables.
In such case, the standard output would be similar to:

```
[INFO]	[2017-09-18 08:09:06,947]  (MainThread) Analyzing the columns ['rowkey', 'calc_timestamp', 'categorization_step', 'dts_modified', 'global_id', 'product_category', 'product_group', 'product_subgroup', 'product_subsubgroup', 'unit'] with a sample of 10000 values
[INFO]	[2017-09-18 08:09:07,739]  (MainThread) Best column to do a GROUP BY is rowkey (apparitions of most frequent value: 1 / the 50 most frequentvalues sum up 50 apparitions)
[INFO]	[2017-09-18 08:09:07,739]  (MainThread) Executing the 'Group By' Count queries for sluangsay.hive_compared_bq_table2 (hive) and bidwh2.hive_compared_bq_table2 (bigQuery) to do first comparison
[INFO]	[2017-09-18 08:09:35,392]  (MainThread) We found at least 3 differences in Group By count
```

And the return value of the script would be 1.

Some of the differences are also shown in a HTML file that is automatically opened in your browser:

![alt text](docs/images/differences_count.png?raw=true "Differences in GroupBy numbers")

This shows a table, with the rows of 1 table on the right side, and the ones of the other table on the right side.
The names of the table appear at the top of the table.
There we can also see the names of the columns that are shown.
For performance reason and also sake of brevity, only some 7 columns are shown.
The first 2 columns are "special columns": the GroupBy column (2nd column), and just before its SHA1 value (1st column).
In this example, we can see that only rows on the left side appear: this is because the table on the right does not contain rows that contain the rowkeys 21411000029, 65900009 and 6560009.

#### Case of differences inside the rows

TODO

python hive_compared_bq.py -s "{'jar': 'hdfs://hdp-b/user/sluangsay/lib/sha1.jar', 'hs2': 'shd-hdp-b-master-003.bolcom.net'}" hive/sluangsay.hive_compared_bq_table3 bq/bidwh2.hive_compared_bq_table3

### Advanced executions

#### Faster executions

By default, the script executes 3 steps (see explanation on how the algorithm executes).
You can run the script faster by removing some steps that you think are unnecessary:

* with the '--group-by-column' option, you can indicate directly which column you want to use for the Group By.
That means that the first step of analyzing some sample of 10 columns won't be needed.
This step won't usually make a huge difference in the execution time (it usually takes 1-2 seconds), but by doing this you avoid launching a query that might cost you some money (example of BigQuery).
With this option, you might also be able to provide a better column that the one the script would have discovered by itself, which might speed up the following queries (by avoiding Skew for instance, see notes later).

* with the '--just-count' option, you say that you just want to do a 'count rows validation'.
This is some kind of "basic validation" because you won't be sure that the columns have some correct values.
But that allows you to avoid the final "full SHAs validation" step, that is more expensive and timely to compute.
And maybe this 'count' validation' is enough for you now, because you are at an early stage of development, and you don't need to be 100% sure of your data
(checking the count of rows is also a good idea to double check if some JOINs or some Filters conditions work properly).
Don't forget to eventually run a full SHAs validation when you finish developing.

* with '--just-sha', you specify that you don't need the 'count' validation. If you know from previous executions that the counts are correct, then you might indeed to skip that previous step.
However, it is a bit at your own risk, because if the counts are not correct, the script will fail but you will have executed a more complex/costly query for that ('count' validation use faster/cheaper queries).

Another solution to have your validation being executed faster is to limit the scope of your validations. If you decide to validate less data, then you need to process less data, meaning that your queries will be faster/cheaper:

* for instance, you might be interested in just validating some specific critical columns (maybe because you know that your ETL process does not make any changes on some columns, so why "validating" them?).
In such case, you can use the '--column-range', '--columns' or '--ignore-columns' options.

* another example would be just validating some specific partitions. If your data is partitioned by days for instance, then you might decide to only validate 1 day of data.
In such case you have to use the '--source-where' and '--destination-where' to specify a Where condition that tells which partition you want to consider.

#### Skewing problem
TODO

#### Schema not matching
TODO

#### Problems in the selection of the GroupBy column

The first step of the algorithms tries to estimate the best "GroupBy" column (see explanations about Algorithm below).
However, this estimation can go wrong and in such case the following message appears:
```
Error: we could not find a suitable column to do a Group By. Either relax the selection condition with the '--max-gb-percent' option or directly select the column with '--group-by-column'
```

If you face this problem, then just follow one of the 2 solutions proposed in the message above.<br/>
The best solution is obviously to have some knowledge about the data and to directly indicate to the script which column is the best one to do some GroupBy, using the '--group-by-column' option.<br/>
The other possibility is to use the '--max-gb-percent' option and to make it higher than the default value (1%), in order to allow a bit less homogeneous distribution in the data of the GroupBy column and thus have more probability to find a GroupBy column.

## Algorithm

The goal of hive_compared_bq was to avoid all the shortcomings of previous approaches that tried to solve the same comparison problem.
Some of those approaches are:
* Manual check, comparing some few rows and counting the total number of rows.<br/>
Problem of this approach is obviously that it cannot work with a certain amount of data or with tables with lot of columns. So the check cannot be complete.
* Doing some "JOINs" comparisons between the 2 tables.<br/>
The drawback of this is that Joins are some quite slow operations in general. What is more, it is very difficult to do a faire comparison when there are multiple rows with the same "Join column".
The last inconvenient is that you have to ensure that the 2 tables are located on the same place, which might force you to transfer the data from 1 database to another one (ex: from a Hive database to a BigQuery database), which takes a long time if the table is huge.
* Sorting all the rows and doing a Diff between them (just like described here: https://community.hortonworks.com/articles/1283/hive-script-to-validate-tables-compare-one-with-an.html ).<br/>
The inconvenient with this is also that the 2 tables have to be located on the same place. What is more, the "Diff" operations occurs in memory which means that this approach does not work with huge tables.

To solve above problems, it was decided to:
* develop an algorithm that would leverage the BigData backends (Hive, BigQuery or others): all the complicated computations must be performed on those engines that naturally scale.<br/>
That means that the goal of the algorithm is to generate some complex SQL queries that will be sent to those backends, and then that will compare the results of those queries.
Those results must be small, otherwise the Python program would not be able to do the final comparison in memory.
* compare all the rows and all the columns of the tables.<br/>
To ensure that we can have some relatively small amount of results (see reason just above) coming from a huge amount of data, we need to "reduce" the results.
Using some checksums (SHA1) algorithm seems appropriate because it helps in doing that reduction and also in giving a high confidence in the validity of the comparison.

In summary, the idea of the programs is just to compute locally to each table all the checksums of all the rows/columns, to "Group BY" those checksums and generate some global checksums on top of them.<br/>
And then, all those general checksums will be transferred back to the program where the final comparison will be made and where potential differences will be shown to the user.
The following simplified pseudo SQL query summarizes a bit this idea:

    WITH blocks AS (
        SELECT MOD( hash2( column), 100000) as gb, sha1(concat( col0, col1, col2, col3, col4)) as block_0,
          sha1(concat( col5, col6, col7, col8, col9)) as block_1, ... as block_N FROM table
    ),
    full_lines AS (
        SELECT gb, sha1(concat( block_0, |, block_1...) as row_sha, block_0, block_1 ... FROM blocks
    )
    SELECT gb, sha1(concat(list<row_sha>)) as sline, sha1(concat(list<block_0>)) as sblock_1,
        sha1(concat(list<block_1>)) as sblock_2 ... as sblock_N FROM GROUP BY gb

The real query is obviously a bit more complex, because we need to take care about type conversions, NULL values and the ordering of the rows for the same GroupBy values.<br/>
If you feel interested in seeing the real query, launch the script with the '--verbose' option.

The result of above query is stored in a temporary table. The size of this table is small (because we take a modulo of the HASH of the groupBy column, and because the number of columns is limited).<br/>
Therefore, because this amount of data is reasonable, we can allow ourselves to download those results locally where our Python program is being executed.

In the 2 temporary tables, the first 2 columns (gb and sline) are the most important.<br/>
"gb" is used as a kind of "index". "sline" is the total checksum that
corresponds to all the columns of all the rows whose GroupBy value modulo 100000 matches the "gb" value.<br/>
If "sline" differs between the 2 tables for a same "gb" value, then we know that some rows in that "GroupBy-modulo block" don't match between the 2 tables.<br/>
Then, the program queries again the temporary tables and ask all the columns for all the specific "gb" values that have a difference.<br/>
The idea is to discover which "block of columns" (called "sblock_N" in the previous pseudo query) are different.<br/>
By doing so, we are able to know not only which rows are different, but also in which columns those differences appear.<br/>
This allows us to launch some final simple SELECT queries against the original tables, fetching those rows and those specific columns.<br/>
Finally, those 2 small datasets are sorted and a difference of them is exposed in a web browser.

The pseudo SQL query shown above is quite heavy to compute, and has to access all the data of each table.
In order to avoid launching such a heavy query in case of "stupid mistakes" (maybe your ETL flow failed and there is 1 table that is void), a quick comparison step has been introduced before.
In general, we can divide the program in 3 steps:

* Determination of the GroupBy column.<br/>
In this first step, we try to assess which column is more suitable to be used as a GroupBy column.<br/>
It is important to find a column that has many different values (otherwise, the next queries will be poorly parallelized, and we can have some OOM problems).<br/>
We want also to avoid some skewed columns.<br/>
To do so, the program fetch a sample of data from 1 table: some 10 000 rows and some 10 columns. We only look at 10 columns for 3 reasons: 1) we suppose that it would be enough to find there a good one 2) to limit the amount of data transfered over the network 3) to avoid being billed too much (for instance, the price in BigQuery grows with the number of columns being read).<br/>
Finally, an analysis is done on those columns, we discard all the columns that don't have enough diversity or that are too skewed, and from the remaining columns we keep the one that seems less skewed.
Working on a sample has some limits and it is possible that the chosen column is not the best one. It is also possible that all the 10 columns are discarded.<br/>
In both situation you can use the '--group-by-column' option to overcome that problem.

* Quick count check.<br/>
The program does a first validation to quickly check, in a light way if there are some big differences in the 2 tables.<br/>
This is why we just try to count the number of rows in the tables. More than counting the total number of rows, we will check the number of rows for each "row-bucket", that is: group of rows with a GroupBy value whose hash modulo 10000 are the same.<br/>
This verification is fast because it just need to do some small computation on just 1 column. There is indeed no point in trying to do some very complex computation on all the columns if the number of rows does not match.<br/>
In such case, the differences are shown to the user and the program stops, without executing the 3rd step.<br/>
During this step, a skew detection is also performed, which is a protection for the 3rd step.

* Full SHA1 validation.<br/>
This step, described earlier in this paragraph, is the only one that can guarantee that the 2 tables are identical.

