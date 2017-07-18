import pyhs2
import logging
from collections import Counter
from google.cloud import bigquery

server = 'shd-hdp-a-master-003.bolcom.net'
myDatabase = 'ldebruijn'
bq_database = 'bidwh2'
#  myTable = 'ppp_retail_promotion_reference_group'
myTable = 'PPP_retail_promotion'

# 10 000 rows should be good enough to use as a sample
#  (Google uses some sample of 1000 or 3000 rows)
#  Estimation of memory for the Counters: 10000 * 10 * ( 10 * 4 + 4) * 4 = 16.8 MB
#  (based on estimation : rows * columns * ( size string + int) * overhead Counter )
sample_rows_number = 10000
sample_column_number = 10
max_percent_most_frequent_value_in_column = 1  # if in one sample a column has a value whose frequency is highest than
#  this percentage, then this column is discarded
number_of_most_frequent_values_to_weight = 50

number_of_group_by = 100000  # 7999 is the limit if you want to manually download the data from BQ. This limit does
#  not apply in this script because we fetch the data with the Python API instead.

fullHiveTable = myDatabase + "." + myTable
fullBqTable = bq_database + "." + myTable
conn = pyhs2.connect(host=server, port=10000, authMechanism="KERBEROS", database=myDatabase)

ddlColumns = []  # array instead of dictionary because we want to maintain the order of the columns
ddlPartitions = []

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s]\t[%(asctime)s]\t(%(threadName)-10s) %(message)s',)


def get_table_ddl(table):
    """ Get the DDL of the (Hive) table and store it into arrays"""
    is_col_def = True
    cur = conn.cursor()
    cur.execute("describe " + table)
    while cur.hasMoreRows:
        row = str(cur.fetchone())  # TODO check if we should not do fetchall instead, or other fetch batch
        if row == 'None':
            continue
        c_row = row.replace("[", "").replace("]", "").replace("'", "")
        s_row = c_row.split(",")
        col_name = s_row[0].strip()
        col_type = s_row[1].strip()  # TODO can we use the fact that row is initially a list?

        if col_name == "" or col_name == "None":
            continue
        if col_name.startswith('#'):
            if "Partition Information" in col_name:
                is_col_def = False
            continue

        my_dic = {"name": col_name, "type": col_type}
        if is_col_def:
            ddlColumns.append(my_dic)
        else:
            ddlPartitions.append(my_dic)  # take care, those rows also appear in the columns array
    cur.close()


def get_sample_query(table):
    """ Build a SQL query that allows to get some sample lines with limited amount of columns"""
    query = "SELECT"
    selected_columns = ddlColumns[:sample_column_number]
    for col in selected_columns:
        query = query + " " + col["name"] + ","  # for the last column we'll remove that trailing ","
    query = query[:-1] + " FROM " + table + " LIMIT " + str(sample_rows_number)
    return query, selected_columns


def find_groupby_column(table):
    """Use a sample to return a column that seems enough spread to do interesting GROUP BY on it"""
    query, selected_columns = get_sample_query(table)

    #  Get a sample from the table and fill Counters to each column
    logging.info("Analyzing the columns %s with a sample of %i values", str([x["name"] for x in selected_columns]),
                 sample_rows_number)
    for col in selected_columns:
        col["Counter"] = Counter()
    cur = conn.cursor()
    cur.execute(query)
    while cur.hasMoreRows:
        fetched = cur.fetchone()
        if fetched is None:
            continue
        index = 0
        for col in selected_columns:
            value_column = fetched[index]
            col["Counter"][value_column] += 1  # TODO what happens with NULL?
            index += 1
    cur.close()

    #  Look at the statistics to estimate which column is the best to do a GROUP BY
    max_frequent_number = sample_rows_number * max_percent_most_frequent_value_in_column / 100
    minimum_weight = sample_rows_number
    highest_first = max_frequent_number
    column_with_minimum_weight = None
    for col in selected_columns:
        highest = col["Counter"].most_common(1)[0]
        if highest[1] > max_frequent_number:
            logging.debug("Discarding column '%s' because '%s' was found in sample %i times (higher than limit of %i)",
                          col["name"], highest[0], highest[1], max_frequent_number)
            continue
        # The biggest value is not too high, so let's see how big are the 50 biggest values
        weight_of_most_frequent_values = sum([x[1] for x in col["Counter"]
                                             .most_common(number_of_most_frequent_values_to_weight)])
        logging.debug("%s %s", col["name"], weight_of_most_frequent_values)
        if weight_of_most_frequent_values < minimum_weight:
            column_with_minimum_weight = col["name"]
            minimum_weight = weight_of_most_frequent_values
            highest_first = highest[1]
    logging.info("Best column to do a GROUP BY is %s (%i / %i)", column_with_minimum_weight, highest_first,
                 minimum_weight)
    return column_with_minimum_weight


'''
create temp function hash2(v STRING)
returns INT64
LANGUAGE js AS """
  var myHash = 0
  for (let c of v){
    myHash = myHash * 31 + c.charCodeAt(0)
    if (myHash >= 4294967296){ // because in Hive hash() is computed on integers range
      myHash = myHash % 4294967296
    }
  }
  if (myHash >= 2147483648){
    myHash = myHash - 4294967296
  }
  return myHash
""";
'''


def get_groupby_count_sql(hive_table, bq_table, column):
    hive_query = "SELECT hash(" + column + ") % " + str(number_of_group_by) + " as gb, count(*) as count " \
                 "FROM " + hive_table + " GROUP BY hash(" + column + ") % " + str(number_of_group_by)
    logging.debug("Hive query is: %s", hive_query)

    bq_query = '''create temp function hash2(v STRING)
returns INT64
LANGUAGE js AS """
  var myHash = 0
  for (let c of v){
    myHash = myHash * 31 + c.charCodeAt(0)
    if (myHash >= 4294967296){ // because in Hive hash() is computed on integers range
      myHash = myHash % 4294967296
    }
  }
  if (myHash >= 2147483648){
    myHash = myHash - 4294967296
  }
  return myHash
""";
    ''' + "SELECT MOD( hash2( " + column + "), " + str(number_of_group_by) + ") as gb, count(*) as count " \
        "FROM " + bq_table + " GROUP BY gb ORDER BY gb"
    logging.debug("BigQuery query is: %s", bq_query)
    return hive_query, bq_query


def compare_groupby_count(hive_table, bq_table, column):
    """Runs a light query on Hive and BigQuery to check if the counts match, using the ideal column estimated before"""
    hive_query, bq_query = get_groupby_count_sql(hive_table, bq_table, column)

    # TODO Hive & BQ should run on separated threads
    #cur = conn.cursor()
    #cur.execute(hive_query)
    #cur.close()

    # Querying BQ
    logging.debug("Starting to launch BigQuery query")
    bigquery_client = bigquery.Client()
    q = bigquery_client.run_sync_query(bq_query)
    q.use_legacy_sql = False
    q.run()
    logging.debug("Starting to fetch BigQuery results")
    r = q.fetch_data()
    bq_count_dict = {}
    for row in r:
        bq_count_dict[row[0]] = row[1]
    logging.debug("All %i BQ rows fetched", len(bq_count_dict.keys()))


get_table_ddl(fullHiveTable)
group_by_column = find_groupby_column(fullHiveTable)
compare_groupby_count(fullHiveTable, fullBqTable, group_by_column)
