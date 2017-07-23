import pyhs2
import logging
import threading
import difflib
import sys
import webbrowser
from collections import Counter
from google.cloud import bigquery

server = 'shd-hdp-b-master-003.bolcom.net'
#  myDatabase = 'ldebruijn'
myDatabase = 'sluangsay'
bq_database = 'bidwh2'
#  myTable = 'PPP_retail_promotion_reference_group'
#  myTable = 'PPP_retail_promotion'
myTable = 'hive_compared_bq_table2'

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
#  TODO ideally this limit would be dynamic (counting the total rows), in order to get an average of 7 lines per bucket

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s]\t[%(asctime)s]  (%(threadName)-10s) %(message)s',)

fullHiveTable = myDatabase + "." + myTable
fullBqTable = bq_database + "." + myTable
conn = pyhs2.connect(host=server, port=10000, authMechanism="KERBEROS", database=myDatabase)
bigquery_client = bigquery.Client()

hash2_js_udf = '''create temp function hash2(v STRING)
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

# TODO put this into a real object to have type check!?
ddlColumns = []  # array instead of dictionary because we want to maintain the order of the columns
ddlPartitions = []  # take care, those rows also appear in the columns array


def get_table_ddl(table):
    """ Get the DDL of the (Hive) table and store it into arrays"""
    is_col_def = True
    cur = conn.cursor()
    cur.execute("describe " + table)
    while cur.hasMoreRows:
        row = cur.fetchone()  # TODO check if we should not do fetchall instead, or other fetch batch
        if row is None:
            continue
        col_name = row[0]
        col_type = row[1]

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
            ddlPartitions.append(my_dic)
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
        if fetched is not None:
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


def get_groupby_count_sql(hive_table, bq_table, column):
    hive_query = "SELECT hash(" + column + ") % " + str(number_of_group_by) + " as gb, count(*) as count " \
                 "FROM " + hive_table + " GROUP BY hash(" + column + ") % " + str(number_of_group_by)
    logging.debug("Hive query is: %s", hive_query)

    bq_query = hash2_js_udf + "SELECT MOD( hash2(%s), %i) as gb, count(*) as count FROM %s GROUP BY gb ORDER BY gb" % \
                              (column, number_of_group_by, bq_table)
    logging.debug("BigQuery query is: %s", bq_query)
    return hive_query, bq_query


def query_bq(query):
    """Execute the received query in BigQuery and return an iterate Result object"""
    logging.debug("Launching BigQuery query")
    q = bigquery_client.run_sync_query(query)
    q.timeout_ms = 60000  # 1 minute to execute the query in BQ should be more than enough
    q.use_legacy_sql = False  # TODO use maxResults https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query?
    q.run()
    logging.debug("Fetching BigQuery results")
    return q.fetch_data()


def query_hive(query):
    """Execute the received query in Hive and return the cursor which is ready to be fetched and MUST be closed after"""
    logging.debug("Launching Hive query")
    cur = conn.cursor()
    cur.execute(query)
    logging.debug("Fetching Hive results")
    return cur


def compare_groupby_count(hive_table, bq_table, column):
    """Runs a light query on Hive and BigQuery to check if the counts match, using the ideal column estimated before"""
    logging.info("Executing the 'Group By' Count queries in Hive and BigQuery to do first comparison")
    hive_query, bq_query = get_groupby_count_sql(hive_table, bq_table, column)

    def launch_hive():
        """Utility to Thread"""
        cur = query_hive(hive_query)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                hive_count_dict[row[0]] = row[1]
        logging.debug("All %i Hive rows fetched", len(hive_count_dict.keys()))
        cur.close()

    def launch_bq():
        """Utility to Thread"""
        r = query_bq(bq_query)
        for row in r:
            bq_count_dict[row[0]] = row[1]
        logging.debug("All %i BQ rows fetched", len(bq_count_dict.keys()))

    bq_count_dict = {}
    hive_count_dict = {}
    t_bq = threading.Thread(name='bqGroupBy', target=launch_bq)
    t_hive = threading.Thread(name='hiveGroupBy', target=launch_hive)
    t_bq.start()
    t_hive.start()
    t_bq.join()
    t_hive.join()

    # #### Let's compare the count between the 2 Group By queries
    # iterate on biggest dictionary so that we're sure to se a difference if there is one
    logging.debug("Searching differences in Group By")
    if len(bq_count_dict.keys()) > len(hive_count_dict.keys()):
        big_dict = bq_count_dict
        small_dict = hive_count_dict
        bq_biggest_distribution = True
    else:
        big_dict = hive_count_dict
        small_dict = bq_count_dict
        bq_biggest_distribution = False
    differences = Counter()
    for (k, v) in big_dict.iteritems():
        if k not in small_dict:
            differences[k] = -v  # we want to see the differences where we have less lines to compare
        elif v != small_dict[k]:
            differences[k] = -v - small_dict[k]
    summary_differences = [(k, -v, big_dict[k]) for (k, v) in differences.most_common()]
    if len(summary_differences) != 0:
        logging.info("We found at least %i differences in Group By count", len(summary_differences))
        logging.debug("Differences in Group By count are: %s", summary_differences[:300])
    return summary_differences, bq_biggest_distribution


def show_results_count(hive_table, bq_table, gb_column, differences, bq_biggest):
    """If any differences found in the Count Group By step, then show them to the user"""
    if len(differences) == 0:
        print "No differences where found when doing a Count on the tables %s and %s and grouping by on the column " \
              "%s" % (hive_table, bq_table, gb_column)
        return True  # means that we should continue executing the script
    # We want to return at most 6 blocks of lines corresponding to different group by values. For the sake of brevity,
    # each block should not show more than 40 lines. Blocks that show rows only on BQ or only on Hive should be limited
    # to 3 (so that we can see "context" when debugging). To also give context, we will show some few other columns.
    number_buckets_only_one_table = 0
    number_buckets_found = 0
    buckets_hive = []
    buckets_bq = []
    for (bucket, total, biggest_num) in differences:
        if total > 40:
            break  # since the Counter was ordered from small differences to biggest, we know that this difference
            # number can only increase. So let's go out of the loop
        if biggest_num + (biggest_num - total) > 40:  # "biggest_num - total" = number of lines in small dictionary
            continue
        if total == biggest_num:
            if number_buckets_only_one_table == 3:
                continue
            else:
                number_buckets_only_one_table += 1
                number_buckets_found += 1
                if number_buckets_found == 6:
                    break
                if bq_biggest:
                    buckets_bq.append(bucket)
                else:
                    buckets_hive.append(bucket)
        else:
            buckets_bq.append(bucket)
            buckets_hive.append(bucket)
            number_buckets_found += 1
            if number_buckets_found == 6:
                break

    logging.debug("Buckets for Hive: %s \t\tBuckets for BQ: %s", str(buckets_hive), str(buckets_bq))
    extra_columns = [x["name"] for x in ddlColumns[:6]]
    if gb_column in extra_columns:
        extra_columns.remove(gb_column)
    else:
        extra_columns = extra_columns[:-1]
    extra_columns_str = str(extra_columns)[1:-1].replace("'", "")
    hive_query = "SELECT hash(%s) %% %i as bucket, %s, %s FROM %s WHERE hash(%s) %% %i IN (%s)" \
                 % (gb_column, number_of_group_by, gb_column, extra_columns_str, hive_table, gb_column,
                    number_of_group_by, str(buckets_hive)[1:-1])
    logging.debug("Hive query to show Group By Count differences is: %s", hive_query)
    bq_query = hash2_js_udf + "SELECT MOD( hash2(%s), %i) as bucket, %s, %s FROM %s WHERE MOD( hash2(%s), %i) IN (%s)" \
                              % (gb_column, number_of_group_by, gb_column, extra_columns_str, bq_table,
                                 gb_column, number_of_group_by, str(buckets_bq)[1:-1])
    logging.debug("BQ query to show Group By Count differences is: %s", bq_query)

    def launch_hive():
        """Utility to Thread"""
        cur = query_hive(hive_query)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                line = "^ " + " | ".join([str(col) for col in row]) + " $"
                hive_lines.append(line)
        logging.debug("All %i Hive rows fetched", len(hive_lines))
        cur.close()

    def launch_bq():
        """Utility to Thread"""
        r = query_bq(bq_query)
        for row in r:
            line = "^ " + " | ".join([str(col) for col in row]) + " $"
            bq_lines.append(line)
        logging.debug("All %i BQ rows fetched", len(bq_lines))

    bq_lines = []
    hive_lines = []
    t_bq = threading.Thread(name='bqShowCountDifferences', target=launch_bq)
    t_hive = threading.Thread(name='hiveShowCountDifferences', target=launch_hive)
    t_bq.start()
    t_hive.start()
    t_bq.join()
    t_hive.join()

    bq_lines.sort()
    bq_file = "/tmp/count_diff_bq"
    with open(bq_file, "w") as f:
        f.write("\n".join(bq_lines))
    hive_lines.sort()
    hive_file = "/tmp/count_diff_hive"
    with open(hive_file, "w") as f:
        f.write("\n".join(hive_lines))
    diff = difflib.HtmlDiff().make_file(hive_lines, bq_lines, "Hive", "BigQuery", context=False, numlines=30)
    html_file = "/tmp/count_diff.html"
    with open(html_file, "w") as f:
        f.write(diff)
    logging.debug("Sorted results of the queries are in the files %s and %s. HTML differences are in %s",
                  hive_file, bq_file, html_file)
    webbrowser.open("file://" + html_file, new=2)
    return False  # no need to execute the script further since errors have already been spotted


def get_intermediate_checksum_sql(hive_table, bq_table, column):
    """Build and return the queries that generate all the checksums to make the final comparison

    The queries will have the following schema:

WITH blocks AS (
    SELECT MOD( hash2( column), 100000) as gb, sha1(concat( col0, col1, col2, col3, col4)) as block_1,
      sha1(concat( col5, col6, col7, col8, col9)) as block_2, ... as block_N FROM table
),
full_lines AS (
    SELECT gb, sha1(concat( block_1, |, block_2...) as row_sha, block_1, block_2 ... FROM blocks
)
SELECT gb, sha1(concat(list<row_sha>)) as sline, sha1(concat(list<block_1>)) as sblock_1,
    sha1(concat(list<block_2>)) as sblock_2 ... as sblock_N FROM GROUP BY gb

    Args:
        hive_table (string): Full name of the Hive table
        bq_table (string): Full name of the BigQuery table
        column (string): The column used to Group By on

    Returns:
        tuple: the Hive query , the BQ query
    """

    column_blocks = []
    block_size = 5  # 5 columns means that when we want to debug we have enough context. But it small enough to avoid
    # being charged too much by Google when querying on it
    for idx, col in enumerate(ddlColumns):
        block_id = idx / block_size
        if idx % block_size == 0:
            column_blocks.append([])
        column_blocks[block_id].append({"name": col["name"], "type": col["type"]})
    number_of_blocks = len(column_blocks)
    logging.debug("%i column_blocks (with a size of %i columns) have been considered: %s", number_of_blocks, block_size,
                  str(column_blocks))

    # Generate the concatenations for the column_blocks
    hive_basic_shas = ""
    bq_basic_shas = ""
    for idx, block in enumerate(column_blocks):  # TODO use enumerate everywhere
        hive_basic_shas += "base64( unhex( SHA1( concat( "
        bq_basic_shas += "TO_BASE64( sha1( concat( "
        for col in block:
            name = col["name"]
            hive_value_name = name
            bq_value_name = name
            if col["type"] == 'date':
                hive_value_name = "cast( %s as STRING)" % name
            elif "decimal" in col["type"]:  # aligning formatting of Decimal types in Hive with Float in BQ
                hive_value_name = 'regexp_replace( %s, "\\.0$", "")' % name
            if not col["type"] == 'string':
                bq_value_name = "cast( %s as STRING)" % name
            hive_basic_shas += "CASE WHEN %s IS NULL THEN 'n_%s' ELSE %s END, '|'," % (name, name[:2], hive_value_name)
            bq_basic_shas += "CASE WHEN %s IS NULL THEN 'n_%s' ELSE %s END, '|'," % (name, name[:2], bq_value_name)
        hive_basic_shas = hive_basic_shas[:-6] + ")))) as block_%i,\n" % idx
        bq_basic_shas = bq_basic_shas[:-6] + "))) as block_%i,\n" % idx
    hive_basic_shas = hive_basic_shas[:-2]
    bq_basic_shas = bq_basic_shas[:-2]

    hive_query = "WITH blocks AS (\nSELECT hash(%s) %% %i as gb,\n%s\nFROM %s\n),\n" \
                 % (column, number_of_group_by, hive_basic_shas, hive_table)  # 1st CTE with the basic block shas
    list_blocks = ", ".join(["block_%i" % i for i in range(number_of_blocks)])
    hive_query += "full_lines AS(\nSELECT gb, base64( unhex( SHA1( concat( %s)))) as row_sha, %s FROM blocks\n)\n" % \
                  (list_blocks, list_blocks)  # 2nd CTE to get all the info of a row
    hive_list_shas = ", ".join(["base64( unhex( SHA1( concat_ws( '|', collect_list( block_%i))))) as block_%i_gb " %
                                (i, i) for i in range(number_of_blocks)])
    hive_query += "SELECT gb, base64( unhex( SHA1( concat_ws( '|', collect_list( row_sha))))) as row_sha_gb, %s FROM " \
                  "full_lines GROUP BY gb" % hive_list_shas  # final query where all the shas are grouped by row-blocks
    logging.debug("##### Final Hive query is:\n%s\n", hive_query)

    bq_query = hash2_js_udf + "WITH blocks AS (\nSELECT MOD( hash2(%s), %i) as gb,\n%s\nFROM %s\n),\n" \
                              % (column, number_of_group_by, bq_basic_shas, bq_table)  # 1st CTE
    bq_query += "full_lines AS(\nSELECT gb, TO_BASE64( sha1( concat( %s))) as row_sha, %s FROM blocks\n)\n"\
                % (list_blocks, list_blocks)  # 2nd CTE to get all the info of a row
    bq_list_shas = ", ".join(["SHA1( STRING_AGG( block_%i, '|')) as block_%i_gb "
                              % (i, i) for i in range(number_of_blocks)])
    bq_query += "SELECT gb, SHA1( STRING_AGG( row_sha, '|')) as row_sha_gb, %s FROM full_lines GROUP BY gb" \
                % bq_list_shas  # final query where all the shas are grouped by row-blocks
    logging.debug("##### Final BigQuery query is:\n%s\n", bq_query)

get_table_ddl(fullHiveTable)
group_by_column = find_groupby_column(fullHiveTable)
'''
gb_differences, bq_biggest_dic = compare_groupby_count(fullHiveTable, fullBqTable, group_by_column)
do_we_continue = show_results_count(fullHiveTable, fullBqTable, group_by_column, gb_differences, bq_biggest_dic)
if not do_we_continue:
    sys.exit(1)
'''
get_intermediate_checksum_sql(fullHiveTable, fullBqTable, group_by_column)
