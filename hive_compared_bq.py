import pyhs2
import logging
import threading
import difflib
import sys
import time
import webbrowser
from abc import ABCMeta, abstractmethod
from collections import Counter
from google.cloud import bigquery

ABC = ABCMeta('ABC', (object,), {}) # compatible with Python 2 *and* 3


class _Table(ABC):
    """"Represent an abstract table that contains database connection and the related SQL executions"""
    # TODO more description

    __metaclass__ = ABCMeta

    def __init__(self, database, table):
        self.database = database
        self.table = table
        self.full_name = database + '.' + table
        self.connection = self._create_connection()
        self._ddl_columns = []  # array instead of dictionary because we want to maintain the order of the columns
        self._ddl_partitions = []  # take care, those rows also appear in the columns array
        self._group_by_column = None  # the column that is used to "bucket" the rows

    @abstractmethod
    def get_type(self):
        """Return the (string) type of the database (Hive, BigQuery)"""
        pass

    @abstractmethod
    def _create_connection(self):
        """Connect to the table and return the connection object that we will use to launch queries"""
        pass

    @abstractmethod
    def get_ddl_column(self):
        """ Return the columns of this table

        The list of the column is an attribute of the class. If it already exists, then it is directly returns.
        Otherwise, a connection is made to the database to get the schema of the table, and at the same time the
        attribute (list) partition is also filled.

        :rtype: list of dict
        :returns: list of {"name", "type"} dictionaries that represent the columns of this table
        """
        pass

    @abstractmethod
    def get_groupby_column(self):
        # TODO doc
        pass

    @abstractmethod
    def create_sql_groupby_count(self):
        # TODO doc
        pass

    @abstractmethod
    def launch_query_dict_result(self, query, result_dic):
        # TODO doc
        pass

    def get_sample_query(self):
        """ Build a SQL query that allows to get some sample lines with limited amount of columns"""
        # TODO doc again
        query = "SELECT"
        selected_columns = self.get_ddl_column()[:tc.sample_column_number]
        for col in selected_columns:
            query += " %s," % col["name"]  # for the last column we'll remove that trailing ","
        query = query[:-1] + " FROM %s LIMIT %i" % (self.full_name, tc.sample_column_number)
        return query, selected_columns


class THive(_Table):
    """Hive implementation of the _Table object"""

    def __init__(self, database, table, cluster):
        self.server = 'shd-hdp-' + cluster + '-master-003.bolcom.net'
        _Table.__init__(self, database, table)
        self.jarPath = 'hdfs://hdp-' + cluster + '/user/sluangsay/lib/sha1.jar'

    def get_type(self):
        return "hive"

    def _create_connection(self):  # TODO maybe this underlying object should be private?
        return pyhs2.connect(host=self.server, port=10000, authMechanism="KERBEROS", database=self.database)

    def get_ddl_column(self):
        if len(self._ddl_columns) > 0:
            return self._ddl_columns

        # TODO doc again
        is_col_def = True
        cur = self.connection.cursor()
        cur.execute("describe " + self.full_name)
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
                self._ddl_columns.append(my_dic)
            else:
                self._ddl_partitions.append(my_dic)
        cur.close()
        return self._ddl_columns

    def get_groupby_column(self):
        """Use a sample to return a column that seems enough spread to do interesting GROUP BY on it"""
        # TODO doc again
        if self._group_by_column is not None:
            return self._group_by_column

        query, selected_columns = self.get_sample_query()

        #  Get a sample from the table and fill Counters to each column
        logging.info("Analyzing the columns %s with a sample of %i values", str([x["name"] for x in selected_columns]),
                     tc.sample_column_number)
        for col in selected_columns:
            col["Counter"] = Counter()
        cur = self.connection.cursor()
        cur.execute(query)
        while cur.hasMoreRows:
            fetched = cur.fetchone()
            if fetched is not None:
                for idx, col in enumerate(selected_columns):
                    value_column = fetched[idx]
                    col["Counter"][value_column] += 1  # TODO what happens with NULL?
        cur.close()

        #  Look at the statistics to estimate which column is the best to do a GROUP BY
        max_frequent_number = tc.sample_rows_number * tc.max_percent_most_frequent_value_in_column / 100
        minimum_weight = tc.sample_rows_number  # TODO put infinite for clarity of code
        highest_first = max_frequent_number
        column_with_minimum_weight = None
        for col in selected_columns:
            highest = col["Counter"].most_common(1)[0]
            if highest[1] > max_frequent_number:
                logging.debug(
                    "Discarding column '%s' because '%s' was found in sample %i times (higher than limit of %i)",
                    col["name"], highest[0], highest[1], max_frequent_number)
                continue
            # The biggest value is not too high, so let's see how big are the 50 biggest values
            weight_of_most_frequent_values = sum([x[1] for x in col["Counter"]
                                                 .most_common(tc.number_of_most_frequent_values_to_weight)])
            logging.debug("%s %s", col["name"], weight_of_most_frequent_values)
            if weight_of_most_frequent_values < minimum_weight:
                column_with_minimum_weight = col["name"]
                minimum_weight = weight_of_most_frequent_values
                highest_first = highest[1]
        logging.info("Best column to do a GROUP BY is %s (%i / %i)", column_with_minimum_weight, highest_first,
                     minimum_weight)

        self._group_by_column = column_with_minimum_weight  # TODO directly use the correct object above
        return self._group_by_column

    def create_sql_groupby_count(self):
        # TODO doc again
        query = "SELECT hash( %s) %% %s AS gb, count(*) AS count FROM %s GROUP BY hash(%s) %% %i"\
                % (self.get_groupby_column(), tc.number_of_group_by, self.full_name, self.get_groupby_column(),
                   tc.number_of_group_by)
        logging.debug("Hive query is: %s", query)

        return query

    def query(self, query):
        """Execute the received query in Hive and return the cursor which is ready to be fetched and MUST be closed after

        :type query: str
        :param query: query to execute in Hive

        :rtype: :class:`pyhs2.cursor.Cursor`
        :returns: the cursor for this query
        """

        logging.debug("Launching Hive query")
        cur = self.connection.cursor()
        cur.execute(query)
        logging.debug("Fetching Hive results")
        return cur

    def launch_query_dict_result(self, query, result_dic):
        # TODO doc
        cur = self.query(query)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                result_dic[row[0]] = row[1]
        logging.debug("All %i Hive rows fetched", len(result_dic))
        cur.close()


class TBigQuery(_Table):
    """BigQuery implementation of the _Table object"""

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

    def get_type(self):
        return "bigQuery"

    def _create_connection(self):  # TODO maybe this underlying object should be private?
        return bigquery.Client()

    def get_ddl_column(self):
        # TODO doc
        if len(self._ddl_columns) > 0:
            return self._ddl_columns
        else:
            raise AttributeError("DDL for this BigQuery table has not been given yet")

    def get_groupby_column(self):
        pass # not implemented yet

    def create_sql_groupby_count(self):
        # TODO doc
        query = self.hash2_js_udf + "SELECT MOD( hash2(%s), %i) as gb, count(*) as count FROM %s GROUP BY gb ORDER BY " \
                                    "gb" % (self.get_groupby_column(), tc.number_of_group_by, self.full_name)
        logging.debug("BigQuery query is: %s", query)
        return query

    def query(self, query):
        """Execute the received query in BigQuery and return an iterate Result object

        :type query: str
        :param query: query to execute in BigQuery

        :rtype: list of rows
        :returns: the QueryResults for this query
        """
        logging.debug("Launching BigQuery query")
        q = self.connection.run_sync_query(query)
        q.timeout_ms = 60000  # 1 minute to execute the query in BQ should be more than enough
        q.use_legacy_sql = False  # TODO use maxResults https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query?
        q.run()
        logging.debug("Fetching BigQuery results")
        return q.fetch_data()


class TableComparator(object):
    """Represent the general configuration of the program (tables names, number of rows to scan...) """

    def __init__(self):
        table = 'hive_compared_bq_table3'
        self.tsrc = THive('sluangsay', table, 'b')
        self.tdst = TBigQuery('bidwh2', table)
        #  myDatabase = 'ldebruijn'
        #  myTable = 'PPP_retail_promotion_reference_group'
        #  myTable = 'PPP_retail_promotion'

        # 10 000 rows should be good enough to use as a sample
        #  (Google uses some sample of 1000 or 3000 rows)
        #  Estimation of memory for the Counters: 10000 * 10 * ( 10 * 4 + 4) * 4 = 16.8 MB
        #  (based on estimation : rows * columns * ( size string + int) * overhead Counter )
        self.sample_rows_number = 10000
        self.sample_column_number = 10
        self.max_percent_most_frequent_value_in_column = 1  # if in one sample a column has a value whose frequency is
        # highest than this percentage, then this column is discarded
        self.number_of_most_frequent_values_to_weight = 50

        self.number_of_group_by = 100000  # 7999 is the limit if you want to manually download the data from BQ. This
        # limit does not apply in this script because we fetch the data with the Python API instead.
        #  TODO ideally this limit would be dynamic (counting the total rows), in order to get an average of 7 lines per bucket
        self.block_size = 5  # 5 columns means that when we want to debug we have enough context. But it small enough to
        #  avoid being charged too much by Google when querying on it

    def compare_groupby_count(self):
        """Runs a light query on Hive and BigQuery to check if the counts match, using the ideal column estimated before

        :type hive_table: str
        :param hive_table: full name of the Hive table

        :type bq_table: str
        :param bq_table: full name of the BigQuery table

        :type column: str
        :param column: the column used to Group By on

        :rtype: tuple
        :returns: ``(summary_differences, big_small_bucket)``, where ``summary_differences`` is a list of tuples,
        one per difference containing (groupByValue, number of differences for this bucket, count of rows for this
        bucket for the "biggest table"); ``big_small_bucket`` is a tuple containing the table that has the biggest
        distribution (according to the Group By column) and then the other table
        """
        logging.info("Executing the 'Group By' Count queries in Hive and BigQuery to do first comparison")
        src_query = self.tsrc.create_sql_groupby_count()
        dst_query = self.tdst.create_sql_groupby_count()

        result = {"src_count_dict": {}, "dst_count_dict": {}}
        t_src = threading.Thread(name='srcGroupBy-' + self.tsrc.get_type(), target=self.tsrc.launch_query_dict_result,
                                 args=(src_query, result["src_count_dict"]))
        t_dst = threading.Thread(name='dstGroupBy-' + self.tdst.get_type(), target=self.tdst.launch_query_dict_result,
                                 args=(dst_query, result["dst_count_dict"]))
        t_src.start()
        t_dst.start()
        t_src.join()
        t_dst.join()

        # #### Let's compare the count between the 2 Group By queries
        # iterate on biggest dictionary so that we're sure to se a difference if there is one
        logging.debug("Searching differences in Group By")
        if len(result["src_count_dict"]) > len(result["dst_count_dict"]):
            big_dict = result["src_count_dict"]
            small_dict = result["dst_count_dict"]
            big_small_bucket = (t_src, t_dst)
        else:
            big_dict = result["dst_count_dict"]
            small_dict = result["src_count_dict"]
            big_small_bucket = (t_dst, t_src)
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
        return summary_differences, big_small_bucket


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s]\t[%(asctime)s]  (%(threadName)-10s) %(message)s',)

tc = TableComparator()


def query_ctas_bq(query):
    """Execute the received query in BigQuery and return the name of the cache results table

    This is the equivalent of a "Create Table As a Select" in Hive. The advantage is that BigQuery only keeps that
    table during 24 hours (we don't have to delete it just like in the case of Hive), and we're not charged for the
    space used.

    :type query: str
    :param query: query to execute in BigQuery

    :rtype: str
    :returns: the full name of the cache table (dataset.table) that stores those results

    :raises: IOError if the query has some execution errors
    """

    logging.debug("Launching BigQuery CTAS query")
    job_name = "job_hive_compared_bq_%f" % time.time()  # Job ID must be unique
    job = bigquery_client.run_async_query(job_name.replace('.', '_'), query)  # replace(): Job IDs must be alphanumeric
    job.use_legacy_sql = False
    job.begin()
    time.sleep(3)  # minimum latency we will get in BQ
    retry_count = 30  # 1 minute
    while retry_count > 0 and job.state != 'DONE':
        retry_count -= 1
        time.sleep(2)
        job.reload()
    logging.debug("BigQuery CTAS query finished")

    if job.errors is not None:
        raise IOError("There was a problem in executing the query in BigQuery: %s" % str(job.errors))

    cache_table = job.destination.dataset_name + '.' + job.destination.name
    print "The cache table of the final comparison query in BigQuery is: " + cache_table

    return cache_table


def show_results_count(hive_table, bq_table, gb_column, differences, bq_biggest):
    """If any differences found in the Count Group By step, then show them in a webpage

    :type hive_table: str
    :param hive_table: full name of the Hive table

    :type bq_table: str
    :param bq_table: full name of the BigQuery table

    :type gb_column: str
    :param gb_column: the column used to Group By on

    :type differences: list of tuple
    :param differences: list of all the differences where each difference is described as: (groupByValue, number of
    differences for this bucket, count of rows for this bucket for the "biggest table")

    :type bq_biggest: bool
    :param bq_biggest: True if BigQuery is the "biggest table": the table that contains more distinct Group By values

    :rtype: bool
    :returns: True if we haven't found differences yet and further analysis is needed
    """
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
        cur = query_hive(hive_query)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                line = "^ " + " | ".join([str(col) for col in row]) + " $"
                hive_lines.append(line)
        logging.debug("All %i Hive rows fetched", len(hive_lines))
        cur.close()

    def launch_bq():
        for row in query_bq(bq_query):
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


def get_column_blocks(ddl):
    """Returns the list of a column blocks for a specific DDL (see function get_intermediate_checksum_sql)

    :type ddl: list of dict
    :param ddl: the ddl of the tables, containing dictionaries with keys (name, type) to describe each column

    :rtype: list of list
    :returns: list of each block, each one containing the (5) columns dictionary ({name, type}) that describe it
    """
    column_blocks = []
    for idx, col in enumerate(ddl):
        block_id = idx / block_size
        if idx % block_size == 0:
            column_blocks.append([])
        column_blocks[block_id].append({"name": col["name"], "type": col["type"]})
    return column_blocks


def get_intermediate_checksum_sql(hive_table, bq_table, column):
    """Build and return the queries that generate all the checksums to make the final comparison

    The queries will have the following schema:

WITH blocks AS (
    SELECT MOD( hash2( column), 100000) as gb, sha1(concat( col0, col1, col2, col3, col4)) as block_0,
      sha1(concat( col5, col6, col7, col8, col9)) as block_1, ... as block_N FROM table
),
full_lines AS (
    SELECT gb, sha1(concat( block_0, |, block_1...) as row_sha, block_0, block_1 ... FROM blocks
)
SELECT gb, sha1(concat(list<row_sha>)) as sline, sha1(concat(list<block_0>)) as sblock_1,
    sha1(concat(list<block_1>)) as sblock_2 ... as sblock_N FROM GROUP BY gb

    :type hive_table: str
    :param hive_table: full name of the Hive table

    :type bq_table: str
    :param bq_table: full name of the BigQuery table

    :type column: str
    :param column: the column used to Group By on

    :rtype: tuple of str
    :returns: the Hive query , the BQ query
    """

    column_blocks = get_column_blocks(ddlColumns)
    number_of_blocks = len(column_blocks)
    logging.debug("%i column_blocks (with a size of %i columns) have been considered: %s", number_of_blocks, block_size,
                  str(column_blocks))

    # Generate the concatenations for the column_blocks
    hive_basic_shas = ""
    bq_basic_shas = ""
    for idx, block in enumerate(column_blocks):
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
    hive_list_shas = ", ".join(["base64( unhex( SHA1( concat_ws( '|', sort_array( collect_list( block_%i)))))) as "
                                "block_%i_gb " % (i, i) for i in range(number_of_blocks)])
    hive_query += "SELECT gb, base64( unhex( SHA1( concat_ws( '|', sort_array( collect_list( row_sha)))))) as " \
                  "row_sha_gb, %s FROM full_lines GROUP BY gb" % hive_list_shas  # final query where all the shas are
    # grouped by row-blocks
    logging.debug("##### Final Hive query is:\n%s\n", hive_query)

    bq_query = hash2_js_udf + "WITH blocks AS (\nSELECT MOD( hash2(%s), %i) as gb,\n%s\nFROM %s\n),\n" \
                              % (column, number_of_group_by, bq_basic_shas, bq_table)  # 1st CTE
    bq_query += "full_lines AS(\nSELECT gb, TO_BASE64( sha1( concat( %s))) as row_sha, %s FROM blocks\n)\n"\
                % (list_blocks, list_blocks)  # 2nd CTE to get all the info of a row
    bq_list_shas = ", ".join(["TO_BASE64( sha1( STRING_AGG( block_%i, '|' ORDER BY block_%i))) as block_%i_gb "
                              % (i, i, i) for i in range(number_of_blocks)])
    bq_query += "SELECT gb, TO_BASE64( sha1( STRING_AGG( row_sha, '|' ORDER BY row_sha))) as row_sha_gb, %s FROM " \
                "full_lines GROUP BY gb" % bq_list_shas  # final query where all the shas are grouped by row-blocks
    logging.debug("##### Final BigQuery query is:\n%s\n", bq_query)

    return hive_query, bq_query


def compare_shas(hive_table, bq_table, column):
    """Runs the final queries on Hive and BigQuery to check if the checksum match and return the list of differences

    :type hive_table: str
    :param hive_table: full name of the Hive table

    :type bq_table: str
    :param bq_table: full name of the BigQuery table

    :type column: str
    :param column: the column used to Group By on

    :rtype: tuple
    :returns: ``(list_differences, names_sha_tables)``, where ``list_differences`` is the list of Group By values which
    present different row checksums; ``names_sha_tables`` is a dictionary that contains the names of the "temporary"
    result tables
    """
    logging.info("Executing the 'shas' queries in Hive and BigQuery to do final comparison")
    hive_query, bq_query = get_intermediate_checksum_sql(hive_table, bq_table, column)

    def launch_hive():
        cur = query_hive("add jar " + conf.jarPath)  # must be in a separated execution
        cur.execute("create temporary function SHA1 as 'org.apache.hadoop.hive.ql.udf.UDFSha1'")

        if "error" in result:
            return

        tmp_table = "%s.temporary_hive_compared_bq_%s" % (conf.myDatabase, str(time.time()).replace('.', '_'))
        cur.execute("CREATE TABLE " + tmp_table + " AS\n" + hive_query)
        cur.close()
        result["names_sha_tables"]["hive"] = tmp_table  # we confirm this table has been created
        print "The temporary table for Hive is %s. REMEMBER to delete it when you've finished doing the analysis!" \
              % tmp_table

        if "error" in result:  # A problem happened in BQ so there is no need to pursue or have the temp table
            return

        projection_hive_row_sha = "SELECT gb, row_sha_gb FROM %s" % result["names_sha_tables"]["hive"]
        cur = query_hive(projection_hive_row_sha)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                result["hive_sha_dict"][row[0]] = row[1]
        logging.debug("All %i Hive rows fetched", len(result["hive_sha_dict"]))
        cur.close()

    def launch_bq():
        try:
            result["names_sha_tables"]["bq"] = query_ctas_bq(bq_query)  # tmp table is automatically deleted after 1 day
            projection_gb_row_sha = "SELECT gb, row_sha_gb FROM %s" % result["names_sha_tables"]["bq"]
            for row in query_bq(projection_gb_row_sha):
                result["bq_sha_dict"][row[0]] = row[1]
        except:
            result["error"] = sys.exc_info()[1]
            raise
        logging.debug("All %i BQ rows fetched", len(result["bq_sha_dict"].keys()))

    result = {"names_sha_tables": {}, "bq_sha_dict": {}, "hive_sha_dict": {}}
    t_bq = threading.Thread(name='bqShaBy', target=launch_bq)
    t_hive = threading.Thread(name='hiveShaBy', target=launch_hive)
    t_bq.start()
    t_hive.start()
    t_bq.join()
    t_hive.join()

    if "error" in result:
        if "hive" in result["names_sha_tables"]:
            query_hive("DROP TABLE " + str(result["names_sha_tables"]["hive"])).close()
        sys.exit(result["error"])

    # Comparing the results of those dictionaries
    logging.debug("Searching differences in Group By")
    bq_num_gb = len(result["bq_sha_dict"])
    hive_num_gb = len(result["hive_sha_dict"])
    if not bq_num_gb == hive_num_gb:
        sys.exit("The number of Group By values is not the same when doing the final sha queries (Hive: %i - "
                 "BigQuery: %i).\nMake sure to first execute the 'count' verification step!" % (bq_num_gb, hive_num_gb))

    list_differences = []
    for (k, v) in result["bq_sha_dict"].iteritems():
        if k not in result["hive_sha_dict"]:
            sys.exit("The Group By value %s appears in BigQuery but not in Hive.\nMake sure to first execute the "
                     "'count' verification step!" % k)
        elif v != result["hive_sha_dict"][k]:
            list_differences.append(k)
    if len(list_differences) != 0:
        logging.info("We found %i differences in sha verification", len(list_differences))
        logging.debug("Differences in Group By count are: %s", list_differences[:300])
    return list_differences, result["names_sha_tables"]


def get_sql_final_differences(hive_table, bq_table, gb_column, differences, temporary_tables):
    """If any differences found in the last sha step, then return the queries to get the real data for those differences

    :type hive_table: str
    :param hive_table: full name of the Hive table

    :type bq_table: str
    :param bq_table: full name of the BigQuery table

    :type gb_column: str
    :param gb_column: the column used to Group By on

    :type differences: list of str
    :param differences: the list of Group By values which present different row checksums

    :type temporary_tables: dict
    :param temporary_tables: contains the names of the temporary tables ["hive", "bq"]

    :rtype: tuple of str
    :returns: ``(hive_final_sql, bq_final_sql)``, the queries to be executed to do the final debugging, or None if no
    differences have been found in the shas analysis and the tables are considered equal
    """
    if len(differences) == 0:
        return None
    subset_differences = str(differences[:8])[1:-1]  # we will just show some few (8) differences
    logging.debug("The sha differences that we consider are: %s", str(subset_differences))

    hive_query = "SELECT * FROM %s WHERE gb IN (%s)" % (temporary_tables["hive"], subset_differences)
    logging.debug("Hive query to find differences in bucket_blocks is: %s", hive_query)
    bq_query = "SELECT * FROM %s WHERE gb IN (%s)" % (temporary_tables["bq"], subset_differences)
    logging.debug("BQ query to find differences in bucket_blocks is: %s", bq_query)

    def launch_hive():
        cur = query_hive(hive_query)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                hive_sha_lines[row[0]] = row[2:]
        logging.debug("All %i Hive rows fetched", len(hive_sha_lines))
        cur.close()

    def launch_bq():
        for row in query_bq(bq_query):
            bq_sha_lines[row[0]] = row[2:]
        logging.debug("All %i BQ rows fetched", len(bq_sha_lines))

    bq_sha_lines = {}  # key=gb, values=list of shas from the blocks (not the one of the whole line)
    hive_sha_lines = {}
    t_bq = threading.Thread(name='bqFetchShaDifferences', target=launch_bq)
    t_hive = threading.Thread(name='hiveFetchShaDifferences', target=launch_hive)
    t_bq.start()
    t_hive.start()
    t_bq.join()
    t_hive.join()
    logging.debug("The 8 sha lines for Hive are: %s. The 8 sha lines for BigQuery are: %s", hive_sha_lines,
                  bq_sha_lines)

    # We want to find the column blocks that present most of the differences, and the bucket_rows associated to it
    blocks_most_differences = Counter()
    column_blocks = get_column_blocks(ddlColumns)
    map_colblocks_bucketrows = [[] for x in range(len(column_blocks))]
    for bucket_row, bq_blocks in bq_sha_lines.iteritems():
        hive_blocks = hive_sha_lines[bucket_row]
        for idx, sha in enumerate(bq_blocks):
            if sha != hive_blocks[idx]:
                blocks_most_differences[idx] += 1
                map_colblocks_bucketrows[idx].append(bucket_row)
    logging.debug("Block columns with most differences are: %s. Which correspond to those bucket rows: %s",
                  blocks_most_differences, map_colblocks_bucketrows)

    block_most_different = blocks_most_differences.most_common(1)[0][0]  # TODO we should check if we have enough
    # buckets otherwise we might want to take a second block
    list_column_to_check = " ,".join([x["name"] for x in column_blocks[block_most_different]])
    list_hashs = " ,".join(map(str, map_colblocks_bucketrows[block_most_different]))

    hive_final_sql = "SELECT %s, %s FROM %s WHERE hash(%s) %% %i IN (%s)" \
                     % (gb_column, list_column_to_check, hive_table, gb_column, number_of_group_by, list_hashs)
    bq_final_sql = hash2_js_udf + "SELECT %s, %s FROM %s WHERE MOD( hash2(%s), %i) IN (%s)" \
                                  % (gb_column, list_column_to_check, bq_table, gb_column, number_of_group_by,
                                     list_hashs)
    logging.debug("Final Hive query is: %s   -   Final BQ query is: %s", hive_final_sql, bq_final_sql)

    return hive_final_sql, bq_final_sql


def show_results_final_differences(hive_sql, bq_sql):
    """If any differences found in the shas analysis step, then show them in a webpage

    :type hive_sql: str
    :param hive_sql: hive query to launch to see the rows that are different

    :type bq_sql: str
    :param bq_sql: BigQuery query to launch to see the rows that are different
    """
    def launch_hive():
        cur = query_hive(hive_sql)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                line = "^ " + " | ".join([str(col) for col in row]) + " $"
                hive_lines.append(line)
        logging.debug("All %i Hive rows fetched", len(hive_lines))
        cur.close()

    def launch_bq():
        for row in query_bq(bq_sql):
            line = "^ " + " | ".join([str(col) for col in row]) + " $"
            bq_lines.append(line)
        logging.debug("All %i BQ rows fetched", len(bq_lines))

    bq_lines = []
    hive_lines = []
    t_bq = threading.Thread(name='bqShowShaDifferences', target=launch_bq)
    t_hive = threading.Thread(name='hiveShaCountDifferences', target=launch_hive)
    t_bq.start()
    t_hive.start()
    t_bq.join()
    t_hive.join()

    # TODO: this function could be repeated with the other show function?
    bq_lines.sort()
    bq_file = "/tmp/sha_diff_bq"
    with open(bq_file, "w") as f:
        f.write("\n".join(bq_lines))
    hive_lines.sort()
    hive_file = "/tmp/sha_diff_hive"
    with open(hive_file, "w") as f:
        f.write("\n".join(hive_lines))
    diff = difflib.HtmlDiff().make_file(hive_lines, bq_lines, "Hive", "BigQuery", context=False, numlines=30)
    html_file = "/tmp/sha_diff.html"
    with open(html_file, "w") as f:
        f.write(diff)
    logging.debug("Sorted results of the queries are in the files %s and %s. HTML differences are in %s",
                  hive_file, bq_file, html_file)
    webbrowser.open("file://" + html_file, new=2)
    return False  # no need to execute the script further since errors have already been spotted

get_table_ddl(conf.fullHiveTable)
group_by_column = find_groupby_column(conf.fullHiveTable)
'''
gb_differences, bq_biggest_dic = compare_groupby_count(fullHiveTable, fullBqTable, group_by_column)
do_we_continue = show_results_count(fullHiveTable, fullBqTable, group_by_column, gb_differences, bq_biggest_dic)
if not do_we_continue:
    sys.exit(1)
'''
sha_differences, temp_tables = compare_shas(conf.fullHiveTable, conf.fullBqTable, group_by_column)
queries = get_sql_final_differences(conf.fullHiveTable, conf.fullBqTable, group_by_column, sha_differences, temp_tables)
if queries is None:
    print "Sha queries were done and no differences where found: the tables %s and %s are equal!" \
          % (conf.fullHiveTable, conf.fullBqTable)
    sys.exit(0)
else:
    show_results_final_differences(queries[0], queries[1])
