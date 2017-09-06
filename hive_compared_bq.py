import argparse
import ast
import pyhs2
import logging
import threading
import difflib
import re
import sys
import time
import webbrowser
from abc import ABCMeta, abstractmethod
from collections import Counter
from google.cloud import bigquery

ABC = ABCMeta('ABC', (object,), {})  # compatible with Python 2 *and* 3


class _Table(ABC):
    """Represent an abstract table that contains database connection and the related SQL executions

    :type database: str
    :param database: Name of the database

    :type table: str
    :param table: Name of the table

    :type tc: :class:`TableComparator`
    :param tc: reference to the parent object, in order to access the configuration

    :type column_range: str
    :param column_range: Python array range that represents the range of columns in the DDL that we want to compare

    :type where_condition: str
    :param where_condition: boolean SQL condition that will be added to the WHERE condition of all the queries for that
                            table, so that we can restrict the analysis to a specific scope. Also quite useful when
                            we want to work on specific partitions.

    :type table: str
    :param table: Name of the table

    :type table: str
    :param table: Name of the table

    """
    # TODO more description

    __metaclass__ = ABCMeta

    def __init__(self, database, table, parent, *args, **kwargs):
        """"Represent an abstract table that contains database connection and the related SQL executions"""
        self.database = database
        self.table = table
        self.tc = parent
        self.column_range = ":"
        self.chosen_columns = None  # list of the (str) columns we want to focus on.
        self.ignore_columns = None
        self.where_condition = None
        """str: Docstring *after* attribute, with type specified."""
        self.full_name = database + '.' + table
        #: list of str: Doc comment *before* attribute, with type specified
        self.connection = self._create_connection()
        self._ddl_columns = []  # array instead of dictionary because we want to maintain the order of the columns
        self._ddl_partitions = []  # take care, those rows also appear in the columns array
        self._group_by_column = None  # the column that is used to "bucket" the rows

    @staticmethod
    def create_table_from_string(argument, options, table_comparator):
        """Parse an argument (usually received on the command line) and return the corresponding Table object

        :type argument: str
        :param argument: description of the table to connect to. Must have the format <type>/<database>.<table>
                        type can be {hive,bq}

        :type options: str
        :param options: the dictionary of all the options for this table connection. Could be for instance:
                         "{'jar': 'hdfs://hdp/user/sluangsay/lib/sha1.jar', 'hs2': 'master-003.bol.net'}"

        :type table_comparator: :class:`TableComparator`
        :param table_comparator: the TableComparator parent object, to have a reference to the configuration properties

        :rtype: :class:`_Table`
        :returns: the _Table object that corresponds to the argument

        :raises: ValueError if the argument is void or does not match the format
        """
        match = re.match(r'(\w+)/(\w+)\.(\w+)', argument)
        if match is None:
            raise ValueError("Table description must follow the following format: '<type>/<database>.<table>'")

        typedb = match.group(1)
        database = match.group(2)
        table = match.group(3)

        hash_options = {}
        if options is not None:
            try:
                hash_options = ast.literal_eval(options)
            except:
                raise ValueError("The option must be in a Python dictionary format (just like: {'jar': "
                                 "'hdfs://hdp/user/sluangsay/lib/sha1.jar', 'hs2': 'master-003.bol.net'}\nThe value"
                                 "received was: %s", options)

        if typedb == "bq":
            return TBigQuery(database, table, table_comparator)
        elif typedb == "hive":
            allowed_options = ["jar", "hs2"]
            for key in hash_options:
                if key not in allowed_options:
                    raise ValueError("The following option for Hive is not supported: %s\nThe only supported options"
                                     "are: %s", key, allowed_options)
            if 'hs2' not in hash_options:
                raise ValueError("hs2 option (Hive Server2 hostname) must be defined for Hive tables")

            return THive(database, table, table_comparator, hash_options['hs2'], hash_options.get('jar'))
        else:
            raise ValueError("The database type %s is not implemented" % typedb)

    @abstractmethod
    def get_type(self):
        """Return the (string) type of the database (Hive, BigQuery)"""
        pass

    def get_id_string(self):
        """Return a string that fully identifies the table"""
        return self.get_type() + "_" + self.full_name

    def set_where_condition(self, where):
        """the WHERE condition we want to apply for the table. Could be useful in case of partitioned tables

        :type where: str
        :param where: the WHERE condition. Example: datedir="2017-05-01"
        """
        self.where_condition = where

    def set_column_range(self, column_range):
        self.column_range = column_range

    def set_chosen_columns(self, cols):
        self.chosen_columns = cols.split(",")

    def set_ignore_columns(self, cols):
        self.ignore_columns = cols.split(",")

    def set_group_by_column(self, col):
        self._group_by_column = col

    @abstractmethod
    def _create_connection(self):
        """Connect to the table and return the connection object that we will use to launch queries"""
        pass

    @abstractmethod
    def get_ddl_columns(self):
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
        """Return a column that seems to have a good distribution in order to do interesting GROUP BY queries with it

        This _group_by_column is an attribute of the class. If it already exists, then it is directly returns.
        Otherwise, a small query to get some sample rows on some few columns is performed, in order to evaluate
        which of those columns present the best distribution (we want a column that will have as many Group By values
        as possible, and that avoid a bit the skew, so that when we detect a specific difference on a bucket, we will
        be able to show a number of lines for this bucket not too big). The found column is then saved as the attribute
        and returned.

        :rtype: str
        :returns: the column that will be used in the Group By
        """
        pass

    @abstractmethod
    def create_sql_groupby_count(self):
        """ Return a SQL query where we count the number of rows for each Group of hash() on the groupby_column

        The column found in get_groupby_column() is used to do a Group By and count the number of rows for each group.
        But in order to reduce the number of Groups, we hash the value of this column, and we take the modulo
        "number_of_group_by" of this hash value.

        :rtype: str
        :returns: SQL query to do the Group By Count
        """
        pass

    @abstractmethod
    def create_sql_show_bucket_columns(self, extra_columns_str, buckets_values):
        """ Return a SQL query that shows the rows that match some specific buckets and some given columns

        :type extra_columns_str: str
        :param extra_columns_str: the list of extra columns (separated by ",") we want to show to help debugging

        :type buckets_values: str
        :param buckets_values: the list of values (separated by ",") of the buckets we want to fetch

        :rtype: str
        :returns: SQL query to do the Group By Buckets
        """
        pass

    @abstractmethod
    def create_sql_intermediate_checksums(self):
        """Build and return the query that generates all the checksums to make the final comparison

        The query will have the following schema:

    WITH blocks AS (
        SELECT MOD( hash2( column), 100000) as gb, sha1(concat( col0, col1, col2, col3, col4)) as block_0,
          sha1(concat( col5, col6, col7, col8, col9)) as block_1, ... as block_N FROM table
    ),
    full_lines AS (
        SELECT gb, sha1(concat( block_0, |, block_1...) as row_sha, block_0, block_1 ... FROM blocks
    )
    SELECT gb, sha1(concat(list<row_sha>)) as sline, sha1(concat(list<block_0>)) as sblock_1,
        sha1(concat(list<block_1>)) as sblock_2 ... as sblock_N FROM GROUP BY gb

        :rtype: str
        :returns: the SQL query with the Group By and the shas
        """
        pass

    @abstractmethod
    def delete_temporary_table(self, table_name):
        """Drop the temporary table if needed (if it is not automatically deleted by the system)

        :type table_name: str
        :param table_name: name of the table to delete
        """
        pass

    @abstractmethod
    def launch_query_dict_result(self, query, result_dic, all_columns_from_2=False):
        """Launch the SQL query and stores the results of the 1st and 2nd columns in the dictionary

        The 1st column of each row is stored as the key of the dictionary, the 2nd column is for the value. This method
        is meant to catch the result of a Group By query, so that we are sure that the keys fetched are unique.

        :type query: str
        :param query: query to execute

        :type result_dic: dict
        :param result_dic: dictionary to store the result

        :type all_columns_from_2: bool
        :param all_columns_from_2: True if we want to fetch all the columns of the row, starting from the 2nd one. False
                                    if we want the value of the dictionary to only have the 1st column (take care:
                                    columns start counting with 0).
                                    (default: False)
        """
        pass

    @abstractmethod
    def launch_query_csv_compare_result(self, query, rows):
        """Launch the SQL query and stores the rows in an array with some kind of CSV formatting

        The only reason for the "CSV formatting" (separation of columns with "|") is to help in comparing the rows with
        the difflib library. This is also the reason why all the lines start with "^" and end with "$"

        :type query: str
        :param query: query to execute

        :type rows: list of str
        :param rows: the (void) array that will store the rows
        """
        pass

    @abstractmethod
    def launch_query_with_intermediate_table(self, query, result):
        """Launch the query, stores the results in a temporary table and put the first 2 columns in a dictionary

        This method is used to computes a lot of checksums and thus is a bit heavy to compute. This is why we store
        all those detailed results in a temporary table. Then present a summary of the returned rows by storing the
        first 2 columns in a dictionary under the ``result`` dictionary.

        :type query: str
        :param query: query to execute

        :type result: dict
        :param result: dictionary to store the result
        """

    def get_sample_query(self):
        """ Build a SQL query to get some sample lines with limited amount of columns

        We limit the number of columns to a small number (ex: 10) because it is usually unnecessary to look at all
        the columns in order to find one with a good distribution (usually, the "index" will be in the first columns).
        What is more, in BigQuery we are billed by the number of columns we read so we need to avoid reading
        hundreds of columns just like what we have for big tables.

        :rtype: tuple
        :returns: ``(query, selected_columns)``, where ``query`` is the sample SQL query; ``selected_columns`` is the
                    list of columns that are fetched
        """
        query = "SELECT"
        selected_columns = self.get_ddl_columns()[:self.tc.sample_column_number]
        for col in selected_columns:
            query += " %s," % col["name"]  # for the last column we'll remove that trailing ","
        where_condition = ""
        if self.where_condition is not None:
            where_condition = "WHERE " + self.where_condition
        query = query[:-1] + " FROM %s %s LIMIT %i" % (self.full_name, where_condition, self.tc.sample_rows_number)
        return query, selected_columns

    def get_column_blocks(self, ddl):
        """Returns the list of a column blocks for a specific DDL (see function create_sql_intermediate_checksums)

        :type ddl: list of dict
        :param ddl: the ddl of the tables, containing dictionaries with keys (name, type) to describe each column

        :rtype: list of list
        :returns: list of each block, each one containing the (5) columns dictionary ({name, type}) that describe it
        """
        column_blocks = []
        for idx, col in enumerate(ddl):
            block_id = idx / self.tc.block_size
            if idx % self.tc.block_size == 0:
                column_blocks.append([])
            column_blocks[block_id].append({"name": col["name"], "type": col["type"]})
        return column_blocks


class THive(_Table):
    """Hive implementation of the _Table object"""

    def __init__(self, database, table, parent, hs2_server, jar_path):
        self.server = hs2_server
        _Table.__init__(self, database, table, parent)
        self.jarPath = jar_path

    def get_type(self):
        return "hive"

    def _create_connection(self):
        return pyhs2.connect(host=self.server, port=10000, authMechanism="KERBEROS", database=self.database)

    def get_ddl_columns(self):
        if len(self._ddl_columns) > 0:
            return self._ddl_columns

        is_col_def = True
        cur = self.connection.cursor()
        cur.execute("describe " + self.full_name)
        all_columns = []
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
                all_columns.append(my_dic)
            else:
                self._ddl_partitions.append(my_dic)
        cur.close()

        if self.column_range == ":":
            if self.chosen_columns is not None:  # user has declared the columns he wants to analyze
                leftover = list(self.chosen_columns)
                for col in all_columns:
                    if col['name'] in leftover:
                        leftover.remove(col['name'])
                        self._ddl_columns.append(col)
                if len(leftover) > 0:
                    sys.exit("Error: you asked to analyze the columns %s but we could not find them in the table %s"
                             % (str(leftover), self.get_id_string()))
            else:
                self._ddl_columns = all_columns
        else:  # user has requested a specific range of columns
            match = re.match(r'(\d*):(\d*)', self.column_range)
            if match is None:
                raise ValueError("The column range must follow the Python style '1:9'. You gave: %s", self.column_range)

            start = 0
            if len(match.group(1)) > 0:
                start = int(match.group(1))
            end = len(all_columns)
            if len(match.group(2)) > 0:
                end = int(match.group(2))
            self._ddl_columns = all_columns[start:end]
            logging.debug("The range of columns has been reduced to: %s", self._ddl_columns)

        if self.ignore_columns is not None:
            all_columns = []
            for col in self._ddl_columns:
                if not col['name'] in self.ignore_columns:
                    all_columns.append(col)
            self._ddl_columns = list(all_columns)

        return self._ddl_columns

    def get_groupby_column(self):
        if self._group_by_column is not None:
            return self._group_by_column

        query, selected_columns = self.get_sample_query()

        #  Get a sample from the table and fill Counters to each column
        logging.info("Analyzing the columns %s with a sample of %i values", str([x["name"] for x in selected_columns]),
                     self.tc.sample_rows_number)
        for col in selected_columns:
            col["Counter"] = Counter()
        cur = self.connection.cursor()
        cur.execute(query)
        while cur.hasMoreRows:
            fetched = cur.fetchone()
            if fetched is not None:
                for idx, col in enumerate(selected_columns):
                    value_column = fetched[idx]
                    col["Counter"][value_column] += 1  # TODO what happens with NULL? (case of globalid in Omniture)
        cur.close()

        #  Look at the statistics to estimate which column is the best to do a GROUP BY
        max_frequent_number = self.tc.sample_rows_number * self.tc.max_percent_most_frequent_value_in_column / 100
        minimum_weight = sys.maxint
        highest_first = max_frequent_number
        for col in selected_columns:
            highest = col["Counter"].most_common(1)[0]
            if highest[1] > max_frequent_number:
                logging.debug(
                    "Discarding column '%s' because '%s' was found in sample %i times (higher than limit of %i)",
                    col["name"], highest[0], highest[1], max_frequent_number)
                continue
            # The biggest value is not too high, so let's see how big are the 50 biggest values
            weight_of_most_frequent_values = sum([x[1] for x in col["Counter"]
                                                 .most_common(self.tc.number_of_most_frequent_values_to_weight)])
            logging.debug("%s: sum up of the %i most frequent apparitions: %i", col["name"],
                          self.tc.number_of_most_frequent_values_to_weight, weight_of_most_frequent_values)
            if weight_of_most_frequent_values < minimum_weight:
                self._group_by_column = col["name"]
                minimum_weight = weight_of_most_frequent_values
                highest_first = highest[1]
        if self._group_by_column is None:
            sys.exit("Error: we could not find a suitable column to do a Group By. Either relax the selection condition"
                     " with the '--max-gb-percent' option or directly select the column with '--group-by-column' ")
        logging.info("Best column to do a GROUP BY is %s (apparitions of most frequent value: %i / the %i most frequent"
                     "values sum up %i apparitions)", self._group_by_column, highest_first,
                     self.tc.number_of_most_frequent_values_to_weight, minimum_weight)

        return self._group_by_column

    def create_sql_groupby_count(self):
        where_condition = ""
        if self.where_condition is not None:
            where_condition = "WHERE " + self.where_condition
        query = "SELECT hash( cast( %s as STRING)) %% %i AS gb, count(*) AS count FROM %s %s GROUP BY " \
                "hash( cast( %s as STRING)) %% %i" \
                % (self.get_groupby_column(), self.tc.number_of_group_by, self.full_name, where_condition,
                   self.get_groupby_column(), self.tc.number_of_group_by)
        logging.debug("Hive query is: %s", query)

        return query

    def create_sql_show_bucket_columns(self, extra_columns_str, buckets_values):
        gb_column = self.get_groupby_column()
        where_condition = ""
        if self.where_condition is not None:
            where_condition = self.where_condition + " AND"
        hive_query = "SELECT hash( cast( %s as STRING)) %% %i as bucket, %s, %s FROM %s WHERE %s " \
                     "hash( cast( %s as STRING)) %% %i IN (%s)" \
                     % (gb_column, self.tc.number_of_group_by, gb_column, extra_columns_str, self.full_name,
                        where_condition, gb_column, self.tc.number_of_group_by, buckets_values)
        logging.debug("Hive query to show the buckets and the extra columns is: %s", hive_query)

        return hive_query

    def create_sql_intermediate_checksums(self):
        column_blocks = self.get_column_blocks(self.get_ddl_columns())
        number_of_blocks = len(column_blocks)
        logging.debug("%i column_blocks (with a size of %i columns) have been considered: %s", number_of_blocks,
                      self.tc.block_size, str(column_blocks))

        # Generate the concatenations for the column_blocks
        hive_basic_shas = ""
        for idx, block in enumerate(column_blocks):
            hive_basic_shas += "base64( unhex( SHA1( concat( "
            for col in block:
                name = col["name"]
                hive_value_name = name
                if col["type"] == 'date':
                    hive_value_name = "cast( %s as STRING)" % name
                elif col["type"] == 'string' and name == 'allexceptbooks':  # TODO unhardcode this name value
                    hive_value_name = "DecodeCP1252( %s)" % name
                hive_basic_shas += "CASE WHEN %s IS NULL THEN 'n_%s' ELSE %s END, '|'," % (name, name[:2],
                                                                                           hive_value_name)
            hive_basic_shas = hive_basic_shas[:-6] + ")))) as block_%i,\n" % idx
        hive_basic_shas = hive_basic_shas[:-2]

        where_condition = ""
        if self.where_condition is not None:
            where_condition = "WHERE " + self.where_condition

        hive_query = "WITH blocks AS (\nSELECT hash( cast( %s as STRING)) %% %i as gb,\n%s\nFROM %s %s\n),\n" \
                     % (self.get_groupby_column(), self.tc.number_of_group_by, hive_basic_shas, self.full_name,
                        where_condition)  # 1st CTE with the basic block shas
        list_blocks = ", ".join(["block_%i" % i for i in range(number_of_blocks)])
        hive_query += "full_lines AS(\nSELECT gb, base64( unhex( SHA1( concat( %s)))) as row_sha, %s FROM blocks\n)\n" \
                      % (list_blocks, list_blocks)  # 2nd CTE to get all the info of a row
        hive_list_shas = ", ".join(["base64( unhex( SHA1( concat_ws( '|', sort_array( collect_list( block_%i)))))) as "
                                    "block_%i_gb " % (i, i) for i in range(number_of_blocks)])
        hive_query += "SELECT gb, base64( unhex( SHA1( concat_ws( '|', sort_array( collect_list( row_sha)))))) as " \
                      "row_sha_gb, %s FROM full_lines GROUP BY gb" % hive_list_shas  # final query where all the shas
        # are grouped by row-blocks
        logging.debug("##### Final Hive query is:\n%s\n", hive_query)

        return hive_query

    def delete_temporary_table(self, table_name):
        self.query_hive("DROP TABLE " + table_name).close()

    def query(self, query):
        """Execute the received query in Hive and return the cursor which is ready to be fetched and MUST be closed after

        :type query: str
        :param query: query to execute in Hive

        :rtype: :class:`pyhs2.cursor.Cursor`
        :returns: the cursor for this query

        :raises: IOError if the query has some execution errors
        """
        logging.debug("Launching Hive query")
        #  split_maxsize = 256000000
        # split_maxsize = 64000000
        # split_maxsize = 8000000
        split_maxsize = 16000000
        try:
            cur = self.connection.cursor()
            cur.execute("set mapreduce.input.fileinputformat.split.maxsize = %i" % split_maxsize)
            cur.execute(query)
        except:
            raise IOError("There was a problem in executing the query in Hive: %s", sys.exc_info()[1])
        logging.debug("Fetching Hive results")
        return cur

    def launch_query_dict_result(self, query, result_dic, all_columns_from_2=False):
        try:
            cur = self.query(query)
            while cur.hasMoreRows:
                row = cur.fetchone()
                if row is not None:
                    if not all_columns_from_2:
                        result_dic[row[0]] = row[1]
                    else:
                        result_dic[row[0]] = row[2:]
        except:
            result_dic["error"] = sys.exc_info()[1]
            raise
        logging.debug("All %i Hive rows fetched", len(result_dic))
        cur.close()

    def launch_query_csv_compare_result(self, query, rows):
        cur = self.query(query)
        while cur.hasMoreRows:
            row = cur.fetchone()
            if row is not None:
                line = "^ " + " | ".join([str(col) for col in row]) + " $"
                rows.append(line)
        logging.debug("All %i Hive rows fetched", len(rows))
        cur.close()

    def launch_query_with_intermediate_table(self, query, result):
        try:
            cur = self.query("add jar " + self.jarPath)  # must be in a separated execution
            cur.execute("create temporary function SHA1 as 'org.apache.hadoop.hive.ql.udf.UDFSha1'")
            cur.execute("create temporary function DecodeCP1252 as "
                        "'org.apache.hadoop.hive.ql.udf.generic.GenericUDFRemoveControlChar'")  # TODO change class name
        except:
            result["error"] = sys.exc_info()[1]
            raise

        if "error" in result:
            return  # let's stop the thread if some error popped up elsewhere

        tmp_table = "%s.temporary_hive_compared_bq_%s" % (self.database, str(time.time()).replace('.', '_'))
        cur.execute("CREATE TABLE " + tmp_table + " AS\n" + query)
        cur.close()
        result["names_sha_tables"][self.get_id_string()] = tmp_table  # we confirm this table has been created
        result["cleaning"] = (tmp_table, self)

        print "The temporary table for Hive is %s. REMEMBER to delete it when you've finished doing the analysis!" \
              % tmp_table

        if "error" in result:  # A problem happened in BQ so there is no need to pursue or have the temp table
            return

        projection_hive_row_sha = "SELECT gb, row_sha_gb FROM %s" % tmp_table
        self.launch_query_dict_result(projection_hive_row_sha, result["sha_dictionaries"][self.get_id_string()])


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

    def _create_connection(self):
        return bigquery.Client()

    def get_ddl_columns(self):
        if len(self._ddl_columns) > 0:
            return self._ddl_columns
        else:
            raise AttributeError("DDL for this BigQuery table has not been given yet")  # need to be implemented one day

    def get_groupby_column(self):
        if self._group_by_column is not None:
            return self._group_by_column
        raise AttributeError("Not implemented yet for BigQuery since we have to receive the result from Hive")

    def create_sql_groupby_count(self):
        where_condition = ""
        if self.where_condition is not None:
            where_condition = "WHERE " + self.where_condition
        query = self.hash2_js_udf + "SELECT MOD( hash2( cast(%s as STRING)), %i) as gb, count(*) as count FROM %s %s " \
                                    "GROUP BY gb ORDER BY gb" \
                                    % (self.get_groupby_column(), self.tc.number_of_group_by, self.full_name,
                                       where_condition)
        logging.debug("BigQuery query is: %s", query)
        return query

    def create_sql_show_bucket_columns(self, extra_columns_str, buckets_values):
        where_condition = ""
        if self.where_condition is not None:
            where_condition = self.where_condition + " AND"
        gb_column = self.get_groupby_column()
        bq_query = self.hash2_js_udf + "SELECT MOD( hash2( cast(%s as STRING)), %i) as bucket, %s as gb, %s FROM %s " \
                                       "WHERE %s MOD( hash2( cast(%s as STRING)), %i) IN (%s)" \
                                       % (gb_column, self.tc.number_of_group_by, gb_column, extra_columns_str,
                                          self.full_name, where_condition, gb_column, self.tc.number_of_group_by,
                                          buckets_values)
        logging.debug("BQ query to show the buckets and the extra columns is: %s", bq_query)

        return bq_query

    def create_sql_intermediate_checksums(self):
        column_blocks = self.get_column_blocks(self.get_ddl_columns())
        number_of_blocks = len(column_blocks)
        logging.debug("%i column_blocks (with a size of %i columns) have been considered: %s", number_of_blocks,
                      self.tc.block_size, str(column_blocks))

        # Generate the concatenations for the column_blocks
        bq_basic_shas = ""
        for idx, block in enumerate(column_blocks):
            bq_basic_shas += "TO_BASE64( sha1( concat( "
            for col in block:
                name = col["name"]
                bq_value_name = name
                if col["type"] == 'decimal':  # removing trailing & unnecessary 'zero decimal' (*.0)
                    bq_value_name = 'regexp_replace( %s, "\\.0$", "")' % name
                elif not col["type"] == 'string':
                    bq_value_name = "cast( %s as STRING)" % name
                bq_basic_shas += "CASE WHEN %s IS NULL THEN 'n_%s' ELSE %s END, '|'," % (name, name[:2], bq_value_name)
            bq_basic_shas = bq_basic_shas[:-6] + "))) as block_%i,\n" % idx
        bq_basic_shas = bq_basic_shas[:-2]

        where_condition = ""
        if self.where_condition is not None:
            where_condition = "WHERE " + self.where_condition

        bq_query = self.hash2_js_udf + "WITH blocks AS (\nSELECT MOD( hash2( cast(%s as STRING)), %i) as gb,\n%s\n" \
                                       "FROM %s %s\n),\n" \
                                       % (self.get_groupby_column(), self.tc.number_of_group_by, bq_basic_shas,
                                          self.full_name, where_condition)  # 1st CTE with the basic block shas
        list_blocks = ", ".join(["block_%i" % i for i in range(number_of_blocks)])
        bq_query += "full_lines AS(\nSELECT gb, TO_BASE64( sha1( concat( %s))) as row_sha, %s FROM blocks\n)\n" \
                    % (list_blocks, list_blocks)  # 2nd CTE to get all the info of a row
        bq_list_shas = ", ".join(["TO_BASE64( sha1( STRING_AGG( block_%i, '|' ORDER BY block_%i))) as block_%i_gb "
                                  % (i, i, i) for i in range(number_of_blocks)])
        bq_query += "SELECT gb, TO_BASE64( sha1( STRING_AGG( row_sha, '|' ORDER BY row_sha))) as row_sha_gb, %s FROM " \
                    "full_lines GROUP BY gb" % bq_list_shas  # final query where all the shas are grouped by row-blocks
        logging.debug("##### Final BigQuery query is:\n%s\n", bq_query)

        return bq_query

    def delete_temporary_table(self, table_name):
        pass  # The temporary (cached) tables in BigQuery are deleted after 24 hours

    def query(self, query):
        """Execute the received query in BigQuery and return an iterate Result object

        :type query: str
        :param query: query to execute in BigQuery

        :rtype: list of rows
        :returns: the QueryResults for this query
        """
        logging.debug("Launching BigQuery query")
        q = self.connection.run_sync_query(query)
        q.timeout_ms = 600000  # 10 minute to execute the query in BQ should be more than enough. 1 minute was too short
        # TODO use maxResults https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query? :
        q.use_legacy_sql = False
        q.run()
        logging.debug("Fetching BigQuery results")
        return q.fetch_data()

    def query_ctas_bq(self, query):
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
        job = self.connection.run_async_query(job_name.replace('.', '_'),
                                              query)  # replace(): Job IDs must be alphanumeric
        job.use_legacy_sql = False
        job.begin()
        time.sleep(3)  # 3 second is the minimum latency we get in BQ in general. So no need to try fetching before
        retry_count = 300  # 10 minute (should be enough)
        while retry_count > 0 and job.state != 'DONE':
            retry_count -= 1
            time.sleep(2)
            job.reload()
        logging.debug("BigQuery CTAS query finished")

        if job.errors is not None:
            raise IOError("There was a problem in executing the query in BigQuery: %s" % str(job.errors))

        cache_table = job.destination.dataset_name + '.' + job.destination.name
        print "The cache table of the final comparison query in BigQuery is: " + cache_table  # tmp table is
        # automatically deleted after 1 day. No need to tell the user that they have to delete it

        return cache_table

    def launch_query_dict_result(self, query, result_dic, all_columns_from_2=False):
        for row in self.query(query):
            if not all_columns_from_2:
                result_dic[row[0]] = row[1]
            else:
                result_dic[row[0]] = row[2:]
        logging.debug("All %i BigQuery rows fetched", len(result_dic))

    def launch_query_csv_compare_result(self, query, rows):
        for row in self.query(query):
            line = "^ " + " | ".join([str(col) for col in row]) + " $"
            rows.append(line)
        logging.debug("All %i BigQuery rows fetched", len(rows))

    def launch_query_with_intermediate_table(self, query, result):
        try:
            result["names_sha_tables"][self.get_id_string()] = self.query_ctas_bq(query)
            projection_gb_row_sha = "SELECT gb, row_sha_gb FROM %s" % result["names_sha_tables"][self.get_id_string()]
            self.launch_query_dict_result(projection_gb_row_sha, result["sha_dictionaries"][self.get_id_string()])
        except:
            result["error"] = sys.exc_info()[1]
            raise


class TableComparator(object):
    """Represent the general configuration of the program (tables names, number of rows to scan...) """

    def __init__(self):
        self.tsrc = None
        self.tdst = None

        # 10 000 rows should be good enough to use as a sample
        #  (Google uses some sample of 1000 or 3000 rows)
        #  Estimation of memory for the Counters: 10000 * 10 * ( 10 * 4 + 4) * 4 = 16.8 MB
        #  (based on estimation : rows * columns * ( size string + int) * overhead Counter )
        self.sample_rows_number = 10000
        self.sample_column_number = 10
        self.max_percent_most_frequent_value_in_column = None
        self.number_of_most_frequent_values_to_weight = 50

        self.number_of_group_by = 100000  # 7999 is the limit if you want to manually download the data from BQ. This
        # limit does not apply in this script because we fetch the data with the Python API instead.
        #  TODO ideally this limit would be dynamic (counting the total rows), in order to get an average of 7
        # lines per bucket
        self.skew_threshold = 40000  # if we detect that a Group By has more than this amount of rows
        # (compare_groupby_count() method), then we raise an exception, because the computation of the shas (which is
        # computationally expensive) might suffer a lot from this skew, and might also trigger some OOM exception.
        # 40 000 seems a safe number:
        # Let's consider a table with many column: 1000 columns. That means 201 sha blocks (1000 / 5 + 1)
        # a sha is 29 characters
        # so the max memory with a skew of 40 000 would be:
        # 201 * 40000 * 29 / 1024 /1024 = 222 MB, which should fit into the Heap of a task process
        self.block_size = 5  # 5 columns means that when we want to debug we have enough context. But it small enough to
        #  avoid being charged too much by Google when querying on it

        reload(sys)
        sys.setdefaultencoding('utf-8')

    def set_tsrc(self, table):
        """Set the source table to be compared

        :type table: :class:`_Table`
        :param table: the _Table object
        """
        self.tsrc = table

    def set_tdst(self, table):
        """Set the destination table to be compared

        :type table: :class:`_Table`
        :param table: the _Table object
        """
        self.tdst = table

    def set_skew_threshold(self, threshold):
        """Set the threshold value for the skew

        :type threshold: int
        :param threshold: the threshold value (default: 40 000)
        """
        self.skew_threshold = threshold

    def set_max_percent_most_frequent_value_in_column(self, percent):
        """Set the max_percent_most_frequent_value_in_column value

        If in one sample a column has a value whose frequency is highest than this percentage, then this column is not
        considered as a suitable column to do the Group By

        :type percent: float
        :param percent: the percentage value
        """
        self.max_percent_most_frequent_value_in_column = percent

    def compare_groupby_count(self):
        """Runs a light query on Hive and BigQuery to check if the counts match, using the ideal column estimated before

        Some skew detection is also performed here. And the program stops if we detect some important skew and no
        difference was detected (meaning that the sha computation would have been performed with that skew).

        :rtype: tuple
        :returns: ``(summary_differences, big_small_bucket)``, where ``summary_differences`` is a list of tuples,
                    one per difference containing (groupByValue, number of differences for this bucket, count of rows
                    for this bucket for the "biggest table"); ``big_small_bucket`` is a tuple containing the table that
                    has the biggest distribution (according to the Group By column) and then the other table
        """
        logging.info("Executing the 'Group By' Count queries for %s (%s) and %s (%s) to do first comparison",
                     self.tsrc.full_name, self.tsrc.get_type(), self.tdst.full_name, self.tdst.get_type())
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

        for k in result:
            if 'error' in result[k]:
                sys.exit(result[k]["error"])

        # #### Let's compare the count between the 2 Group By queries
        # iterate on biggest dictionary so that we're sure to se a difference if there is one
        logging.debug("Searching differences in Group By")
        if len(result["src_count_dict"]) > len(result["dst_count_dict"]):
            big_dict = result["src_count_dict"]
            small_dict = result["dst_count_dict"]
            big_small_bucket = (self.tsrc, self.tdst)
        else:
            big_dict = result["dst_count_dict"]
            small_dict = result["src_count_dict"]
            big_small_bucket = (self.tdst, self.tsrc)

        differences = Counter()
        skew = Counter()
        for (k, v) in big_dict.iteritems():
            if k not in small_dict:
                differences[k] = -v  # we want to see the differences where we have less lines to compare
            elif v != small_dict[k]:
                differences[k] = - abs(v - small_dict[k])
            # we check the skew even if some differences were found above and we will never enter the sha computation,
            # so that the developer can fix at early stage
            max_value = max(v, small_dict.get(k))
            if max_value > self.skew_threshold:
                skew[k] = max_value
        summary_differences = [(k, -v, big_dict[k]) for (k, v) in differences.most_common()]
        if len(skew) > 0:
            logging.warning("Some important skew (threshold: %i) was detected in the Group By column %s. The top values"
                            " are: %s", self.skew_threshold, self.tsrc.get_groupby_column(), str(skew.most_common(10)))
            if len(summary_differences) == 0:
                sys.exit("No difference in Group By count was detected but we saw some important skew that could make"
                         "the next step (comparison of the shas) very slow or failing. So better stopping now. You"
                         "should consider choosing another Group By column with the '--group-by-column' option")
        if len(summary_differences) != 0:
            logging.info("We found at least %i differences in Group By count", len(summary_differences))
            logging.debug("Differences in Group By count are: %s", summary_differences[:300])

        return summary_differences, big_small_bucket

    def show_results_count(self, summary_differences, big_small_bucket):
        """If any differences found in the Count Group By step, then show them in a webpage

        :type summary_differences: list of tuple
        :param summary_differences: list of all the differences where each difference is described as: (groupByValue,
        number of differences for this bucket, count of rows for this bucket for the "biggest table")

        :type big_small_bucket: tuple
        :param big_small_bucket: tuple containing the table that has the biggest distribution (according to the Group By
         column) and then the other table

        :rtype: bool
        :returns: True if we haven't found differences yet and further analysis is needed
        """
        if len(summary_differences) == 0:
            print "No differences where found when doing a Count on the tables %s and %s and grouping by on the " \
                  "column %s" % (self.tsrc.full_name, self.tdst.full_name, self.tsrc.get_groupby_column())
            return True  # means that we should continue executing the script

        # We want to return at most 6 blocks of lines corresponding to different group by values. For the sake of
        # brevity, each block should not show more than 70 lines. Blocks that show rows only on 1 table should be
        # limited to 3 (so that we can see "context" when debugging). To also give context, we will show some few other
        # columns.
        number_buckets_only_one_table = 0
        number_buckets_found = 0
        buckets_bigtable = []
        buckets_smalltable = []
        bigtable = big_small_bucket[0]
        smalltable = big_small_bucket[1]
        for (bucket, difference_num, biggest_num) in summary_differences:
            if difference_num > 70:
                break  # since the Counter was ordered from small differences to biggest, we know that this difference
                # number can only increase. So let's go out of the loop
            if biggest_num > 70:
                continue
            if difference_num == biggest_num:
                if number_buckets_only_one_table == 3:
                    continue
                else:
                    number_buckets_only_one_table += 1
                    number_buckets_found += 1
                    if number_buckets_found == 6:
                        break
                    buckets_bigtable.append(bucket)
            else:
                buckets_bigtable.append(bucket)
                buckets_smalltable.append(bucket)
                number_buckets_found += 1
                if number_buckets_found == 6:
                    break
        if len(buckets_bigtable) == 0:  # let's ensure that we have at least 1 value to show
            (bucket, difference_num, biggest_num) = summary_differences[0]
            buckets_bigtable.append(bucket)
            if difference_num != biggest_num:
                buckets_smalltable.append(bucket)
        logging.debug("Buckets for %s: %s \t\tBuckets for %s: %s", bigtable.full_name, str(buckets_bigtable),
                      smalltable.full_name, str(buckets_smalltable))

        gb_column = self.tsrc.get_groupby_column()
        extra_columns = [x["name"] for x in self.tsrc.get_ddl_columns()[:6]]  # add 5 extra columns to see some context
        if gb_column in extra_columns:
            extra_columns.remove(gb_column)
        elif len(extra_columns) == 6:
            extra_columns = extra_columns[:-1]  # limit to 5 columns
        extra_columns_str = str(extra_columns)[1:-1].replace("'", "")
        bigtable_query = bigtable.create_sql_show_bucket_columns(extra_columns_str, str(buckets_bigtable)[1:-1])
        smalltable_query = smalltable.create_sql_show_bucket_columns(extra_columns_str, str(buckets_smalltable)[1:-1])

        result = {"big_rows": [], "small_rows": []}
        t_big = threading.Thread(name='bigShowCountDifferences-' + bigtable.get_type(),
                                 target=bigtable.launch_query_csv_compare_result,
                                 args=(bigtable_query, result["big_rows"]))
        t_small = threading.Thread(name='smallShowCountDifferences-' + smalltable.get_type(),
                                   target=smalltable.launch_query_csv_compare_result,
                                   args=(smalltable_query, result["small_rows"]))
        t_big.start()
        t_small.start()
        t_big.join()
        t_small.join()

        sorted_file = {}
        for instance in ("big_rows", "small_rows"):
            result[instance].sort()
            sorted_file[instance] = "/tmp/count_diff_" + instance
            with open(sorted_file[instance], "w") as f:
                f.write("\n".join(result[instance]))

        column_description = "</br>hash(%s) , %s , %s" \
                             % (bigtable.get_groupby_column(), bigtable.get_groupby_column(), extra_columns_str)
        diff_string = difflib.HtmlDiff().make_file(result["big_rows"], result["small_rows"], bigtable.get_id_string() +
                                                   column_description, smalltable.get_id_string() + column_description,
                                                   context=False, numlines=30)
        html_file = "/tmp/count_diff.html"
        with open(html_file, "w") as f:
            f.write(diff_string)
        logging.debug("Sorted results of the queries are in the files %s and %s. HTML differences are in %s",
                      sorted_file["big_rows"], sorted_file["small_rows"], html_file)
        webbrowser.open("file://" + html_file, new=2)
        return False  # no need to execute the script further since errors have already been spotted

    def compare_shas(self):
        """Runs the final queries on Hive and BigQuery to check if the checksum match and return the list of differences

        :rtype: tuple
        :returns: ``(list_differences, names_sha_tables)``, where ``list_differences`` is the list of Group By values
                    which presents different row checksums; ``names_sha_tables`` is a dictionary that contains the names
                    of the "temporary" result tables
        """
        logging.info("Executing the 'shas' queries for %s and %s to do final comparison",
                     self.tsrc.get_id_string(), self.tdst.get_id_string())

        tsrc_query = self.tsrc.create_sql_intermediate_checksums()
        tdst_query = self.tdst.create_sql_intermediate_checksums()

        # "cleaning" is for all the tables that will need to be eventually deleted. It must contain tuples (<name of
        # table to delete>, corresponding _Table object). "names_sha_tables" contains all the temporary tables generated
        # even the BigQuery cached table that does not need to be deleted. "sha_dictionaries" contains the results.
        result = {"cleaning": [], "names_sha_tables": {}, "sha_dictionaries": {
            self.tsrc.get_id_string(): {},
            self.tdst.get_id_string(): {}
        }}
        t_src = threading.Thread(name='shaBy-' + self.tsrc.get_id_string(),
                                 target=self.tsrc.launch_query_with_intermediate_table,
                                 args=(tsrc_query, result))
        t_dst = threading.Thread(name='shaBy-' + self.tdst.get_id_string(),
                                 target=self.tdst.launch_query_with_intermediate_table,
                                 args=(tdst_query, result))
        t_src.start()
        t_dst.start()
        t_src.join()
        t_dst.join()

        if "error" in result:
            for table_name, table_object in result["cleaning"]:
                table_object.delete_temporary_table(table_name)
            sys.exit(result["error"])

        # Comparing the results of those dictionaries
        logging.debug("Searching differences in Shas")
        src_num_gb = len(result["sha_dictionaries"][self.tsrc.get_id_string()])
        dst_num_gb = len(result["sha_dictionaries"][self.tdst.get_id_string()])
        if not src_num_gb == dst_num_gb:
            sys.exit("The number of Group By values is not the same when doing the final sha queries (%s: %i - "
                     "%s: %i).\nMake sure to first execute the 'count' verification step!"
                     % (self.tsrc.get_id_string(), src_num_gb, self.tdst.get_id_string(), dst_num_gb))

        list_differences = []
        for (k, v) in result["sha_dictionaries"][self.tdst.get_id_string()].iteritems():
            if k not in result["sha_dictionaries"][self.tsrc.get_id_string()]:
                sys.exit("The Group By value %s appears in %s but not in %s.\nMake sure to first execute the "
                         "'count' verification step!" % (k, self.tdst.get_id_string(), self.tsrc.get_id_string()))
            elif v != result["sha_dictionaries"][self.tsrc.get_id_string()][k]:
                list_differences.append(k)
        if len(list_differences) != 0:
            logging.info("We found %i differences in sha verification", len(list_differences))
            logging.debug("Differences in sha are: %s", list_differences[:300])
        return list_differences, result["names_sha_tables"]

    def get_sql_final_differences(self, differences, temp_tables):
        """Return the queries to get the real data for the differences found in the last compare_shas() step

        From the compare_shas step, we know all the rowBuckets that present some differences. The goal of this
        function is to identify which columnsBuckets have those differences, so that we can show to the developer
        only the rows and the columns in error. We will focus on the columns that present most of the errors.

        :type differences: list of str
        :param differences: the list of Group By values which present different row checksums

        :type temp_tables: dict
        :param temp_tables: contains the names of the temporary tables ["hive", "bq"]

        :rtype: tuple of str
        :returns: ``(hive_final_sql, bq_final_sql, list_column_to_check)``, the queries to be executed to do the final
                    debugging, and the name of the columns that are fetched.

        :raises: IOError if the query has some execution errors
        """
        subset_differences = str(differences[:3000])[1:-1]  # let's choose quite a big number (instead of just looking
        # at some few (5 for instance) differences for 2 reasons: 1) by fetching more rows we will find estimate
        # better which column blocks fail often 2) we have less possibilities to face some 'permutations' problems
        logging.debug("The sha differences that we consider are: %s", str(subset_differences))

        src_query = "SELECT * FROM %s WHERE gb IN (%s)" % (temp_tables[self.tsrc.get_id_string()], subset_differences)
        dst_query = "SELECT * FROM %s WHERE gb IN (%s)" % (temp_tables[self.tdst.get_id_string()], subset_differences)
        logging.debug("queries to find differences in bucket_blocks are: \n%s\n%s", src_query, dst_query)

        src_sha_lines = {}  # key=gb, values=list of shas from the blocks (not the one of the whole line)
        dst_sha_lines = {}
        t_src = threading.Thread(name='srcFetchShaDifferences', target=self.tsrc.launch_query_dict_result,
                                 args=(src_query, src_sha_lines, True))
        t_dst = threading.Thread(name='dstFetchShaDifferences', target=self.tdst.launch_query_dict_result,
                                 args=(dst_query, dst_sha_lines, True))
        t_src.start()
        t_dst.start()
        t_src.join()
        t_dst.join()

        # We want to find the column blocks that present most of the differences, and the bucket_rows associated to it
        blocks_most_differences = Counter()
        column_blocks = self.tsrc.get_column_blocks(self.tsrc.get_ddl_columns())
        map_colblocks_bucketrows = [[] for x in range(len(column_blocks))]
        for bucket_row, dst_blocks in dst_sha_lines.iteritems():
            src_blocks = src_sha_lines[bucket_row]
            for idx, sha in enumerate(dst_blocks):
                if sha != src_blocks[idx]:
                    blocks_most_differences[idx] += 1
                    map_colblocks_bucketrows[idx].append(bucket_row)
        logging.debug("Block columns with most differences are: %s. Which correspond to those bucket rows: %s",
                      blocks_most_differences, map_colblocks_bucketrows)

        # collisions could happen for instance with those 2 "rows" (1st column is the Group BY value, 2nd column is the
        # value of a column in the data, 3rd column is the value of another column which belongs to another 'block
        # column' than the 2nd one):
        # ## SRC table:
        # bucket1   1   A
        # bucket1   0   B
        # ## DST table:
        # bucket1   0   A
        # bucket1   1   B
        # In such case, the 'grouped sha of each column' will be always the same. But the sha-lines will be different
        if len(blocks_most_differences) == 0:
            raise RuntimeError("Program faced some collisions when trying to assess which blocks of columns were not"
                               "correct. Please contact the developer to ask for a fix")
        block_most_different = blocks_most_differences.most_common(1)[0][0]
        # buckets otherwise we might want to take a second block
        list_column_to_check = " ,".join([x["name"] for x in column_blocks[block_most_different]])
        # let's display just 10 buckets in error max
        list_hashs = " ,".join(map(str, map_colblocks_bucketrows[block_most_different][:10]))

        src_final_sql = self.tsrc.create_sql_show_bucket_columns(list_column_to_check, list_hashs)
        dst_final_sql = self.tdst.create_sql_show_bucket_columns(list_column_to_check, list_hashs)
        logging.debug("Final source query is: %s   -   Final dest query is: %s", src_final_sql, dst_final_sql)

        return src_final_sql, dst_final_sql, list_column_to_check

    @staticmethod
    def display_html_diff(result, file_name, col_description):
        """Show the difference of the analysis in a graphical webpage

         :type result: dict
         :param result: dictionary that contains the hashMaps of some of the rows with differences between the 2 tables

         :type file_name: str
         :param file_name: prefix of the path where will be written the temporary files
         """
        # TODO doc
        sorted_file = {}
        keys = result.keys()
        for instance in keys:
            result[instance].sort()
            sorted_file[instance] = file_name + "_" + instance
            with open(sorted_file[instance], "w") as f:
                f.write("\n".join(result[instance]))

        diff_html = difflib.HtmlDiff().make_file(result[keys[0]], result[keys[1]], keys[0] + col_description, keys[1]
                                                 + col_description, context=True, numlines=15)
        html_file = file_name + ".html"
        with open(html_file, "w") as f:
            f.write(diff_html)
        logging.debug("Sorted results of the queries are in the files %s and %s. HTML differences are in %s",
                      sorted_file[keys[0]], sorted_file[keys[1]], html_file)
        webbrowser.open("file://" + html_file, new=2)

    def show_results_final_differences(self, src_sql, dst_sql, list_extra_columns):
        """If any differences found in the shas analysis step, then show them in a webpage

        :type src_sql: str
        :param src_sql: the query of the source table to launch to see the rows that are different

        :type dst_sql: str
        :param dst_sql: the query of the destination table to launch to see the rows that are different

        :type list_extra_columns: str
        :param list_extra_columns: list (',' separated) of the extra columns that will be shown in the differences.
                This parameter is only used to show the description in the web page.
        """
        src_id = self.tsrc.get_id_string()
        dst_id = self.tdst.get_id_string()
        result = {src_id: [], dst_id: []}
        t_src = threading.Thread(name='srcShowShaFinalDifferences', target=self.tsrc.launch_query_csv_compare_result,
                                 args=(src_sql, result[src_id]))
        t_dst = threading.Thread(name='dstShowShaFinalDifferences', target=self.tdst.launch_query_csv_compare_result,
                                 args=(dst_sql, result[dst_id]))
        t_src.start()
        t_dst.start()
        t_src.join()
        t_dst.join()

        col_description = "</br>hash(%s) , %s , %s" \
                          % (self.tsrc.get_groupby_column(), self.tsrc.get_groupby_column(), list_extra_columns)

        self.display_html_diff(result, "/tmp/sha_diff", col_description)

        return False  # no need to execute the script further since errors have already been spotted

    def synchronise_tables(self):
        """Ensure that some specific properties between the 2 tables have the same values, like the Group By column"""
        self.tdst._ddl_columns = self.tsrc.get_ddl_columns()  # TODO we should have the same with BQ and have
        # a check DDL comparison
        self.tdst._group_by_column = self.tsrc.get_groupby_column()  # the Group By must use the same column for both
        # tables

    def perform_step_count(self):
        """Execute the Count comparison of the 2 tables

        :rtype: bool
        :returns: True if we haven't found differences yet and further analysis is needed
        """
        self.synchronise_tables()
        diff, big_small = self.compare_groupby_count()
        return self.show_results_count(diff, big_small)

    def perform_step_sha(self):
        """Execute the Sha comparison of the 2 tables"""
        self.synchronise_tables()
        sha_differences, temporary_tables = self.compare_shas()
        if len(sha_differences) == 0:
            print "Sha queries were done and no differences where found: the tables %s and %s are equal!" \
                  % (self.tsrc.get_id_string(), self.tdst.get_id_string())
            sys.exit(0)
        queries = self.get_sql_final_differences(sha_differences, temporary_tables)
        self.show_results_final_differences(queries[0], queries[1], queries[2])
        sys.exit(1)


def parse_arguments():
    """Parse the arguments received on the command line and returns the args element of argparse

    :rtype: namespace
    :returns: The object that contains all the configuration of the command line
    """
    parser = argparse.ArgumentParser(description="Compare table <source> with table <destination>",
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("source", help="the original (correct version) table\n"
                                       "The format must have the following format: <type>/<database>.<table>\n"
                                       "<type> can be: bq or hive\n ")
    parser.add_argument("destination", help="the destination table that needs to be compared\n"
                                            "Format follows the one for the source table")

    parser.add_argument("-s", "--source-options", help="options for the source table\nFor Hive that could be: {'jar': "
                                                       "'hdfs://hdp/user/sluangsay/lib/sha1.jar', 'hs2': "
                                                       "'master-003.bol.net'}")
    parser.add_argument("-d", "--destination-options", help="options for the destination table")

    parser.add_argument("--source-where", help="the WHERE condition we want to apply for the source table\n"
                                               "Could be useful in case of partitioned tables\n"
                                               "Example: \"datedir='2017-05-01'\"")
    parser.add_argument("--destination-where", help="the WHERE condition we want to apply for the destination table")

    parser.add_argument("--max-gb-percent", type=float, default=1.0,
                        help="if in one sample a column has a value whose frequency is highest than this percentage, "
                             "then this column is discarded (default: 1.0)")

    parser.add_argument("--skew-threshold", type=int,
                        help="increase the threshold (default: 40 000) if you have some skew but if the amount of"
                             "columns is reduced so that you feel confident that all the shas will fit into memory")

    group_columns = parser.add_mutually_exclusive_group()
    group_columns.add_argument("--column-range", default=":",
                               help="Instead of checking all the columns, you can define a range of columns to check\n"
                                    "This works as a Python array-range. Meaning that if you want to only analyze the "
                                    "first 20 columns, you need to give:\n :20")
    group_columns.add_argument("--columns",
                               help="Instead of checking all the columns, you can give here the list of the columns you"
                                    " want to check. Example: 'column1,column14,column23'")

    parser.add_argument("--ignore-columns",
                        help="the column in argument will be ignored from analysis. Example: 'column1,column14,"
                             "column23'")  # not in the group_columns, because we want to be able to ask for a range
    # of columns but removing some few at the same time

    parser.add_argument("--group-by-column",
                        help="the column in argument is enforced to be the Group By column. Can be useful if the sample"
                             "query does not manage to find a good Group By column and we need to avoid some skew")

    group_step = parser.add_mutually_exclusive_group()
    group_step.add_argument("--just-count", help="only perform the Count check", action="store_true")
    group_step.add_argument("--just-sha", help="only perform the final sha check", action="store_true")

    group_log = parser.add_mutually_exclusive_group()
    group_log.add_argument("-v", "--verbose", help="show debug information", action="store_true")
    group_log.add_argument("-q", "--quiet", help="only show important information", action="store_true")

    # TODO put mode to partition, to choose the size of the blocks, etc

    return parser.parse_args()


def create_table_from_args(definition, options, where, args, tc):
    # TODO doc
    table = _Table.create_table_from_string(definition, options, tc)
    table.set_where_condition(where)
    table.set_column_range(args.column_range)
    if args.columns is not None:
        table.set_chosen_columns(args.columns)
    if args.ignore_columns is not None:
        table.set_ignore_columns(args.ignore_columns)
    table.set_group_by_column(args.group_by_column)  # if not defined, then it's None and we'll compute it later

    return table


def main():
    args = parse_arguments()

    level_logging = logging.INFO
    if args.verbose:
        level_logging = logging.DEBUG
    elif args.quiet:
        level_logging = logging.WARNING
    logging.basicConfig(level=level_logging, format='[%(levelname)s]\t[%(asctime)s]  (%(threadName)-10s) %(message)s', )

    logging.debug("Starting comparison program with arguments: %s", args)

    # Create the TableComparator that contains the definition of the 2 tables we want to compare
    tc = TableComparator()
    tc.set_max_percent_most_frequent_value_in_column(args.max_gb_percent)
    source_table = create_table_from_args(args.source, args.source_options, args.source_where, args,tc)
    destination_table = create_table_from_args(args.destination, args.destination_options, args.destination_where,
                                               args,tc)
    if args.skew_threshold is not None:
        tc.set_skew_threshold(args.skew_threshold)
    tc.set_tsrc(source_table)
    tc.set_tdst(destination_table)

    # Step: count
    if not args.just_sha:
        do_we_continue = tc.perform_step_count()
        if not do_we_continue:
            sys.exit(1)

    # Step: sha
    if not args.just_count:
        tc.perform_step_sha()

if __name__ == "__main__":
    main()
