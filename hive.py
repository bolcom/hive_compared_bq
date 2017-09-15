import logging
import re
import sys
import time
# noinspection PyProtectedMember
from hive_compared_bq import _Table
import pyhs2  # TODO switch to another module since this one is deprecated and does not support Python 3
# see notes in : https://github.com/BradRuderman/pyhs2
if sys.version_info[0:2] == (2, 6):
    # noinspection PyUnresolvedReferences
    from backport_collections import Counter
else:
    from collections import Counter


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
        max_frequent_number = self.tc.sample_rows_number * self.tc.max_percent_most_frequent_value_in_column // 100
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
        self.query("DROP TABLE " + table_name).close()

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
        split_maxsize = 8000000
        # split_maxsize = 16000000
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

        tmp_table = "%s.temp_hiveCmpBq_%s_%s" % (self.database, self.full_name.replace('.', '_'),
                                                 str(time.time()).replace('.', '_'))
        cur.execute("CREATE TABLE " + tmp_table + " AS\n" + query)
        cur.close()
        result["names_sha_tables"][self.get_id_string()] = tmp_table  # we confirm this table has been created
        result["cleaning"].append((tmp_table, self))

        logging.debug("The temporary table for Hive is " + tmp_table)

        if "error" in result:  # A problem happened in the other query of the other table (usually BQ, since it is
            # faster than Hive) so there is no need to pursue or have the temp table
            return

        projection_hive_row_sha = "SELECT gb, row_sha_gb FROM %s" % tmp_table
        self.launch_query_dict_result(projection_hive_row_sha, result["sha_dictionaries"][self.get_id_string()])
