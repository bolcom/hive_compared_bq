import logging
import sys
import time
# noinspection PyProtectedMember
from hive_compared_bq import _Table
from google.cloud import bigquery


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
        logging.debug("The cache table of the final comparison query in BigQuery is: " + cache_table)

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
