import pyhs2
from collections import Counter

server = 'shd-hdp-a-master-003.bolcom.net'
myDatabase = 'ldebruijn'
#  myTable = 'ppp_retail_promotion_reference_group'
myTable = 'ppp_retail_promotion'

# 10 000 rows should be good enough to use as a sample
#  (Google uses some sample of 1000 or 3000 rows)
#  Estimation of memory for the Counters: 10000 * 10 * ( 10 * 4 + 4) * 4 = 16.8 MB
#  (based on estimation : rows * columns * ( size string + int) * overhead Counter )
sample_rows_number = 10000
max_percent_most_frequent_value_in_column = 1  # if in one sample a column has a value whose frequency is highest than
#  this percentage, then this column is discarded
number_of_most_frequent_values_to_weight = 50

fullHiveTable = myDatabase + "." + myTable
conn = pyhs2.connect(host=server, port=10000, authMechanism="KERBEROS", database=myDatabase)

ddlColumns = []  # array instead of dictionary because we want to maintain the order of the columns
ddlPartitions = []


def get_table_ddl(table):
    """ Get the DDL of the (Hive) table and store it into arrays"""
    is_col_def = True
    cur = conn.cursor()
    cur.execute("describe " + table)
    while cur.hasMoreRows:
        row = str(cur.fetchone())
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
    selected_columns = ddlColumns[:10]  # TODO make the range as an optional argument
    for col in selected_columns:
        query = query + " " + col["name"] + ","  # for the last column we'll remove that trailing ","
    query = query[:-1] + " FROM " + table + " LIMIT " + str(sample_rows_number)
    return query, selected_columns


def find_groupby_column(table):
    """Use a sample to find a column that seems enough spread to do interesting GROUP BY on it"""
    query, selected_columns = get_sample_query(table)

    #  Get a sample from the table and fill Counters to each column
    print "Analizing the columns " + str([x["name"] for x in selected_columns]) + " with a sample " \
        "of " + str(sample_rows_number) + " values"
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
            print "Discarding column " + col["name"] + " because the value " + str(highest[0]) + " was found with " \
                  "a frequency in sample is " + str(highest[1]) + ", which is highest than " + str(max_frequent_number)
            continue
        # The biggest value is not too high, so let's see how big are the 50 biggest values
        weight_of_most_frequent_values = sum([x[1] for x in col["Counter"]
                                             .most_common(number_of_most_frequent_values_to_weight)])
        print col["name"] + " " + str(weight_of_most_frequent_values)
        if weight_of_most_frequent_values < minimum_weight:
            column_with_minimum_weight = col["name"]
            minimum_weight = weight_of_most_frequent_values
            highest_first = highest[1]
    print "Best column to do a GROUP BY is " + column_with_minimum_weight + " (" + str(highest_first) + " /" \
        " " + str(minimum_weight) + ")"


get_table_ddl(fullHiveTable)
find_groupby_column(fullHiveTable)
