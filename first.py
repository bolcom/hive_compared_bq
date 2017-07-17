import pyhs2

server = 'shd-hdp-a-master-003.bolcom.net'
myDatabase = 'ldebruijn'
myTable = 'ppp_retail_promotion'
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
    selected_columns = ddlColumns[:5]  # TODO make the range as an optional argument
    for col in selected_columns:
        query = query + " " + col["name"] + ","  # for the last column we'll remove that trailing ","
    query = query[:-1] + " FROM " + table + " LIMIT 10"  # TODO put 1000 or a greater number
    return query, selected_columns


def find_groupby_column(table):
    """Use a sample to find a column that seems enough spread to do interesting GROUP BY on it"""
    query, selected_columns = get_sample_query(table)
    column_names = [x["name"] for x in selected_columns]
    print column_names
    print query

    cur = conn.cursor()
    cur.execute(query)
    while cur.hasMoreRows:
        fetched = cur.fetchone()
        if fetched == None:
            continue
        print
    cur.close()

get_table_ddl(fullHiveTable)
find_groupby_column(fullHiveTable)
