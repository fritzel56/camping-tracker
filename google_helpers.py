"""Code used to write to and query from google BigQuery.
"""
def get_bq_data(sql, client):
    """Queries BQ using the passed SQL query and returns the result

    Args:
        sql (str): the query to be run.
        client (client): client to connect to BQ.

    Returns:
        df: The result of the passed SQL query
    """
    return client.query(sql).result().to_dataframe()


def write_to_gbq(data, client, table):
    """Takes in a dataframe and writes the values to BQ

    Args:
        data (df): the dataframe to be written
        client (client): client to connect to BQ.
        table (str): the table to be written to.
    """
    # convert to list of lists
    rows_to_insert = data.values.tolist()
    # write data
    errors = client.insert_rows(table, rows_to_insert)
    if errors != []:
        print(errors)
        assert errors == [], 'There were errors writing data see above.'
