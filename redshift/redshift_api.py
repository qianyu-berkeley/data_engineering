import logging

import pandas as pd
import pgpasslib
import psycopg2

from shared.etlexceptions import BaseExpTaskException


def connect_to_redshift(
    host_add=None,
    dbname=None,
    user=None,
    port=None,
    pwd=None,
):
    """Connect to Redshift server
    - redshift parameter can be obtained from environment variables
    - password can be obtained using pgpasslib module

    parameters:
    -----------
    host_add: redshift host
    dbname: redshift database name
    user: redshift user id
    port: redshift port name
    pwd: redshift user password
    """

    # user name need to be provided
    if user is None:
        raise (
            Exception(
                "Did not provide a user_name for:\n\thost: {0}\n\tdbname: {1}".format(
                    host_add, dbname
                )
            )
        )

    # get password
    if pwd is None:
        pwd = pgpasslib.getpass(host=host_add, port=port, dbname=dbname, user=user)

    # connect to redshift with fixed user / password
    try:
        conn = psycopg2.connect(
            dbname=dbname, host=host_add, port=port, user=user, password=pwd
        )
        logging.info("Successfully connected to Redshift")
        return conn
    except Exception as err:
        logging.error(
            "Unable to connect to the database with error {} of error_code: {}".format(
                err, err.code
            )
        )

    raise (BaseExpTaskException("Error in fetching the Redshift Connection"))


def run_query_from_file(user, script, *args):
    """Return dataframe of a single SQL Query
    Run single SQL Query with parameters subsitution

    parameters
    ----------
    user: redshift user id
    script: SQL query script in a file
    *args: list of parameters used in the SQL query

    return
    ------
    Query results in a panda dataframe
    """

    # redshift connection
    conn = connect_to_redshift(user=user)
    cur = conn.cursor()

    f = open(script, "r")
    query = f.read()

    try:
        cur.execute(query, args)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)
        logging.info("Completed Query Data")
        logging.info("-----------------------------------")
        cur = None
        return df
    except psycopg2.Error as e:
        logging.error("Unable to run Query!")
        logging.error(e.pgerror)


def run_query(user, query, *args):
    """Return dataframe of a single SQL Query
    Run single SQL Query with parameters subsitution

    parameters
    ----------
    user: redshift user id
    query: SQL query in string
    *args: list of parameters used in the SQL query

    return
    ------
    Query results in a panda dataframe
    """

    # reshift connection
    conn = connect_to_redshift(user=user)
    cur = conn.cursor()

    try:
        cur.execute(query, args)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colnames)
        logging.info("Completed Query Data")
        logging.info("-----------------------------------")
        cur = None
        return df
    except psycopg2.Error as e:
        logging.error("Unable to run Query!")
        logging.error(e.pgerror)
