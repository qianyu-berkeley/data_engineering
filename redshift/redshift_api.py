import logging

import pandas as pd
import pgpasslib
import psycopg2

from shared.exp_exceptions import BaseExpTaskException


def connect_to_reshift(
    host_add=None,
    dbname=None,
    user=None,
    port=None,
    pwd=None,
):
    """Connect to Redshift server

    parameters:
    -----------
    credential: reshift credential in dictionary
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
    credential: reshift credential in dictionary

    query: Text variable of SQL query

    *args: list of parameters used in the SQL query
    """

    # reshift connection
    conn = connect_to_reshift(user=user)
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
    credential: reshift credential in dictionary

    query: Text variable of SQL query

    *args: list of parameters used in the SQL query
    """

    # reshift connection
    conn = connect_to_reshift(user=user)
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
