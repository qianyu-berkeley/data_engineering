import json
import os
from collections import defaultdict
from datetime import datetime

import pandas as pd
import pkg_resources
from s3api import parse_s3_path

timestamp_format = "%Y-%m-%dT%H:%M:%S"


class MetaUtils(object):

    YEAR_PRE = "year="
    MONTH_PRE = "month="
    DAY_PRE = "day="
    HOUR_PRE = "hour="

    @staticmethod
    def generate_run_date_path(batch_date):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return "{0}{1}/{2}{3}/{4}{5}".format(
            MetaUtils.YEAR_PRE,
            dobj.strftime("%Y"),
            MetaUtils.MONTH_PRE,
            dobj.strftime("%m"),
            MetaUtils.DAY_PRE,
            dobj.strftime("%d"),
        )

    @staticmethod
    def generate_run_date_hour_path(batch_date):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return "{0}{1}/{2}{3}/{4}{5}/{6}{7}".format(
            MetaUtils.YEAR_PRE,
            dobj.strftime("%Y"),
            MetaUtils.MONTH_PRE,
            dobj.strftime("%m"),
            MetaUtils.DAY_PRE,
            dobj.strftime("%d"),
            MetaUtils.HOUR_PRE,
            dobj.strftime(("%H")),
        )

    @staticmethod
    def generate_run_year_month(batch_date):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return "{0}{1}/{2}{3}".format(
            MetaUtils.YEAR_PRE,
            dobj.strftime("%Y"),
            MetaUtils.MONTH_PRE,
            dobj.strftime("%m"),
        )

    @staticmethod
    def extract_from_string_with_format(batch_date, tmformat):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return dobj.strftime(tmformat)


class KinesisPartitionFormatter(object):
    @staticmethod
    def generate_run_date_path(batch_date):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return "{0}/{1}/{2}".format(
            dobj.strftime("%Y"), dobj.strftime("%m"), dobj.strftime("%d")
        )

    @staticmethod
    def generate_run_date_hour_path(batch_date):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return "{0}/{1}/{2}/{3}".format(
            dobj.strftime("%Y"),
            dobj.strftime("%m"),
            dobj.strftime("%d"),
            dobj.strftime("%H"),
        )

    @staticmethod
    def generate_run_year_month(batch_date):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return "{0}/{1}".format(dobj.strftime("%Y"), dobj.strftime("%m"))

    @staticmethod
    def extract_from_string_with_format(batch_date, tmformat):
        dobj = datetime.strptime(batch_date, timestamp_format)
        return dobj.strftime(tmformat)


class SchemaResource(object):
    def __init__(self, module, filepath):

        self._module = module
        self._filepath = filepath
        self._schema_file = pkg_resources.resource_filename(
            self._module, self._filepath
        )

    def schema_file(self):
        return self._schema_file


class Table(object):
    def __init__(self, dbroot, name, schema=None, partitiontype="ymd"):
        self._name = name
        self._partition_type = partitiontype
        self._schema_file = schema
        self._rootpath = dbroot

    @property
    def name(self):
        return self._name

    @property
    def schema(self):
        return self._schema_file

    def meta(self):
        return "{}\t{}".format(self.name, self.schema)

    def prefix(self):

        return "{0}/{1}".format(self._rootpath.lower(), self._name.lower())

    def sub_prefix(self):

        bucket, prefix = parse_s3_path(self.path())
        return prefix

    def bucket(self):

        bucket, prefix = parse_s3_path(self.path())
        return bucket

    def partition_path(self, rdate=None):

        if rdate is None:
            return "{0}/{1}".format(self._rootpath, self._name)

        if self._partition_type == "ymd":
            # partition has year/month/day
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_date_path(rdate)
            )
        elif self._partition_type == "ymdh":
            # partition has year/month/day
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_date_hour_path(rdate)
            )
        elif self._partition_type == "ym":
            # partition has year/month
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_year_month(rdate)
            )
        elif self._partition_type == "y":
            # partition has year
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_year_month(rdate)
            )
        elif self._partition_type is None:
            return "{0}/{1}".format(self._rootpath, self._name)

    def path(self, rdate=None):

        if rdate is None:
            return "{0}/{1}".format(self._rootpath, self._name)

        if self._partition_type == "ymd":
            # partition has year/month/day
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_date_path(rdate)
            )
        elif self._partition_type == "ymdh":
            # partition has year/month/day
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_date_hour_path(rdate)
            )
        elif self._partition_type == "ym":
            # partition has year/month
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_year_month(rdate)
            )
        elif self._partition_type == "y":
            # partition has year
            return "{0}/{1}/{2}".format(
                self._rootpath, self._name, MetaUtils.generate_run_year_month(rdate)
            )
        elif self._partition_type is None:
            return "{0}/{1}".format(self._rootpath, self._name)

    def get_table_schema_column_list(self):

        with open(self.schema, "r") as fin:
            schema_json = json.load(fin)
            columns = [str(x["name"]) for x in schema_json["fields"]]
        return columns


class S3Table(Table):
    def __init__(self, dbroot, name, schema=None, partition_type="ymd"):

        super(S3Table, self).__init__(dbroot, name, schema, partition_type)


class S3TableWithPartition(Table):
    def __init__(self, dbroot, name, partition_descritor, schema=None):

        # initialize with partition_type as None for base
        # class behavior
        super(S3TableWithPartition, self).__init__(dbroot, name, schema)
        # carry the tuple for partition here.
        # kind of overrides the base class behavior
        self._partition_descriptor = partition_descritor
        self._partition_vals = None
        self._attached_path = None

    def path_by_vals(self, partition_vals, separator="="):
        """
        Returns the path by partition values
        :param partition_vals:
        :return:
        """
        p_val_pairs = [
            "{}{}{}".format(k, separator, v)
            for k, v in zip(self._partition_descriptor, partition_vals)
        ]
        s3_path = "/".join(p_val_pairs)
        return "{}/{}".format(self.path(), s3_path)

    def s3_bucket_key(self, partition_vals, separator="=", filename=None):

        path = self.path_by_vals(partition_vals, separator)
        if filename:
            path += f"/{filename}"
        bucket, key = parse_s3_path(path)
        return bucket, key

    def attach(self, partition_vals, separator="="):

        self._partition_vals = partition_vals
        self._separator = separator
        self._attached_path = self.path_by_vals(partition_vals, separator)
        return

    def attached_path(self):

        return self._attached_path


class KinesisTable(S3Table):
    def __init__(self, dbroot, name, schema=None, partition_type="ymdh"):

        super(KinesisTable, self).__init__(dbroot, name, partition_type=partition_type)

    def path(self, rdate=None):

        if rdate is None:
            return "{0}/{1}".format(self._rootpath, self._name)

        if isinstance(rdate, pd.Timestamp):
            rdate = rdate.strftime(timestamp_format)

        if self._partition_type == "ymd":
            # partition has year/month/day
            return "{0}/{1}/{2}".format(
                self._rootpath,
                self._name,
                KinesisPartitionFormatter.generate_run_date_path(rdate),
            )
        elif self._partition_type == "ymdh":
            # partition has year/month/day
            return "{0}/{1}/{2}".format(
                self._rootpath,
                self._name,
                KinesisPartitionFormatter.generate_run_date_hour_path(rdate),
            )
        elif self._partition_type == "ym":
            # partition has year/month
            return "{0}/{1}/{2}".format(
                self._rootpath,
                self._name,
                KinesisPartitionFormatter.generate_run_year_month(rdate),
            )
        elif self._partition_type == "y":
            # partition has year
            return "{0}/{1}/{2}".format(
                self._rootpath,
                self._name,
                KinesisPartitionFormatter.generate_run_year_month(rdate),
            )
        elif self._partition_type is None:
            return "{0}/{1}".format(self._rootpath, self._name)


class Database(object):

    """
    A database is a collection of table & metadata
    """

    def __init__(self, name, rootpath):

        self._name = name
        self._db = defaultdict()
        self._schema_store = None
        self._root_path = rootpath
        self._default_partition = "ymd"

    @property
    def name(self):
        return self._name

    @property
    def root_path(self):
        return self._root_path

    @property
    def schema_store(self):
        return self._schema_store

    def _register_s3_table(self, table_name, fqs_path, custom_partition_type):

        default_partition_type = self.partition_type
        if custom_partition_type:
            table_object = Table(
                self.root_path, table_name, fqs_path, custom_partition_type
            )
        else:
            table_object = Table(
                self.root_path, table_name, fqs_path, default_partition_type
            )

        self._db[table_name] = table_object

    def register_table(self, table_name, schema, custom_partition_type=None):

        # make the fully qualified schema path
        fqs_path = os.path.join(self.schema_store, schema)
        return self._register_s3_table(table_name, fqs_path, custom_partition_type)

    def get_table(self, table):
        if table in self._db.keys():
            return self._db[table]
        return None

    def get_tables(self):
        """
        Returns the iterator for walking through the
        tables for this database.
        :return:
        """
        for tbl, impl in self._db.items():
            yield tbl

    @property
    def tbl(self):
        return self._db

    @property
    def partition_type(self):
        return self._default_partition

    def unregister_table(self, table):

        if table in self._db.keys():
            del self._db[table]

    def isPartitionNone(self):

        return self._default_partition is None

    def show(self):
        """
        dumps the tables information.
        :return:
        """

        for table, impl in self._db.items():

            print(
                "{0}\t{1}\t{2}\t{3}\n".format(
                    self._name,
                    impl.name,
                    impl.path(rdate="2018-11-03T10:00:00"),
                    impl.schema,
                )
            )

    def table_path(self, tablename):
        """
        Retruns the Table path
        :param tablename:
        :return:
        """
        return "{0}/{1}/{2}".format(
            self._root_path, self._name, Table(tablename).path()
        )


class Metastore(object):

    # catalog of databases
    metastore = defaultdict()

    def __init__(self, name):

        self._metastore_name = name
        self._schema_store = None

    def metainfo(self, database):

        if "database" in self.metastore.keys():
            return self.metastore["database"]
        raise Exception("Metastore: Error in fetching metadata")

    def register_database(self, name, impl):

        self.metastore[name] = impl
        return

    def get_database(self, name):
        return self.metastore[name]

    def list_databases(self):
        """
        returns a list of databases
        :return:
        """
        print(
            "Metastore {} is serving the following tables".format(
                "<<<" + self._metastore_name + ">>>"
            )
        )
        print("{0}\t{1}\t{2}\t{3}\n".format("DataBase", "Table", "Path", "Schema"))
        for db, impl in self.metastore.items():
            impl.show()
        return

    def register_table(self, db_name, table_name, schema, custom_parition_type=None):
        """

        :param db_name:
        :param table_name:
        :param schema:
        :param custom_parition_type:
        :return:
        """
        dbimpl = self.metastore[db_name]
        dbimpl.register_table(table_name, schema, custom_parition_type)
        return

    def get_catalog(self, name):
        return self.metastore[name]

    def get_table(self, catalog_name, table_name):
        """
        Adding alternative to access table object as a class method
        """
        return self.get_catalog(catalog_name).tbl[table_name]
