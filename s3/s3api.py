import urllib.parse
import uuid

import boto3
import botocore
import fastavro
import pandas as pd
import s3fs

from shared.utils import oscmd_rmfile


def get_files_in_s3_folder_by_path(s3_path):
    """
    Returns a list of files in the folder identified
    by bucket and prefix. don't use session based
    token if we use IAM role based auth.

    parameters:
    ----------
    s3_path: full s3 path

    return
    ------
    return a list of file name
    """
    use_session = False
    bucket, prefix = parse_s3_path(s3_path)

    if use_session:
        session = boto3.Session()
        client = session.client("s3")
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in response.keys():
            return [f["Key"] for f in response["Contents"]]
        return []

    # don't use session based otken.
    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response.keys():
        return [f["Key"] for f in response["Contents"]]
    return []


def get_files_s3_path_by_path(s3_path):
    """
    Returns a list of files in the folder identified
    by bucket and prefix. don't use session based
    token if we use IAM role based auth.

    parameters:
    ----------
    s3_path: full s3 path

    return
    ------
    return a list of file name in full s3 path
    """
    use_session = False
    bucket, prefix = parse_s3_path(s3_path)

    if use_session:
        session = boto3.Session()
        client = session.client("s3")
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in response.keys():
            return [f["Key"] for f in response["Contents"]]
        return []

    # don't use session based otken.
    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response.keys():
        return ["s3://{}/{}".format(bucket, f["Key"]) for f in response["Contents"]]
    return []


def iterate_files_s3_path_by_path(s3_path):
    """
    Return a generator that iterates over all objects in a given s3 bucket
    don't use session based token if we use IAM role based auth.

    parameters:
    ----------
    s3_path: a full s3 path

    return
    ------
    return a generator that contains a list of file name in full s3 path
    """
    use_session = False
    bucket, prefix = parse_s3_path(s3_path)

    if use_session:
        session = boto3.Session()
        client = session.client("s3")
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in page_iterator:
            if page["KeyCount"] > 0:
                for item in page["Contents"]:
                    yield item

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                yield item


def get_files_in_s3_folder(bucket, prefix):
    """
    Returns a list of files in the folder identified
    by bucket and prefix.

    parameters:
    ----------
    bucket: an s3 bucket name
    prefix: an s3 prefix

    return
    ------
    return a list of file name
    """
    use_session = False
    if use_session:
        session = boto3.Session()
        client = session.client("s3")
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in response.keys():
            return [f["Key"] for f in response["Contents"]]
        return []

    # don't use session based otken.
    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response.keys():
        return [f["Key"] for f in response["Contents"]]
    return []


def check_for_file_s3_path(s3_path):
    """check whether s3 path is valid"""

    client = boto3.client("s3")
    bucket, prefix = parse_s3_path(s3_path)
    try:
        client.head_object(Bucket=bucket, Key=prefix)
        return True
    except botocore.exceptions.ClientError:
        return False


def delete_s3_folder(bucket, prefix):
    """
    Deleted set of folder in a bucket

    parameters:
    -----------
    bucket: an s3 bucket name
    prefix: an s3 prefix
    """

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()
    return


def read_s3_avro_file(s3_filename, userid="test", use_s3fs=False):
    """
    Reads from s3 based avro file and returns
    a pandas dataframe.

    parameters:
    -----------
    s3_filename: an s3 filename
    userid: a prefix of filename for downloading avro file to local
    use_s3fs: a flag indicates whether to use s3fs module or not

    return
    ------
    a panda dataframe
    """

    if use_s3fs:
        if s3_filename.startswith("s3://"):
            s3_filename = s3_filename.split("s3://")[1]

        fs = s3fs.S3FileSystem(anon=False)
        records = []
        with fs.open(s3_filename) as fo:
            records.extend([record for record in fastavro.reader(fo)])
            df = pd.DataFrame(records)
        return df

    # use raw api from boto3 and others.
    bucket, path = parse_s3_path(s3_filename)
    bucket_api = boto3.resource("s3").Bucket(bucket)
    original_filename = path.split("/")[-1].split(".avro")[0]
    tmp_avro_filename = "{}_{}_{}.avro".format(
        userid, original_filename, str(uuid.uuid4())
    )
    with open(tmp_avro_filename, "wb") as fout:
        bucket_api.download_fileobj(path, fout)

    with open(tmp_avro_filename, "rb") as fin:
        records = [record for record in fastavro.reader(fin)]
        df = pd.DataFrame(records)
    oscmd_rmfile(tmp_avro_filename)
    return df


def parse_s3_path(s3_path):
    """
    A helper function to parse a complete s3 path.

    parameters
    ----------
    s3_path: a full s3 path

    return
    ------
    s3 net path and prefix
    """
    s3_detail = urllib.parse.urlparse(s3_path)
    # skip the leading slash
    return s3_detail.netloc, s3_detail.path[1:]


def upload_file_to_s3(filename, s3_path):
    """
    uploads the given file to s3.

    parameters
    ----------
    filename: the local file name to be uploaded
    s3_path: destination s3 path

    return
    ------
    upload status
    """
    s3_client = boto3.client("s3")
    bucket, path = parse_s3_path(s3_path)
    retcode = s3_client.upload_file(filename, bucket, path)
    return retcode


def get_s3_file_as_text(s3_path):
    """
    Suitable only for small text files such as a script.

    parameters
    ----------
    s3_path: s full 3 path

    return
    ------
    file in text
    """
    try:
        bucket, path = parse_s3_path(s3_path)
        s3 = boto3.resource("s3")
        file_obj = s3.Object(bucket, path)
        data = file_obj.get()["Body"].read()
        return data.decode("utf-8")
    except Exception as e:
        print("Failed fetch the file {}".format(s3_path))
        raise e
