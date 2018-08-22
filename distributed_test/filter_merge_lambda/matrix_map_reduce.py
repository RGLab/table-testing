"""Lambdas to demonstrate a distributed filter and merge of expression matrices
using different file formats.

By "filter and merge", I mean a query along the lines of "in this list of matrices,
find me all the cells that express CD4 and have a qc value greater than .6, and merge
them together".

This is an experiment implementing such a query on lambdas. Lambdas are very burstable
and horizontally scalable, but not all tasks can be subdivided into subtasks that can
run on small, short-lived workers.

The query is kicked off with a request the the following fields:
    - inputs: a list lf {"bucket": bucket, "prefix": prefix} objects that refer
      to the input matrices
    - format: the format of those matrices. Has to match something in
      FORMAT_HANDLERS below
    - filter_string: a pandas-like filter where "matrix" refers to the
      expression matrix. For the query above, it would be
      "matrix['CD4'] > 0 & matrix['qc'] > .6"

There is an attempt to separate the orchestration concerns from the file manipulation.
There are four lambda functions:
    - driver: accepts the request, sticks an id on it, kicks off other lambdas
      and returns. This is just to separate what a "user" of the test framework works
      with from the execution of the filter and merge.
    - mapper: Takes one input file and figures out how to distribute it in
      small enough chunks to worker lambdas that will perform the filtering
    - do_work: perform the filtering on a chunk of work created by mapper
    - reducer: merge all the work chunks together into a single matrix

For some formats, one or more of these steps may be trivial.

This means that a particular format needs to implement three functions that these lambdas
will call:

    - mapper(request_id, bucket, prefix) -> List of work_chunk_spec dicts
    - work(request_id, filter_string, **work_chunk_spec)
    - reducer(request_id)

(This is all slightly overfit to parquet at the moment. Also, it uses a dynamodb to
manage global state, which.....works?)
"""

import botocore
import boto3
import json
import os
import uuid

# Needed for parquet
import fastparquet
import pyarrow
import pyarrow.parquet
import s3fs

# These are set in the cloudformation template
CFN_VARS = {
    "mapper_fn": os.environ.get("MAPPER_FN"),
    "work_fn": os.environ.get("WORK_FN"),
    "reducer_fn": os.environ.get("REDUCER_FN"),
    "state_table": os.environ.get("STATE_TABLE"),
    "result_bucket": os.environ.get("RESULT_BUCKET")
}

# A couple convenient interfaces to aws
LAMBDA_CLIENT = boto3.client("lambda", region_name="us-east-1")
STATE_TABLE = boto3.resource("dynamodb", region_name="us-east-1").Table(CFN_VARS["state_table"])

##############################################################################
##############################################################################
#    _                    _         _
#   | |    __ _ _ __ ___ | |__   __| | __ _ ___
#   | |   / _` | '_ ` _ \| '_ \ / _` |/ _` / __|
#   | |__| (_| | | | | | | |_) | (_| | (_| \__ \
#   |_____\__,_|_| |_| |_|_.__/ \__,_|\__,_|___/
#
##############################################################################
##############################################################################

def driver(event, context):
    """Initiate the matrix filter and merge. Return quickly after kicking off
    the work.

    Assume this function is invoked via an API Gateway lambda proxy
    integration.
    """

    body = event["body"]

    # Expect body to look like this
    # {
    #  "format": "parquet_simple",
    #  "inputs": [
    #    {"bucket": bucket_name, "prefix": prefix},
    #    ...
    #  ],
    #  "filter_string": "matrix['CD4'] > 0 & matrix['qc11'] < .25"
    # }

    # Do we have the keys we expect?
    missing_keys = [k for k in ["format", "inputs", "filter_string"] if k not in body]
    if missing_keys:
        return {
            "statusCode": "400",
            "body": json.dumps({
                "msg": "Missing required keys in body: {}".format(missing_keys),
            })
        }

    # Do we know what to do with this format?
    format_ = body["format"]
    if format_ not in FORMAT_HANDLERS:
        return {
            "statusCode": "400",
            "body": json.dumps({
                "msg": "Format {} not recognized".format(format_),
            })
        }

    # Okay, let's go for it
    request_id = str(uuid.uuid4())

    # Record the request in the state table
    STATE_TABLE.put_item(
        Item={
            "request_id": request_id,
            "expected_work_executions": 0,
            "completed_work_executions": 0,
            "expected_mapper_executions": len(body["inputs"]),
            "completed_mapper_executions": 0,
            "expected_reducer_executions": 1,
            "completed_reducer_executions": 0
        }
    )

    # Run mappers on each input
    for input_ in body["inputs"]:
        mapper_payload = {
            "request_id": request_id,
            "bucket": input_["bucket"],
            "prefix": input_["prefix"],
            "format": format_,
            "filter_string": event["body"]["filter_string"]
        }

        LAMBDA_CLIENT.invoke(
            FunctionName=CFN_VARS["mapper_fn"],
            InvocationType="Event",
            Payload=json.dumps(mapper_payload).encode()
        )

    # And return the request id to the caller
    return {
        "statusCode": "200",
        "body": json.dumps({"request_id": request_id})
    }


def mapper(event, context):
    """Distribute work from one (bucket, prefix) pair to worker lambdas."""

    format_mapper_fn = FORMAT_HANDLERS[event["format"]]["mapper"]
    work_chunk_specs = format_mapper_fn(event["request_id"], event["bucket"], event["prefix"])

    increment_state_field(event["request_id"], "expected_work_executions", len(work_chunk_specs))

    for work_chunk_spec in work_chunk_specs:
        work_payload = {
            "request_id": event["request_id"],
            "format": event["format"],
            "filter_string": event["filter_string"],
            "work_chunk_spec": work_chunk_spec
        }
        LAMBDA_CLIENT.invoke(
            FunctionName=CFN_VARS["work_fn"],
            InvocationType="Event",
            Payload=json.dumps(work_payload).encode()
        )

    increment_state_field(event["request_id"], "completed_mapper_executions", 1)

def do_work(event, context):
    """Filter one work chunk."""

    format_work_fn = FORMAT_HANDLERS[event["format"]]["work"]

    work_chunk_spec = event["work_chunk_spec"]

    format_work_fn(event["request_id"], event["filter_string"], **work_chunk_spec)

    increment_state_field(event["request_id"], "completed_work_executions", 1)

    # Are we all done? Then run the reducer
    item = STATE_TABLE.get_item(
        Key={"request_id": event["request_id"]},
        ConsistentRead=True
    )

    done_mapping = (item["Item"]["expected_mapper_executions"] ==
                    item["Item"]["completed_mapper_executions"])

    done_working = (item["Item"]["expected_work_executions"] ==
                    item["Item"]["completed_work_executions"])

    if done_mapping and done_working:
        reducer_payload = {
            "request_id": event["request_id"],
            "format": event["format"]
        }
        LAMBDA_CLIENT.invoke(
            FunctionName=CFN_VARS["reducer_fn"],
            InvocationType="Event",
            Payload=json.dumps(reducer_payload).encode()
        )

def reducer(event, context):
    format_reducer_fn = FORMAT_HANDLERS[event["format"]]["reducer"]
    format_reducer_fn(event["request_id"])
    increment_state_field(event["request_id"], "completed_reducer_executions", 1)

##############################################################################
##############################################################################
#    _   _      _
#   | | | | ___| |_ __   ___ _ __ ___
#   | |_| |/ _ \ | '_ \ / _ \ '__/ __|
#   |  _  |  __/ | |_) |  __/ |  \__ \
#   |_| |_|\___|_| .__/ \___|_|  |___/
#                |_|
##############################################################################
##############################################################################

def increment_state_field(request_id, field_name, increment_size):
    """Safely increment a count in the state table."""

    while True:
        item = STATE_TABLE.get_item(
            Key={"request_id": request_id},
            ConsistentRead=True
        )
        current_value = item["Item"][field_name]
        new_value = current_value + increment_size

        try:
            STATE_TABLE.update_item(
                Key={"request_id": request_id},
                UpdateExpression=f"SET {field_name} = :n",
                ConditionExpression=f"{field_name} = :c",
                ExpressionAttributeValues={":n": new_value, ":c": current_value}
            )
        except botocore.exceptions.ClientError as exc:
            print(exc)
            continue
        break

##############################################################################
##############################################################################
#    ____                            _
#   |  _ \ __ _ _ __ __ _ _   _  ___| |_
#   | |_) / _` | '__/ _` | | | |/ _ \ __|
#   |  __/ (_| | | | (_| | |_| |  __/ |_
#   |_|   \__,_|_|  \__, |\__,_|\___|\__|
#                      |_|
##############################################################################
##############################################################################
# Parquet Simple
# A parquet archive contained in a single file (with multiple row groups)
##############################################################################

def parquet_simple_mapper(request_id, bucket, prefix):

    fs = s3fs.S3FileSystem(anon=True)
    s3_url = f"s3://{bucket}/{prefix}"
    pq = pyarrow.parquet.ParquetFile(fs.open(s3_url))

    return [{"bucket": bucket, "prefix": prefix, "row_group": row_group}
            for row_group in range(pq.num_row_groups)]

def parquet_simple_work(request_id, filter_string, bucket, prefix, row_group):

    fs = s3fs.S3FileSystem(anon=True)
    s3_url = f"s3://{bucket}/{prefix}"
    pq = pyarrow.parquet.ParquetFile(fs.open(s3_url))

    table = pq.read_row_group(row_group)
    matrix = table.to_pandas(zero_copy_only=True)

    filtered_matrix = matrix[eval(filter_string)]

    shard_id = str(uuid.uuid4())
    result_bucket = CFN_VARS["result_bucket"]
    dest_s3_url = f"s3://{result_bucket}/{request_id}/{shard_id}.parquet"
    fs = s3fs.S3FileSystem(anon=False)
    pyarrow.parquet.write_table(
        pyarrow.Table.from_pandas(filtered_matrix),
        fs.open(dest_s3_url, "wb"),
        compression="BROTLI"
    )


def parquet_simple_reducer(request_id):

    fs = s3fs.S3FileSystem()

    result_bucket = CFN_VARS["result_bucket"]
    s3_url = f's3://{result_bucket}/{request_id}'

    shards = fs.ls(s3_url)
    # This is the slowest step of the whole process...
    fastparquet.writer.merge(shards, open_with=fs.open)

# Record functions that handle different concerns for different formats here
FORMAT_HANDLERS = {
    "parquet_simple": {
        "mapper": parquet_simple_mapper,
        "work": parquet_simple_work,
        "reducer": parquet_simple_reducer
    }
}