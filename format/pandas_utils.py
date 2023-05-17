import datetime
import io
import json
import logging
import os
from io import BytesIO
from calendar import monthrange
import tarfile

import numpy as np
import pandas as pd

DICTIONARY_PANDAS = {
    "TIMESTAMP": "DATETIME",
    "DATE": "DATETIME",
    "VARBINARY": "STRING",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "BOOLEAN": "INT8",
    "TINYINT": "INT8",
    "INTEGER": "INT32",
    "BIGINT": "INT64",
    "DOUBLE": "FLOAT"
}


def remove_duplicates_pandas(key_columns, data):
    """
    Remove duplicates by columns.

    Parameters
    ----------
    List : `key_columns`
        Columns name to remove duplicates by.

  removed  Parameters
    ----------
    List : `data`
        To remove duplicates.

    Returns
    -------
    `Dataframe`
        Data without duplicates.
        :param data:
        :param key_columns:
    """
    # Drop selected columns
    data = data.drop_duplicates(subset=key_columns)
    return data


def from_julian_to_gregorian(julian):
    """
    Transform julian date to gregorian date following `yyyy-mm-dd` format.

    Parameters
    ----------
    int : `julian`
            Julian date.

    Returns
    -------
    `string`
        Gregorian date formatted."""
    date_string_format = "%Y-%m-%d"

    gregorian = None

    if julian == 0 or str(julian).strip() == "":
        gregorian = "1900-01-01"
    julian = str(julian)

    if len(julian) == 7:
        year = int(julian[0:4])
        days_in_year = int(julian[-3:])

        if year >= 1900:
            base_date = datetime.datetime(year, 1, 1)

            date = base_date + datetime.timedelta(days_in_year - 1)
            gregorian = datetime.datetime.strftime(date, date_string_format)
        else:
            gregorian = "1900-01-01"

    if len(julian) == 6:
        year = int(julian[-2:])
        days = int(julian[:2])
        month = julian[2:4]

        month = int(month)
        if month > 12 or month == 0 or days > 31 or days == 0:
            gregorian = None
        else:
            year = 2000 + year
            month = "00" + str(month)
            month = month.strip()
            month = int(month[-2:])

            date = datetime.datetime(year=year, month=month, day=days)
            gregorian = datetime.datetime.strftime(date, date_string_format)

    if len(julian) == 5:
        # print("length of 5")
        year = int(julian[-2:])
        days = julian[:1]
        month = julian[:3]
        month = month[-2:]

        # print("year:", year, "\ndays: ", days, "\nmonth:", month)
    
        year = 2000 + year
        month = month.strip()
        month = "00" + month
        month = month[-2:]

        days = days.strip()
        days = "00" + days
        days = days[-2:]

        # print("year:", year, "\ndays: ", days, "\nmonth:", month)

        date = datetime.datetime(year=year, month=int(month), day=int(days))
        gregorian = datetime.datetime.strftime(date, date_string_format)

    if len(julian) < 5 and len(julian) != 1:
        gregorian = None

    return gregorian


def get_json_file(bucket, prefix, client):
    data_info = b''
    response = client.get_object(
        bucket_name=bucket, object_name=prefix)
    for data in response.stream(1024 * 1024 * 1024):
        data_info += data
    data_info = data_info.decode('utf8')
    obj = json.loads(data_info)
    return obj


def number_of_days_in_month(year, month):
    return monthrange(year, month)[1]

def get_list_objects(bucket, prefix, client):
    objects = client.list_objects(
        bucket_name=bucket, prefix=prefix, recursive=True,
    )
    objects = list(objects)
    return objects


def split_list(lst, n):
    # Convert the list to a numpy array
    arr = np.array(lst)
    # Use the numpy array_split() function to split the array into sub-arrays
    sub_arrays = np.array_split(arr, n)
    # Convert each sub-array back to a list
    result = [sub_arr.tolist() for sub_arr in sub_arrays]
    return result

def number_of_days_in_month(year, month):
    return monthrange(year, month)[1]