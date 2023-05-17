#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid

import logging
import datetime
import hashlib
from datetime import timedelta
from datetime import datetime as dtime

DEFAULT_DATE_FORMAT_YYYY_MM_DD = "%Y-%m-%d"


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
            gregorian = datetime.datetime.strftime(
                date, date_string_format)
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
            gregorian = datetime.datetime.strftime(
                date, date_string_format)

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

        date = datetime.datetime(
            year=year, month=int(month), day=int(days))
        gregorian = datetime.datetime.strftime(date, date_string_format)

    if len(julian) < 5 and len(julian) != 1:
        gregorian = None

    return gregorian


def hash_string(string):
    """
    Hash string with sha256.

    Parameters
    ----------
    string : `string`
            String to hash.

    Returns
    -------
    `string`
        String hash."""
    if string is not None:
        try:
            string = str(string)
        except UnicodeEncodeError:
            string = string.encode("ascii", "ignore").decode("ascii")
        return hashlib.sha256(string.encode()).hexdigest()
    return string


def get_push_down_predicate(is_for_month=False, processed_date=None):
    """
    Build a push down predicate based on a specific date,
    if processed_date is None, push down predicate will be the current date.
    Current and previous month data can be retrieved with push down predicate if
    is_for_month is True.

    Available push_down_predicates options.

    By default: `"partition_0 = {} and partition_1 = {} and partition_2 = {}"`

    is_for_month=True:`"partition_0 = {} and partition_1 >= {} and partition_2 >= {}"`

    Parameters
    ----------
    Boolean : `is_for_month`
        Build push down predicate to take current and previous month data.

    Returns
    -------
    `string`
        push_down_predicate.            
    """
    PUSH_DOWN_PREDICATE_STRING = "partition_0 {} {} and partition_1 {} {} and partition_2 {} {}"

    if processed_date is None:
        processed_date = datetime.date.today()

    year = processed_date.strftime("%Y")
    month = processed_date.strftime("%m")
    day = processed_date.strftime("%d")

    print("Year: ", year)
    print("Month: ", month)
    print("Day: ", day)

    if is_for_month:
        print("Is for month")
        month = processed_date.month - 1 if processed_date.month > 1 else 12
        year = processed_date.year - 1 if processed_date.month == 1 else processed_date.year

        init_date = processed_date.replace(day=1, month=month, year=year)
        print("Init date: ", init_date)
        return PUSH_DOWN_PREDICATE_STRING.format(
            ">=" if year < processed_date.year else "=",
            init_date.strftime("%Y"),
            ">=",
            init_date.strftime("%m"),
            ">=",
            init_date.strftime("%d"))

    return PUSH_DOWN_PREDICATE_STRING.format("=", year, "=", month, "=", day)


def remove_duplicates(key_columns, data):
    """
    Remove duplicates by columns.

    Parameters
    ----------
    List : `key_columns`
        Columns name to remove duplicates by.

    Parameters
    ----------
    List : `data`
        To remove duplicates.

    Returns
    -------
    `Dataframe`
        Data without duplicates.            
    """
    print("Show duplicates before filter")
    grouped_data = data.groupBy(
        key_columns).count().where(col("count") > 1)
    key_columns.append("count")
    grouped_data.select(key_columns).show(300, False)

    # Drop selected columns
    key_columns.remove("count")
    data = data.dropDuplicates(key_columns)

    print("Show duplicates after filter")
    grouped_data = data.groupBy(
        key_columns).count().where(col("count") > 1)
    key_columns.append("count")
    grouped_data.select(key_columns).show(300, False)

    return data


def get_average(date_to_process, data):
    """
    Get the average data of a specific date. Take the first date of date_to_process
    and iterate through to date_to_process, get value for each date, if date not exists,
    take the closest value to fill it. Finally, get the average.

    Parameters
    ----------
    date_to_process : `datetime.date`
            Indicates the date required to get the average.

    data : `dict`
            Dictionary of data containing date and value column values ordered ascending.

    Returns
    -------
    `float`
        Average data for date_to_process."""
    print("-------------------DATE TO PROCESS-------------------", date_to_process)

    pivot_date = date_to_process.replace(day=1)

    tmp_data = {}
    while pivot_date <= date_to_process:
        print("PIVOT DATE: ", pivot_date)
        try:
            value = data[pivot_date]
            print("VALUE: ", value)
            tmp_data[pivot_date] = value
            print("-------------------------------------------------------")
        except KeyError:
            print("------------NOT EXISTS, GET PREVOUS VALUE------------")
            key = max(key for key in data.keys() if key < pivot_date)
            print("CLOSEST DATE: ", key)
            value = data[key]
            print("VALUE: ", value)
            tmp_data[pivot_date] = value
            print("-------------------------------------------------------")

        pivot_date = (pivot_date + datetime.timedelta(days=1))

    print("GETTING AVERAGE FOR: ", tmp_data)
    _sum = sum(tmp_data.values())
    _len = len(tmp_data)
    _avg = _sum / _len
    print("_sum: ", _sum)
    print("_len: ", _len)
    print("_avg: ", _avg)
    return _avg


def to_date(str_date):
    """
    Convert a string date to defaul date format "YYYY-MM-DD".

    Parameters
    ----------
    str_date : `string`
            Date to convert with default date format.

    Returns
    -------
    `datetime.date`
        date."""
    return datetime.datetime.strptime(str_date, Utils.DEFAULT_DATE_FORMAT_YYYY_MM_DD).date()


def validate_delta_date_in_data(data, delta_date_key, delta_date_value):
    """
    Validate if a delta date is contained in data.

    Parameters
    ----------
    data : `dataframe`
            Data to validate.

    delta_date_key : `string`
            Delta column name.

    delta_date_value : `date`
            Delta column value to validate.

    Returns
    -------
    `dataframe`
        Filtered data if delta date is contained in data, otherwise data with last available delta date."""
    # Filter data by delta_date key and value
    delta = data.filter(data[delta_date_key] == delta_date_value)
    # Verify delta data
    if delta.count() == 0:
        print("No delta data")
        # Get last delta date
        delta = data.sort(delta_date_key, ascending=False)
        delta = delta.filter(
            delta[delta_date_key] <= delta_date_value).limit(1)
        delta_date = delta.select(delta_date_key).first()[0]
        print("delta_date: ", delta_date)

        # filter by last delta date
        delta = data.filter(data[delta_date_key] == delta_date)

    return delta


def get_date_now(format_date):
    date_now = dtime.now()
    date_subtract = date_now - timedelta(hours=6)
    date_string = date_subtract.strftime(format_date)

    return date_string


def gregorian_to_julian(date_string, format_date):
    #julian_date = datetime.strptime(date_string, format_date).strftime('%Y%j')
    julian_date = dtime.strptime(date_string, format_date).strftime('%Y%j')

    return julian_date


def get_partition_date(bucket_scope, date, name):
    year = date[0:4]
    month = date[4:6]
    day = date[6:8]
    key = '{}/year={}/month={}/day={}/{}'.format(bucket_scope, year, month, day, f'{name}-{uuid.uuid4()}')
    return key
