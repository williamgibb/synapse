#!/usr/bin/env python
# -*- coding: utf-8 -*-
# XXX Update Docstring
"""
mss_data - ibutton_temps.py
Created on 3/12/17.


"""
# Stdlib
import argparse
import collections
import csv
import datetime
import hashlib
import json
import logging
import os
import sys
import uuid
# Third Party Code
from synapse import cortex
# Custom Code


log = logging.getLogger(__name__)

"""
Daily minimum temp, daily maximum temperature, average daily temperature, accumulated growing degree days (GDD), and the average temperature between 7am and 7pm.


GDD = [ (daily min + daily max)/2 ] - 10 deg C

That gets added up across the days.


William Gibb <williamgibb@gmail.com>
Jan 23

to Maria, bcc: me
Items 1-3 and 5 are clear.
Your GDD calculation is given as (avg(dailymin,dailymax))-10C.  Is there a threshold that determines if the result of that is a growing day or not?  Otherwise the GDDs are not clear.

-Will


Maria Smith <mssmith373@gmail.com>
Jan 23

to me
Yes, 10C is a baseline temperature for metabolic activity in grapes.  A presumed ave daily temperature would have a GDD of 0. Any daily average temp > 10C would indicate GDD.


"""
ROW = 'Row'
BLOCK = 'Block'
LOCATION_KEYS = [ROW, BLOCK]

def parse_order_csv(fp: str) -> dict:
    ret = {}
    with open(fp, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            ret[i] = row
    return ret


def undo_tufo(tufo: tuple, prop_base='mss_ibutton'):
    tid, props = tufo
    d = {
        ROW: int(props.get('{}:{}'.format(prop_base, ROW))),
        BLOCK: int(props.get('{}:{}'.format(prop_base, BLOCK))),
        'temp': float(props.get('{}:temperature'.format(prop_base))),
        'date': datetime.datetime.strptime(props.get('{}:date'.format(prop_base)), TIME_FORMAT)
    }
    return d


def parse_ibutton_csv(fp: str, order_d: dict):
    with open(fp, 'rb') as f:
        ibutton_found = False
        i = 0
        ibutton = ButtonData()
        ibutton.metadata[ROW] = order_d[i].get(ROW)
        ibutton.metadata[BLOCK] = order_d[i].get(BLOCK)
        lines = f.readlines()
        for line in lines:
            line = line.decode(encoding='latin-1').strip()

            if not line:
                if ibutton_found:
                    yield ibutton
                    i = i + 1
                    ibutton = ButtonData()
                    ibutton.metadata[ROW] = order_d[i].get(ROW)
                    ibutton.metadata[BLOCK] = order_d[i].get(BLOCK)
                    ibutton_found = False
                continue

            if 'download complete' in line:
                if ibutton.metadata != {} and ibutton.rows != []:
                    yield ibutton
                return

            key, value = line.split(',')

            key = key.strip('"')
            value = value.strip('"')
            if key.endswith(':'):
                key = key.rstrip(':')

            if key == 'Date/time logger downloaded':
                ibutton_found = True

            if ibutton_found:
                try:
                    key = datetime.datetime.strptime(key, TIME_FORMAT)
                except ValueError as e:
                    ibutton.metadata[key] = value
                else:
                    value = float(value)
                    ibutton.append((key, value))

            else:
                continue
                # print(key, value)

class ButtonData(object):
    def __init__(self):
        self.rows = []
        self.metadata = {}
        self.daily_minimums = {}
        self.daily_maximums = {}
        self.daily_average = {}
        self.missing_full_days = set([])
        self.daily_halfday_average = {}
        self.missing_half_days = set([])
        self.gdd = {} # Year -> count

    def __repr__(self):
        return '<ButtonData object at {id}, gdd: {gdd}, missing half days: {mhd}, missing full days {mfd} >'.format(
                id=hex(id(self)),
                gdd=self.gdd,
                mhd=len(self.missing_half_days),
                mfd=len(self.missing_full_days)
        )

    def gen_tufos(self):
        for key in LOCATION_KEYS:
            if key not in self.metadata:
                raise ValueError('Missing location key: {}'.format(key))
        for date, temperature in self.rows:
            # XXX Why can't we ingest a datetime structure into a tufo property?
            # XXX Schema requirement I need to look into?
            # XXX WTF We don't support floats?
            # d = {'date': date,
            #      'temperature': temperature}
            d = {
                'date': date.strftime(TIME_FORMAT),
                'temperature': str(temperature)
                }
            for key in LOCATION_KEYS:
                d[key] = self.metadata.get(key)
            # Add in anymore metadata about the measurement itself?
            guid_d = d.copy()
            d['button'] = self.metadata.get('Logger serial number', 'MissingSerialNumber')
            s = json.dumps(guid_d, sort_keys=True)
            h = hashlib.md5(s.encode())
            guid = uuid.UUID(bytes=h.digest())
            yield str(guid), d

    def append(self, row):
        self.rows.append(row)

    def analyze_button(self):
        # Compute whole day data
        self.compute_daily_data()
        # Compute half day data
        self.compute_half_day_data()
        # Compute growing days
        self.compute_gdd()

    def compute_half_day_data(self):
        daily_data = collections.defaultdict(list)
        max_len = 0
        for dt, temp in self.rows:
            # Ensure we're collecting samples between 7am and 7pm
            if dt.hour < 7 or dt.hour > 18:
                continue
            day = datetime.datetime(year=dt.year, month=dt.month, day=dt.day)
            daily_data[day].append(temp)

        for day, temps in daily_data.items():
            l = len(temps)
            if l > max_len:
                max_len = l

        if max_len == 0:
            raise ValueError('No temps data availible')
        if max_len != 24:
            log.debug('Expected to get 24 samples in a half day period; only got {}'.format(max_len))

        for day in sorted(list(daily_data.keys())):
            temps = daily_data.get(day)
            if len(temps) < max_len:
                log.debug('Half day {} is missing samples - expected {}, got {}.'.format(day, max_len, len(temps)))
                self.missing_half_days.add(day)
                continue
            s = sum(temps)
            avg_temp = s / len(temps)
            self.daily_halfday_average[day] = avg_temp

    def compute_gdd(self):
        if not self.daily_minimums:
            raise ValueError('Must compute daily minimums first')
        if not self.daily_maximums:
            raise ValueError('Must compute daily maximums first.')
        dmindays = set(list(self.daily_minimums.keys()))
        dmaxdays = set(list(self.daily_maximums.keys()))
        if dmindays != dmaxdays:
            raise ValueError('Missing days between daily min and max values.')

        log.debug('Computing GDD between: {} and {}'.format(min(dmindays), max(dmindays)))

        # GDD = true if [ (daily min + daily max)/2 ] - 10 deg C
        # Yes, 10C is a baseline temperature for metabolic activity in grapes.
        # A presumed ave daily temperature would have a GDD of 0. Any daily average temp > 10C would indicate GDD.
        dmindays = list(dmindays)
        dmindays.sort()
        current_year = dmindays[0].year
        log.debug('Starting with gdd for [{}]'.format(current_year))
        for day in dmindays:
            if day.year > current_year:
                log.debug('Now computing gdd for [{}]'.format(current_year))
                current_year = day.year
            avg_temp = (self.daily_minimums.get(day) + self.daily_maximums.get(day)) / 2
            if avg_temp > 10.0:
                if current_year in self.gdd:
                    self.gdd[current_year] = self.gdd[current_year] + 1
                else:
                    self.gdd[current_year] = 1

    def write_computed_data_to_files(self, fdir=None):
        if not fdir:
            fdir = os.getcwd()

        row, block = self.metadata.get(ROW), self.metadata.get(BLOCK)

        fn_base = 'row_{}_block_{}'.format(row, block)
        fn = '{}_gdd.csv'.format(fn_base)
        fp = os.path.join(fdir, fn)
        with open(fp, 'w') as f:
            w = csv.DictWriter(f, [ROW, BLOCK, 'year', 'gdd'])
            w.writeheader()
            for k, v in self.gdd.items():
                d = {'year': k,
                     'gdd': v,
                     ROW: row,
                     BLOCK: block}
                w.writerow(d)
        fn = '{}_daily_min.csv'.format(fn_base)
        fp = os.path.join(fdir, fn)
        with open(fp, 'w') as f:
            w = csv.DictWriter(f, [ROW, BLOCK, 'date', 'daily_min'])
            w.writeheader()
            for k, v in self.daily_minimums.items():
                d = {
                    'date': k,
                    'daily_min': v,
                    ROW: row,
                    BLOCK: block
                    }
                w.writerow(d)
        fn = '{}_daily_max.csv'.format(fn_base)
        fp = os.path.join(fdir, fn)
        with open(fp, 'w') as f:
            w = csv.DictWriter(f, [ROW, BLOCK, 'date', 'daily_max'])
            w.writeheader()
            for k, v in self.daily_maximums.items():
                d = {
                    'date': k,
                    'daily_max': v,
                    ROW: row,
                    BLOCK: block
                }
                w.writerow(d)
        fn = '{}_daily_avg.csv'.format(fn_base)
        fp = os.path.join(fdir, fn)
        with open(fp, 'w') as f:
            w = csv.DictWriter(f, [ROW, BLOCK, 'date', 'daily_avg'])
            w.writeheader()
            for k, v in self.daily_average.items():
                d = {
                    'date': k,
                    'daily_avg': v,
                    ROW: row,
                    BLOCK: block
                }
                w.writerow(d)
        fn = '{}_daily_midday_avg.csv'.format(fn_base)
        fp = os.path.join(fdir, fn)
        with open(fp, 'w') as f:
            w = csv.DictWriter(f, [ROW, BLOCK, 'date', 'midday_avg'])
            w.writeheader()
            for k, v in self.daily_halfday_average.items():
                d = {
                    'date': k,
                    'midday_avg': v,
                    ROW: row,
                    BLOCK: block
                }
                w.writerow(d)

    def compute_daily_data(self):
        daily_data = collections.defaultdict(list)
        max_len = 0
        for dt, temp in self.rows:
            day = datetime.datetime(year=dt.year, month=dt.month, day=dt.day)
            daily_data[day].append(temp)
        for day, temps in daily_data.items():
            l = len(temps)
            if l > max_len:
                max_len = l

        if max_len == 0:
            raise ValueError('No temps data availible')

        for day in sorted(list(daily_data.keys())):
            temps = daily_data.get(day)
            if len(temps) < max_len:
                log.debug('Day {} is missing samples - expected {}, got {}.'.format(day, max_len, len(temps)))
                self.missing_full_days.add(day)
                continue
            # log.info('{} - # of samples {}'.format(day, len(temps)))
            s = sum(temps)
            avg_temp = s / len(temps)
            min_temp = min(temps)
            max_temp = max(temps)
            self.daily_minimums[day] = min_temp
            self.daily_maximums[day] = max_temp
            self.daily_average[day] = avg_temp



# 2015/12/22 00:01:00
TIME_FORMAT = '%Y/%m/%d %H:%M:%S'


# noinspection PyMissingOrEmptyDocstring
def main(options):  # pragma: no cover
    if not options.verbose:
        logging.disable(logging.DEBUG)

    c = cortex.openurl('sqlite:////Users/wgibb/Documents/projects/synapse/ibutton_data.db')

    order_d = parse_order_csv(fp=options.order)

    for button in parse_ibutton_csv(options.input, order_d):
        for guid, d in button.gen_tufos():
            r = c.formTufoByProp('mss_ibutton', guid, **d)
            if not r[1].get('.new'):
                log.debug('Tufo already existed for {}'.format(r[0]))
        log.info('Ingested data for ibutton from {}:{}'.format(button.metadata.get(ROW), button.metadata.get(BLOCK)))

    sys.exit(0)


# noinspection PyMissingOrEmptyDocstring
def makeargpaser():  # pragma: no cover
    # XXX Fill in description
    parser = argparse.ArgumentParser(description="Description.")
    parser.add_argument('-i', '--input', dest='input', required=True, type=str, action='store',
                        help='Input file to process')
    parser.add_argument('-o', '--order_csv', dest='order', required=True, type=str,
                        action='store', help='Order CSV file')
    parser.add_argument('-v', '--verbose', dest='verbose', default=False, action='store_true',
                        help='Enable verbose output')
    return parser


def _main():  # pragma: no cover
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s [%(levelname)s] %(message)s [%(filename)s:%(funcName)s]')
    log.info('hi')
    p = makeargpaser()
    opts = p.parse_args()
    main(opts)


if __name__ == '__main__':  # pragma: no cover
    _main()
