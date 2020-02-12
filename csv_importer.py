#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import lzma
import sys
import time
from collections import OrderedDict
from functools import partial

# redis module is required from pypi but may not be installed
try:
    import redis
except ImportError:
    print("The redis module is not installed.", sys.stderr)
    sys.exit(1)

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class CSV2Redis:
    """Loads a csv into Redis using the first column as the key to an array.
    The entire csv line, including the key, is compressed using LZMA then added to Redis. Subsequent lines with the same key
    are appended to the end of the array. The actual csv line is read into an OrderedDict by csv.DictReader and
    then we serialize it into JSON for storage in Redis"""

    def __init__(self, hostname="localhost", db_index=0, port=6379, password=''):
        """Constructor instantiates a connection to Redis DB
        :rtype: object
        """
        try:
            self.redis = redis.Redis(host=hostname, port=port, password=password, db=db_index)
            # Dont trust connection - it lies, so lets do an operation on Redis
            logger.debug(json.dumps(self.redis.info('server'), indent=3))
        except redis.RedisError as e:
            logger.error("Error connecting to Redis: {}".format(e))
            sys.exit(1)

    def _get_record_count(self, file_name: str):
        """Counts lines so we know how many records"""
        buffer = 2 ** 16
        with open(file_name) as f:
            return sum(x.count('\n') for x in iter(partial(f.read, buffer), ''))

    def store_csv(self, csv_file):
        """Read the CSV file into the Redis DB"""
        # Clear existing keys from the Redis database
        self.redis.flushdb()

        with open(csv_file, newline='') as f:
            try:
                n_records = self._get_record_count(csv_file)
                logger.debug("processing {} records".format(n_records))
                reader = csv.DictReader(f, dialect="excel")
                # used for a counters on when to output debug data (every ~ 5%)
                check_count = round(n_records // 20, -2)
                for row in reader:
                    # Get the key by taking the name of the first column in the csv file
                    key = reader.fieldnames[0]
                    # Log some output in verbose mode
                    if reader.line_num % check_count == 0:
                        logger.debug("On record {} Processed {} lines of {}. Elapsed process time {}".format(
                            row[key], reader.line_num, n_records, time.process_time()))
                    # Serialize the OrderedDict(row) as JSON for storage then compress with LZMA
                    json_ordered_dict = json.dumps(row).encode("UTF-8")
                    compressed_row = lzma.compress(json_ordered_dict,
                                                   format=lzma.FORMAT_XZ,
                                                   check=lzma.CHECK_CRC64,
                                                   preset=1
                                                   )
                    # Write to Redis
                    self.redis.rpush(row[key], compressed_row)
            except Exception as e:
                logger.error("Error storing csv row to Redis\n Row: {1}\nException:\n{2}".format(
                    reader.line_num, e
                ))
        # Tell Redis to do a background save the data to disk
        self.redis.bgsave()

    def get_records(self, key):
        """Queries Redis using 'key' and returns a List of OrderedDicts, the same as csv.DictReader()"""
        # Get all rows using key
        compressed_rows = self.redis.lrange(key, 0, -1)
        rows = [OrderedDict(json.loads(lzma.decompress(r))) for r in compressed_rows]
        return rows


def main():
    parser = argparse.ArgumentParser(conflict_handler='resolve', description=str(CSV2Redis.__doc__))

    parser.add_argument('action', choices=['store', 'get'], help="the action to perform")
    parser.add_argument('input', metavar='input', type=str,
                        help='cvs filename for store operation or query key for get operation')
    parser.add_argument('db_index', metavar='db_index', type=int,
                        help='the database index to use [0-15]')
    parser.add_argument('-h', '--hostname', dest='hostname', type=str, required=False, default='localhost',
                        help='the Redis database hostname (default localhost)')
    parser.add_argument('-p', '--port', dest='port', type=int, required=False, default='6379',
                        help='the Redis database port number (default 6379)')
    parser.add_argument('-a', '--password', dest='password', type=str, required=False, default='',
                        help='the Redis database password (default is blank)')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose output')

    args = parser.parse_args()
    action: str = args.action
    db_index: int = args.db_index
    hostname: str = args.hostname
    port: int = args.port
    password: str = args.password
    debug: bool = args.verbose

    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(str(args))

    csv_worker = CSV2Redis(hostname=hostname, port=port, db_index=db_index, password=password)

    if action == 'store':
        csv_worker.store_csv(args.input)
    elif action == 'get':
        records = csv_worker.get_records(args.input)
        [print("{}\n".format(record)) for record in records]
        print("count: {}".format(len(records)))
    else:
        parser.print_usage()


if __name__ == '__main__':
    main()
