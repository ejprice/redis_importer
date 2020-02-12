# redis_importer
Some tooling that loads a csv into a Redis database for fast/easy queries. I designed it to be used as a package and it will return an iterable with the same interface as csv.DictReader(). Therefore, there are no code modifications needed to use it. Redis runs in memory, using about ~ 450MiB for a 208MiB csv file. It is very, very fast once the import is done. The Redis import takes about 10 minutes to load 750,000 records from the 208MiB file. We use LZMA to lower the Redis memory footprint 
when serializing the records to Redis.

Here is how to use it:

1) Make sure Docker and docker-compose are installed to run the Redis database. You also need the redis module so install that at the same time.
apt update
apt install -y docker docker-compose python3-redis

2) Start Redis
cd redis_importer && docker-compose up -d

Redis will now be running in a container and listening on localhost. 

3) import the data into Redis on the command line 
./csv_importer.py --help

./csv_importer.py -v store ../mp_export-2020-02-09/omlhist2.csv 0

This will load the data into the Redis database at index 0. You can use other indexes as well for other files.
Redis will persist the data on shutdown into the ./redis-data directory. IMPORTANT - the "store" 
function initializes/flushes the index to zero records before storing the data. 

To query the db from Python:
from redis_importer import csv_importer
myredis = csv_importer.CSV2Redis()
-or-
myredis = csv_importer.CSV2Redis(db_index=2) # index 0 is default

recs = myredis.get_records('AX3974')

for rec in recs:
   print("{} {} {}".format(rec['INVOICE-NO'], rec['INVOICE-LINE-NO'], rec['ITEM-CODE']))

AX3974 001 325PC
AX3974 002 20W
AX3974 003 9PLATE
AX3974 004 PL4N
AX3974 005 V16C
AX3974 006 V32C
AX3974 007 V8NC
AX3974 008 SPARKLE5
AX3974 009 VINYLL
AX3974 010 12FILM

CSV2Redis.get_records() returns an iterable List of OrderedDict() so this is a drop-in replacement for the 
csv.DictReader. The redis module is thread-safe so multiply threads or processes can query at the same time. 

You can also do the Redis import in Python if you really want with the CSV2Redis.store_csv() method. I don't, but it works. 

4) Shut down Redis
cd redis_importer && docker-compose down

The Redis database will persist in the redis-data directory and load when you start it again. 
