rabbitmq-greenplum-loader
=========================

Use Greenplum's parallel loader to batch load messages from RabbitMQ into Greenplum

Installation
------------

###Dependencies

- Python - tested with version 2.7.3
- Running RabbitMQ instance
- Running Greenplum DB cluster
- GPFdist started and pointed to the correct data directory

Required Python packages:
- [configparser](https://pypi.python.org/pypi/configparser)
- [pg8000](https://pypi.python.org/pypi/pg8000/1.08)
- [pika](https://pypi.python.org/pypi/pika/0.9.13)

Add rgload.py to your path

    $ export PATH=$PATH:bin/rgload.py

Usage
-----

Create a configuration file based on sample.ini.  Be sure Greenplum and RabbitMQ connectivity options are correct.

Start rgload.py, use a process control system like [Supervisor](http://supervisord.org/) to manage if desired.

    $ python rgload.py -c config.ini

Sample expected output:

    $ python rgload.py -c test.ini
    2013-08-07 14:53:41,789:24125-[INFO]:-Connecting to 172.16.81.133:5672
    2013-08-07 14:53:41,839:24125-[INFO]:-Established RabbitMQ connection
    2013-08-07 14:53:41,840:24125-[INFO]:-[c]: Greenplum consumer started
    2013-08-07 14:53:52,598:24125-[INFO]:-[c]: Starting Greenplum loader process
    2013-08-07 14:53:52,635:24228-[INFO]:-[3a044674-7744-48af-ba1a-771f6302f953]: Starting load
    2013-08-07 14:53:52,638:24125-[INFO]:-[c]: Starting Greenplum loader process
    2013-08-07 14:53:52,653:24229-[INFO]:-[88770bd9-4bb9-40e6-9ee9-072174089a78]: Starting load
    2013-08-07 14:53:52,821:24228-[INFO]:-[3a044674-7744-48af-ba1a-771f6302f953]: Load complete
    2013-08-07 14:53:52,822:24229-[INFO]:-[88770bd9-4bb9-40e6-9ee9-072174089a78]: Load complete
    2013-08-07 14:53:54,741:24125-[INFO]:-Shutting down...
    2013-08-07 14:53:54,742:24125-[INFO]:-[c]: Flushing in process loads
    2013-08-07 14:53:54,742:24125-[INFO]:-[c]: Starting Greenplum loader process
    2013-08-07 14:53:54,743:24125-[INFO]:-[c]: Waiting for loads to finish
    2013-08-07 14:53:54,745:24231-[INFO]:-[655174cd-621e-472d-a965-1cb8f7624dd3]: Starting load
    2013-08-07 14:53:54,746:24231-[INFO]:-[655174cd-621e-472d-a965-1cb8f7624dd3]: Load complete

Limitations
-----------

The following known limitations will be addressed in a future release:
- Use FIFO pipes instead of files for intermediate data files
