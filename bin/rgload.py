#!/usr/bin/env python

###
#   Copyright (c) 2013 Dillon Woods <dewoods@gmail.com>
#
#   rabbitmq-greenplum-loader is free software; you can redistribute it and/or modify
#   it under the terms of the MIT license. See LICENSE for details.
###

import sys
import logging
from optparse import OptionParser
import configparser
import pika

sys.path.append( "src" )
from consumer import Consumer

def main():
    ##
    # Parse command line options
    parser = OptionParser()
    parser.add_option( "-d", "--debug", dest="debug", action="store_true", default=False )
    parser.add_option( "-c", "--config", dest="config", default="sample.ini" )
    (options, args) = parser.parse_args()

    ##
    # Parse configuration file
    config = configparser.ConfigParser();
    config.read( options.config )

    ##
    # Setup application logging
    log_level = logging.INFO
    if options.debug:
        log_level = logging.DEBUG
    logging.basicConfig( format="%(asctime)s:%(process)s-[%(levelname)s]:-%(message)s", level=log_level )

    ##
    # Connect to RabbitMQ
    rabbit_config = config['rabbitmq']
    rabbit_credentials = pika.PlainCredentials( rabbit_config['user'], rabbit_config['password'] )
    rabbit_params = pika.ConnectionParameters(
        rabbit_config['host'],
        int(rabbit_config['port']),
        rabbit_config['vhost'],
        rabbit_credentials
    );
    rabbit_connection = pika.BlockingConnection( rabbit_params )
    rabbit_channel = rabbit_connection.channel()

    rabbit_channel.exchange_declare(
        exchange=rabbit_config['exchange'],
        type='direct',
        passive=True
    )

    result = rabbit_channel.queue_declare( exclusive=True )
    queue_name = result.method.queue

    rabbit_channel.queue_bind(
        exchange=rabbit_config['exchange'],
        queue=queue_name,
        routing_key=rabbit_config['route']
    )

    logging.log( logging.INFO, "Established RabbitMQ connection" )

    ##
    # Create a consumer
    consumer = Consumer( config )

    ##
    # Begin ingestion
    rabbit_channel.basic_consume( consumer.newrow_callback, queue=queue_name, no_ack=True )

    try:
        rabbit_channel.start_consuming()
    except( KeyboardInterrupt ):
        logging.log( logging.INFO, "Shutting down..." )
        consumer.shutdown()
    
    return 0

if __name__ == '__main__':
    status = main()
    sys.exit( status )
