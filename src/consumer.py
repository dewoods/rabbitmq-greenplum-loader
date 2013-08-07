###
#   Copyright (c) 2013 Dillon Woods <dewoods@gmail.com>
#
#   rabbitmq-greenplum-loader is free software; you can redistribute it and/or modify
#   it under the terms of the MIT license. See LICENSE for details.
###

from multiprocessing import Process
import logging
import uuid
from loader import load_data_file

class Consumer():
    config = None
    current_uuid = None
    current_count = 0
    data_file = None
    loaders = []

    def __init__( self, config ):
        self.config = config
        self.new_data_file()
        logging.log( logging.INFO, "[c]: Greenplum consumer started" )

    def shutdown( self ):
        if self.data_file:
            self.data_file.close()

        logging.log( logging.INFO, "[c]: Flushing in process loads" )
        self.load( self.current_uuid )

        logging.log( logging.INFO, "[c]: Waiting for loads to finish" )
        for p in self.loaders:
            p.join()

    def newrow_callback( self, ch, method, properties, body ):
        self.data_file.write( "%s\n" % (body) )

        ##
        # Start load if we've reached the threshold
        self.current_count = self.current_count + 1
        if self.current_count == self.config.getint( 'rgload', 'maxrows' ):
            self.load( self.current_uuid )
            self.new_data_file()

    def load( self, uuid ):
        logging.log( logging.INFO, "[c]: Starting Greenplum loader process" )
        p = Process( target=load_data_file, kwargs={ 'uuid': uuid, 'config': self.config, 'count': self.current_count } )
        self.loaders.append( p )
        p.start()

    def new_data_file( self ):
        if self.data_file:
            self.data_file.close()
        self.current_uuid = uuid.uuid4()
        self.current_count = 0
        self.data_file = open( "%s/%s.dat" % ( self.config.get( 'rgload', 'datadir' ), self.current_uuid ), "w" )
