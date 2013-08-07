###
#   Copyright (c) 2013 Dillon Woods <dewoods@gmail.com>
#
#   rabbitmq-greenplum-loader is free software; you can redistribute it and/or modify
#   it under the terms of the MIT license. See LICENSE for details.
###

import os
import logging
from pg8000 import DBAPI

def load_data_file( config, uuid, count ):
    loader = Loader( config, uuid, count )

class Loader():
    config = None
    uuid = None
    table_name = None
    ext_table_name = None
    data_file = None
    gpfdist_str = None
    count = 0
    _gp_conn = None

    def __init__( self, config, uuid, count ):
        self.config = config
        self.uuid = str(uuid).encode( 'ascii' )
        self.count = count

        self.table_name = self.config.get( 'greenplum', 'table' )
        self.ext_table_name = "\"rgload_ext_%s\"" % ( self.uuid )
        self.data_file = "%s.dat" % ( self.uuid )
        self.gpfdist_str = "gpfdist://%s:%s" % (
            config.get( 'rgload', 'host' ),
            config.get( 'rgload', 'gpfdist_port' )
        )

        self.run()
        
    @property
    def gp_conn( self ):
        if not self._gp_conn:
            config = self.config
            self._gp_conn = DBAPI.connect(
                host = config.get( 'greenplum', 'pghost' ),
                port = config.getint( 'greenplum', 'pgport' ),
                user = config.get( 'greenplum', 'pguser' ),
                database = config.get( 'greenplum', 'pgdatabase' )
            )
        return self._gp_conn

    def run( self ):
        logging.log( logging.INFO, "[%s]: Starting load", self.uuid )
        
        if self.count > 0:
            self.setup_external_table()
            self.gp_load()
            self.teardown_external_table()
        if self.config.getboolean( 'rgload', 'purgedata' ):
            self.purge_data_file()

        logging.log( logging.INFO, "[%s]: Load complete", self.uuid )

    def setup_external_table( self ):
        cursor = self.gp_conn.cursor()                                                                                                     
        query = "DROP EXTERNAL TABLE IF EXISTS %s" % (self.ext_table_name)
        cursor.execute( query )
        query = "CREATE EXTERNAL TABLE %s( like %s ) LOCATION( '%s/%s' ) FORMAT 'TEXT'( DELIMITER ',')" % (
            self.ext_table_name,
            self.table_name,
            self.gpfdist_str,
            self.data_file
        )
        query = query.encode( 'ascii' )
        cursor.execute( query )
        self.gp_conn.commit()
        cursor.close()

    def gp_load( self ):
        cursor = self.gp_conn.cursor()
        query = "INSERT INTO %s SELECT * FROM %s" % (self.table_name, self.ext_table_name)
        query = query.encode( 'ascii' )
        cursor.execute( query )
        self.gp_conn.commit()
        cursor.close()

    def teardown_external_table( self ):
        cursor = self.gp_conn.cursor()                                                                                                     
        query = "DROP EXTERNAL TABLE IF EXISTS %s" % (self.ext_table_name)
        cursor.execute( query )

        self.gp_conn.commit()
        cursor.close()

    def purge_data_file( self ):
        os.unlink( "%s/%s" % (
            self.config.get( 'rgload', 'datadir' ),
            self.data_file
        ) )
