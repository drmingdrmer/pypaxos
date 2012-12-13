#!/usr/bin/env python2.6
# coding: utf-8


import os, sys
import time
import shutil
import socket
import unittest

import testframe
import log

dd = testframe.dd

class Base( testframe.TestFrame ):

    hostdir = './xp'
    hostdir2 = './xp2'
    fsize = 1024*1024

    def setUp( self ):
        if os.path.exists( self.hostdir ):
            shutil.rmtree( self.hostdir )

        self.log = log.Log( hostdir=self.hostdir, fsize=self.fsize )

    def tearDown( self ):
        for d in ( self.hostdir, self.hostdir2 ):
            if os.path.exists( d ):
                shutil.rmtree( d )

    def exception( self, func, exc, msg='' ):
        try:
            func()
            fail( repr( exc ) + ' expected. ' + msg )
        except exc as e:
            pass


class TestReadWrite( Base ):

    def test_empty_history( self ):
        log2 = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.eq( 0, log2.seq )

    def test_write( self ):

        self.eq( 0, self.log.seq )
        prev_start = 0

        for val in ( "1", "2", "3" ):
            ( start, end ), err = self.log.write( val )
            self.eq( prev_start, start )
            self.eq( prev_start + self.log.rec_len_size + len( val ), end )
            prev_start = end

    def test_read( self ):

        val = '123'
        self.log.write( val )

        ( seq, next_seq, data ), err = self.log.read( 0 )
        self.eq( 0, seq )
        self.eq( val, data )

        pos = []
        for ii in range( 128 ):
            val = str( ii*ii )
            (s, e), err = self.log.write( val )
            pos.append( ( s, e, val ) )

        for s, e, val in pos:
            (seq, next_seq, data), err = self.log.read( s )
            self.eq( s, seq )
            self.eq( val, data )

    def test_write_data_just_fit_in_block( self ):

        sz = self.log.body_size - self.log.rec_len_size

        val = '1' * sz
        self.log.write( val )

        (seq, next_seq, data), err = self.log.read( 0 )
        self.eq( val, data )

        val = '1' * sz
        self.log.write( val )

        (seq, next_seq, data),err = self.log.read( 512 )
        self.eq( val, data )

    def test_write_smaller_than_block( self ):

        sz = self.log.body_size - 1 - self.log.rec_len_size

        val = '1' * sz
        self.log.write( val )

        (seq, next_seq, data),err = self.log.read( 0 )
        self.eq( val, data )

        val = '1' * sz
        self.log.write( val )

        (seq, next_seq, data),err = self.log.read( sz + self.log.rec_len_size )
        self.eq( val, data )


    def test_write_larger_than_block( self ):

        sz = self.log.body_size + 1 - self.log.rec_len_size
        val = '1' * sz

        for ii in range( 10 ):
            self.log.write( val )

        for ii in range( 10 ):
            (seq, next_seq, data),err = self.log.read( ( self.log.block_size + 1 ) * ii )
            self.eq( val, data )

    def test_write_very_large( self ):

        sz = int(self.fsize * 10.3)
        val = '1' * sz

        for ii in range( 2 ):
            (pos, end), err = self.log.write( val )
            (seq, next_seq, data),err = self.log.read( pos )
            self.eq( val, data )

    def test_read_next( self ):
        val = '1234567'
        for ii in range( 32 ):
            self.log.write( val )


        for i in range( self.log.seq - len( val ) - self.log.rec_len_size ):
            (seq, next_seq, data),err = self.log.read( i, readnext=True )
            self.eq( val, data )

    def test_out_of_range( self ):

        val = '123'
        self.log.write( val )

        _, err = self.log.read( 1 )
        self.eq( 'InvalidSeq', err.err )

        _, err = self.log.read( 1, readnext=True )
        self.eq( 'OutofRange', err.err )

        val = '456'
        self.log.write( val )

        _, err = self.log.read( 7 )
        self.eq( None, err )

        _, err = self.log.read( 6 )
        self.eq( 'InvalidSeq', err.err )

        (seq, next_seq, data), err = self.log.read( 6, readnext=True )
        self.eq( None, err )
        self.eq( val, data )

        (seq, next_seq, data), err = self.log.read( 8, readnext=True )
        self.eq( 'OutofRange', err.err )



class TestReopen( Base ):

    def test_reopen_0_byte_file( self ):

        with open( self.log.hostdir + '/log-0000000001' ) as f:
            st = os.fstat( f.fileno() )
            size = st.st_size
        self.eq( 0, size )

        log2 = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.eq( 0, log2.seq )

    def test_reopen_1_block_file( self ):

        v = '123'
        self.log.write( v )

        with open( self.log.hostdir + '/log-0000000001' ) as f:
            st = os.fstat( f.fileno() )
            size = st.st_size
        self.eq( 512, size )

        log2 = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.eq( self.log.rec_len_size + len( v ), log2.seq )

    def test_reopen_multi_block_file( self ):

        v = '123'
        for ii in range( 100 ):
            self.log.write( v )

        seq = self.log.seq

        log2 = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.eq( seq, log2.seq )

class TestReset( Base ):

    def test_reset( self ):

        ( s, e ), err = self.log.write( '123' )
        ( s, e ), err = self.log.write( '456' )

        self.eq( 14, e )
        self.eq( 14, self.log.seq )

        _, err = self.log.reset( 8 )
        self.eq( 'InvalidSeq', err.err )

        seq, err = self.log.reset( 7 )
        self.eq( 7, seq )
        self.eq( 7, self.log.seq )

        (s, e), err = self.log.write( '789' )
        self.eq( 7, s )
        self.eq( 14, e )

        rst, err = self.log.read( 7 )
        self.eq( None, err )
        self.eq( ( 7, 14, '789' ), rst )


    def test_reset_persistent( self ):

        ( s, e ), err = self.log.write( "123" )
        ( s, e ), err = self.log.write( "456" )

        self.log.reset( s )
        self.eq( 0, self.log.block_first_seq )

        log2 = log.Log( hostdir=self.log.hostdir )

        self.eq( s, log2.seq )

        (seq, next_seq, data),err = self.log.read( 0 )
        self.eq( '123', data )

        rst, err = self.log.read( s )
        self.eq( 'OutofRange', err.err )

    def test_reset_cross_file( self ):

        val = '1' * (self.fsize + 100)

        self.log.write( val )
        (seq, end), err = self.log.write( val )

        self.eq( 3, self.log.i )
        self.eq( 'log-0000000003', self.log.curfn )
        self.eq( 3, len( self.log.files ) )


        self.log.reset( seq )
        self.eq( seq, self.log.seq )
        self.eq( 2, self.log.i )
        self.eq( 'log-0000000002', self.log.curfn )
        self.eq( 2, len( self.log.files ) )


class TestReadLast( Base ):

    def test_read_last_empty( self ):
        rst, err = self.log.read_last()
        self.eq( 'OutofRange', err.err )


    def test_read_last_1_elt( self ):

        v = '123'
        self.log.write( v )

        (seq, next_seq, data),err = self.log.read_last()
        self.eq( 0, seq )
        self.eq( len( v ) + self.log.rec_len_size, next_seq )
        self.eq( v, data )

    def test_read_last_2_elt( self ):

        self.log.write( 'blabla' )

        v = '123'
        self.log.write( v )

        (seq, next_seq, data),err = self.log.read_last()
        self.eq( v, data )

    def test_read_last_cross_block( self ):

        for ii in range( 100 ):
            self.log.write( 'xyz' )

        v = '123'
        self.log.write( v )

        (seq, next_seq, data),err = self.log.read_last()
        self.eq( v, data )

    def test_read_last_after_reset( self ):

        v = 'xyz'
        self.log.write( 'blabla' )
        (seq, next_seq), err = self.log.write( 'xyz' )
        self.log.write( '123' )

        self.log.reset( next_seq )

        (seq, next_seq, data),err = self.log.read_last()
        self.eq( v, data )


class TestCreate( Base ):

    def test_init( self ):

        self.true( self.log.inited )
        self.eq( None, self.log.err )

        l = log.Log( hostdir=self.hostdir2, fsize=self.fsize, create=False )
        self.false( l.inited )
        self.eq( None, l.err )
        self.false( os.path.exists( self.hostdir2 ) )
        self.false( os.path.exists( os.path.join( self.hostdir2, 'ENV' ) ) )
        self.eq( 0, len( l.files ) )

        self._test_uninitialized( l )
        rc, err = l.init()
        self._test_init_log( l )


    def test_not_inited_only_hostdir( self ):

        os.mkdir( self.hostdir2 )

        l = log.Log( hostdir=self.hostdir2, fsize=self.fsize, create=False )
        self.false( l.inited )
        self.eq( None, l.err )
        self.false( os.path.exists( os.path.join( self.hostdir2, 'ENV' ) ) )
        self.eq( 0, len( l.files ) )

        self._test_uninitialized( l )
        rc, err = l.init()
        self._test_init_log( l )


    def test_not_inited_hostdir_and_env( self ):

        os.mkdir( self.hostdir2 )

        envfn = os.path.join( self.hostdir2, 'ENV' )
        with open( envfn, 'w' ) as f:
            f.write( 'fsize={fsize}\n'.format( fsize=self.fsize ) )
            f.write( 'basename={basename}\n'.format( basename='log-' ) )

        l = log.Log( hostdir=self.hostdir2, fsize=self.fsize, create=False )
        self.false( l.inited )
        self.eq( None, l.err )
        self.eq( 0, len( l.files ) )

        self._test_uninitialized( l )
        rc, err = l.init()
        self._test_init_log( l )


    def test_init_no_create_valid_logs( self ):
        l = log.Log( hostdir=self.hostdir, fsize=self.fsize, create=False )
        self._test_init_log( l )


    def _test_uninitialized( self, l ):

        rst, err = l.write( '123' )
        self.eq( 'Uninitialized', err.err )

        rst, err = l.read( 0 )
        self.eq( 'Uninitialized', err.err )

        rst, err = l.read_last()
        self.eq( 'Uninitialized', err.err )

        rst, err = l.reset( 0 )
        self.eq( 'Uninitialized', err.err )


    def _test_init_log( self, l ):

        self.true( l.inited )
        self.eq( None, l.err )
        self.true( os.path.exists( l.hostdir ) )
        self.true( os.path.join( l.hostdir, 'ENV' ) )
        self.neq( 0, len( l.files ) )

        rst, err = l.write( '123' )
        self.eq( None, err )

        rst, err = l.read( 0 )
        self.eq( None, err )

        rst, err = l.read_last()
        self.eq( None, err )

        rst, err = l.reset( 0 )
        self.eq( None, err )

class TestRepair( Base ):

    def test_incomplete_file( self ):

        self.log.write( "1" )
        os.ftruncate( self.log.current_fd, 100 )

        log2 = log.Log( hostdir=self.log.hostdir )
        self.false( log2.inited )
        self.neq( None, log2.err )
        self.eq( 'IncompleteFile', log2.err.err )

    def test_checksum_err( self ):

        self.log.write( "1" )

        self._break_block( 0 )

        st = os.fstat( self.log.current_fd )
        size = st.st_size
        self.eq( 512, size )

        l = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.false( l.inited )
        self.eq( 'ChecksumError', l.err.err )

    def test_repair_checksum_err_1_block( self ):

        self.test_checksum_err()

        l = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.eq( 'ChecksumError', l.err.err )

        self._test_repair( l, 0 )

    def test_repair_checksum_err_2_block( self ):

        self.log.write( "1" )
        self.log.write( '2'*self.log.block_size )

        self._break_block( 0 )
        self._break_block( 1 )

        l = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.eq( 'ChecksumError', l.err.err )

        self._test_repair( l, 0 )


    def test_repair_checksum_err_1_correct_1_err( self ):

        ( s, e ), err = self.log.write( "1" )
        self.log.write( '2'*self.log.block_size )

        self._break_block( 1 )

        l = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.eq( 'ChecksumError', l.err.err )

        self._test_repair( l, e )


    def test_repair_incomplete_1_err( self ):

        self.log.write( "1" )
        os.ftruncate( self.log.current_fd, 100 )

        l = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.false( l.inited )
        self.eq( 'IncompleteFile', l.err.err )

        self._test_repair( l, 0 )


    def test_repair_incomplete_1_correct_1_err( self ):

        ( s, e ), err = self.log.write( "1" )
        self.log.write( '2'*self.log.block_size )
        os.ftruncate( self.log.current_fd, 512 + 100 )

        l = log.Log( hostdir=self.hostdir, fsize=self.fsize )
        self.false( l.inited )
        self.eq( 'IncompleteFile', l.err.err )

        self._test_repair( l, e )
        ( s, e ), err = self.log.write( '2' )
        self.eq( None, err )


    def _break_block( self, iblock ):

        pos = (iblock+1) * self.log.block_size

        os.lseek( self.log.current_fd, pos-1, os.SEEK_SET )
        byte = os.read( self.log.current_fd, 1 )
        byte = chr( int( byte.encode( 'hex' ), 16 ) + 1 )

        os.lseek( self.log.current_fd, pos-1, os.SEEK_SET )
        os.write( self.log.current_fd, byte )


    def _test_repair( self, l, valid_seq ):

        rc, err = l.repair()
        self.eq( None, err )
        self.true( l.inited )
        self.eq( None, l.err )

        self.eq( valid_seq, l.seq )

        ( s, e ), err = l.write( '123' )
        self.eq( None, err )

        ( s, e, data ), err = l.read( s )
        self.eq( None, err )
        self.eq( '123', data )

        rc, err = l.repair()

        self.neq( None, err )
        self.eq( 'NoNeedToRepair', err.err )


if __name__ == "__main__":

    # testframe.run_single( TestReset, 'test_reset_persistent' )
    # sys.exit()

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    classes = (
            TestReadWrite,
            TestReset,
            TestReadLast,
            TestReopen,
            TestCreate,
            TestRepair,
    )

    for clz in classes:
        suite.addTests( loader.loadTestsFromTestCase( clz ) )

    testframe.TestRunner(verbosity=2).run(suite)
