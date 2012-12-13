#!/usr/bin/env python2.6
# coding: utf-8

import os, sys
import binascii
import struct
import logging
import zlib
from collections import namedtuple

DEFAULT_FORMAT = "tid=%(thread)d, %(lineno)d, %(levelname)-6s %(funcName)-25s   %(message)s"
logger = logging.getLogger( 'log' )

# logger.setLevel( logging.DEBUG )
logger.setLevel( logging.INFO )
# logger.setLevel( logging.CRITICAL )

hdr = logging.StreamHandler( sys.stdout )
hdr.setFormatter( logging.Formatter( DEFAULT_FORMAT ) )
logger.addHandler( hdr )


ERR = dict( [ ( x, x ) for x in (
        'OutofRange',
        'TooLarge',
        'TooLargeRecord',
        'ExpiredSeq',
        'InvalidSeq',
        'InvalidBlockSeq',
        'Corrupted',
        'IncompleteFile',
        'IncompleteBlock',
        'ChecksumError',
        'Uninitialized',
        'NoNeedToRepair',
        'Unrepairable',
) ] )

Error = namedtuple( 'Error', 'err,val' )


def Err( err, *vals ):
    return Error( ERR[ err ], vals )

class Log( object ):

    '''
        {
            rec_len(4)
            data
        }*
        block_first_seq(8) # seq of the first record from this block
        length(4)
        checksum(4)
    '''

    max_record = 0xffffffff

    block_size = 512

    block_first_seq_size = 8
    length_size = 4
    checksum_size = 4
    tail_size = block_first_seq_size + length_size + checksum_size
    body_size = block_size - tail_size

    fn_format = '%010d'

    rec_len_size = 4


    def __init__( self, hostdir, fsize=1024*1024*32, basename='log-',
                  create=True ):

        self.hostdir = hostdir
        self.fsize = fsize - fsize % self.block_size
        self.basename = basename

        self.envfn = os.path.join( self.hostdir, 'ENV' )

        # All existent log files. Only the last one is opened for write.
        self.files = []

        # File index for write
        self.i = None
        # File name for write
        self.curfn = None
        self.current_fd = None

        # global position for next record
        self.seq = None

        # Where to start for next write.
        # Every time a block successfully written write_seq is updated.
        #
        # Whilst seq is updated only when a whole record is written.
        self.write_seq = None

        # Current block buf for write
        self.buf = ''

        # the seq of the first record in current block
        self.block_first_seq = None

        # the seq of the first record in next block
        self.next_block_first_seq = None

        self.reader_fds = {}

        self.inited = False
        self.err = None

        rc, err = self.init( create=create )
        if err:
            self.err = err


    def init( self, create=True ):

        self.err = None

        if not os.path.isdir( self.hostdir ):
            if create:
                os.makedirs( self.hostdir, mode=0755 )
            else:
                return None, None

        if os.path.exists( self.envfn ):
            self.read_env()
        else:
            if create:
                self.write_env()
            else:
                return None, None

        rc, err = self.load_fns()
        if err:
            return None, err

        if self.files == [] and not create:
            return None, None

        rc, err = self.init_fns()
        if err:
            return None, err

        self.inited = True

        return None, None

    def read_env( self ):

        with open( self.envfn ) as f:

            lines = f.read()
            lines = lines.strip().split( '\n' )

            lines = [ x.strip().split( '=', 1 )
                      for x in lines ]

            lines = [ ( k.strip(), v.strip() )
                      for k, v in lines ]

            dic = dict( lines )

            if 'fsize' in dic:
                self.fsize = int( dic[ 'fsize' ] )

            if 'basename' in dic:
                self.basename = dic[ 'basename' ]

    def write_env( self ):
        with open( self.envfn, 'w' ) as f:
            f.write( 'fsize={fsize}\n'.format( fsize=self.fsize ) )
            f.write( 'basename={basename}\n'.format( basename=self.basename ) )

    def load_fns( self ):

        fns = os.listdir( self.hostdir )

        fns = [ x for x in fns
                if x.startswith( self.basename ) ]

        fns.sort()
        self.files = fns

        return None, None


    def init_fns( self ):
        rc, err = self.check_files_aligned()
        if err:
            return None, err

        rc, err = self.load_last_block()
        if err:
            return None, err

        self.write_seq = self.seq - self.seq % self.block_size
        self.next_block_first_seq = None

        self.open_next_file_at_seq( self.write_seq )

        return None, None


    def load_last_block( self ):

        if len( self.files ) == 0:
            self.seq = 0
            self.buf = ""
            self.block_first_seq = 0

        else:
            last_fn = self.files[ -1 ]
            i = self.fn_to_i( last_fn )

            with self.open_f_ro( last_fn ) as f:
                st = os.fstat( f.fileno() )
                size = st.st_size

            if size == 0:
                self.seq = 0
                self.buf = ""
                self.block_first_seq = 0
                return None, None

            # not determined, init it for take a valid block read
            self.seq = ( i-1 ) * self.fsize + size

            if self.seq % self.block_size != 0:
                return None, Err( 'IncompleteBlock', size )

            (block_first_seq, buf), err = self.read_block( self.seq - self.block_size )
            if err:
                return None, err

            self.seq = ( i-1 ) * self.fsize + size - self.block_size + len( buf )
            self.buf = buf
            self.block_first_seq = block_first_seq

        return None, None


    def check_files_aligned( self ):

        for fn in self.files:

            with self.open_f_ro( fn ) as f:

                st = os.fstat( f.fileno() )
                size = st.st_size
                if size % self.block_size != 0:
                    return None, Err( 'IncompleteFile', fn, size )

        return None, None


    def repair( self ):


        if self.inited or self.err is None:
            return None, Err( 'NoNeedToRepair' )

        if self.err.err not in ( 'IncompleteBlock', 'IncompleteFile', 'ChecksumError' ):
            return None, Err( 'Unrepairable' )

        fn = self.files[ -1 ]

        fd = self.open_fd( fn )

        st = os.fstat( fd )
        size = st.st_size

        if size % self.block_size != 0:
            size -= size % self.block_size
            os.ftruncate( fd, size )
            logger.warn( 'Discarded last incomplete block at '
                         + repr( ( fn, size ) ) )

        i = self.fn_to_i( fn )
        seq = ( i-1 ) * self.fsize + size

        os.close( fd )

        print 'closed'

        if seq == 0:
            for fn in self.files:
                os.unlink( self.fn_path( fn ) )
            for fd in self.reader_fds.values():
                os.close( fd )
            return self.init( create=True )

        while True:
            if seq == 0:
                for fn in self.files:
                    os.unlink( self.fn_path( fn ) )
                for fd in self.reader_fds.values():
                    os.close( fd )
                return self.init( create=True )

            valid_seq = seq

            (block_first_seq, buf, chksum), err = self.read_block_nocheck(
                    seq - self.block_size )

            if err:
                return None, err

            seq -= self.block_size

            if self.checksum( buf ) == chksum:
                break


        bufs = {}
        seq = block_first_seq
        self.seq = valid_seq


        while True:
            next_seq, err = self.skip_rec( bufs, seq )
            print next_seq, err
            if err:
                return None, err

            if next_seq >= valid_seq:
                break

            seq = next_seq

        self.seq = seq
        self.buf = buf[ :( seq % self.block_size ) ]
        self.block_first_seq = block_first_seq

        self.write_seq = seq - seq % self.block_size
        self.next_block_first_seq = None

        si, offset = self.seq_to_ioff( seq )

        self.open_next_file_at_seq( seq )
        os.ftruncate( self.current_fd, offset )
        self.write_buffered_block( sync=True )

        logger.warn( 'log discarded after: ' + repr( seq ) )

        return self.init( create=False )


    def reset( self, seq ):

        if not self.inited:
            return None, Err( 'Uninitialized' )

        logger.info( "reset to " + repr( seq ) )

        # make sure it is a valid seq
        (_, next_seq, data), err = self.read( seq )
        if err:
            return None, err

        block_seq = self.align( seq )
        (block_first_seq, buf), err = self.read_block( block_seq )
        if err:
            return None, err

        self.seq = seq
        self.buf = buf[ :seq - block_seq ]

        self.block_first_seq = block_first_seq
        self.write_seq = block_seq
        self.next_block_first_seq = None

        si, offset = self.seq_to_ioff( self.seq )

        self.open_next_file_at_seq( self.seq )
        os.ftruncate( self.current_fd, offset )
        logger.debug( "block_first_seq=" + repr( self.block_first_seq ) )
        self.write_buffered_block( sync=True )

        return self.seq, None


    def write( self, data ):

        if not self.inited:
            return None, Err( 'Uninitialized' )
        # data = zlib.compress( data )

        none = ( None, None )
        cur = self.seq

        i, offset = self.seq_to_ioff( self.seq )

        assert i == self.i
        assert type( data ) == type( '' )

        l = len( data )
        if l > self.max_record:
            return none, Err( 'TooLargeRecord', self.max_record, len( data ) )

        total = self.rec_len_size + l
        offset_in_block = offset % self.block_size

        t = total + offset_in_block
        logger.debug( 'total from block start: ' + str( t ) )

        next_seq = self.write_seq \
                + self.block_size * (t / self.body_size) \
                + t % self.body_size

        logger.debug( 'next seq: ' + repr( next_seq ) )

        self.next_block_first_seq = next_seq
        logger.debug( 'next first seq: ' + repr( self.next_block_first_seq ) )


        stream = struct.pack( ">I", l )

        self.write_stream( stream, sync=False )
        self.write_stream( data, sync=True )

        self.seq = next_seq

        return (cur, self.seq), None


    def write_stream( self, stream, sync=False ):

        l = len( stream )

        if len( self.buf ) + l < self.body_size:
            self.buf += stream
            self.write_buffered_block( sync )
            return

        i , l = 0, len( stream )
        while i < l:
            lfree = self.body_size - len( self.buf )
            self.buf += stream[ i:i+lfree ]
            i += lfree

            is_full_write = len( self.buf ) == self.body_size

            self.write_buffered_block( i == l and sync )

            if is_full_write:
                self.new_block()


    def write_buffered_block( self, sync ):

        lbuf = len( self.buf )
        assert lbuf <= self.body_size

        logger.debug( "buf len={l}".format( l=lbuf ) )

        chksum = self.checksum( self.buf )

        padding = '\0' * ( self.body_size - lbuf )
        tail = struct.pack( '>QII', self.block_first_seq, lbuf, chksum )

        i, offset = self.seq_to_ioff( self.write_seq )

        os.lseek( self.current_fd, offset, os.SEEK_SET )
        n = os.write( self.current_fd, self.buf + padding + tail )

        assert n == self.block_size

        if sync:
            os.fsync( self.current_fd )


    def new_block( self ):

        self.buf = ''
        self.write_seq += self.block_size

        if self.block_first_seq < self.write_seq:
            self.block_first_seq = self.next_block_first_seq

        i, offset = self.seq_to_ioff( self.write_seq )

        if i > self.i:
            self.open_next_file_at_seq( self.write_seq )


    def read_last( self ):

        if not self.inited:
            return None, Err( 'Uninitialized' )

        none = ( None, None, None )

        seq = self.align( self.seq )
        bufs = {}

        # first, find backwards
        while seq >= 0:
            (rec_seq, buf), err = self.bufferred_read_block( bufs, seq )
            if err:
                return none, err
            if rec_seq == self.seq:
                seq -= self.block_size
            else:

                # then find forwards
                next_seq = rec_seq
                while next_seq < self.seq:
                    seq = next_seq
                    next_seq, err = self.skip_rec( bufs, seq )
                    if err:
                        return none, err
                return self.read( seq )

        return none, Err( 'OutofRange', 0, 0 )


    def read( self, rseq, readnext=False ):

        if not self.inited:
            return None, Err( 'Uninitialized' )

        none = ( None, None, None )

        logger.debug( 'read request from ' + str( rseq )
                      + ' our seq: ' + repr( self.seq ) )

        if rseq >= self.seq or rseq < 0:
            return none, Err( 'OutofRange', self.seq, rseq )

        bufs = {}
        (rec_seq, buf), err = self.bufferred_read_block( bufs,
                                                         self.align( rseq ) )
        if err:
            return none, err

        if not self.in_one_block( rec_seq, rseq ):
            ( rec_seq, buf ), err = self.bufferred_read_block( bufs,
                                                      self.align( rec_seq ) )
            if err:
                return none, err

        logger.debug( "Search record from seq=" + repr( rec_seq ) )
        while rec_seq < rseq:
            rec_seq, err = self.skip_rec( bufs, rec_seq )
            if err:
                return none, err

            logger.debug( "Skip record, next seq: " + repr( rec_seq ) )

        if rec_seq > rseq and not readnext:
            return none, Err( 'InvalidSeq', rseq )

        ( next_seq, data ), err = self.read_rec( bufs, rec_seq )
        if err:
            return none, err

        # data = zlib.decompress( data )
        return (rec_seq, next_seq, data), None


    def skip_rec( self, bufs, rseq ):
        l, err = self.load_rec_len( bufs, rseq )
        if err:
            logger.info( repr(err) )
            return None, err

        seq = self.seq_seek( rseq, self.rec_len_size + l )
        return seq, None


    def read_rec( self, bufs, rseq ):

        none = ( None, None )
        logger.debug( "Read record from: " + repr( rseq ) )

        l, err = self.load_rec_len( bufs, rseq )
        if err:
            return none, err

        data_seq = self.seq_seek( rseq, self.rec_len_size )

        block_seq = self.align( data_seq )
        (_, buf), err = self.bufferred_read_block( bufs, block_seq )
        if err:
            return none, err

        buf = buf[ data_seq - block_seq: ]
        logger.debug( "record first segment buf:" + repr( buf ) )

        while len( buf ) < l:
            block_seq += self.block_size
            (_, _buf), err = self.bufferred_read_block( bufs, block_seq )
            if err:
                return none, err
            buf += _buf

        end_seq = self.seq_seek( data_seq, l )
        return (end_seq, buf[ :l ]), None


    def load_rec_len( self, bufs, rseq ):

        block_seq = self.align( rseq )
        (first_seq, buf), err = self.bufferred_read_block( bufs, block_seq )
        if err:
            logger.info( repr( err ) )
            return None, err

        buf = buf[ rseq - block_seq: ]
        if len( buf ) < self.rec_len_size:
            block_seq += self.block_size
            ( _, _buf ), err = self.bufferred_read_block( bufs, block_seq )
            if err:
                logger.info( repr( err ) )
                return None, err
            buf += _buf


            if len( buf ) < self.rec_len_size:
                raise # TODO

        l = self.buf_get_record_len( buf, 0 )
        return l, None


    def buf_get_record_len( self, buf, offset ):

        logger.debug( 'buf=' + repr( buf ) )
        l, = struct.unpack( '>I', buf[ offset : offset + self.rec_len_size ] )
        logger.debug( 'record len: ' + repr( l ) )

        return l


    def bufferred_read_block( self, bufs, block_seq ):

        none = ( None, None )

        if block_seq not in bufs:
            data, err = self.read_block( block_seq )
            if err:
                return none, err
            bufs[ block_seq ] = data

        return bufs[ block_seq ], None


    def read_block( self, rseq ):

        none = ( None, None )

        if rseq % self.block_size != 0:
            return none, Err( 'InvalidBlockSeq', rseq )

        if rseq >= self.seq:
            return none, Err( 'OutofRange', self.seq, rseq )

        (block_first_seq, buf, chksum), err =  self.read_block_nocheck( rseq )
        if err:
            return none, err

        if self.checksum( buf ) != chksum:
            return none, Err( 'ChecksumError', rseq )

        return ( block_first_seq, buf ), None

    def read_block_nocheck( self, rseq ):

        none = ( None, None )
        logger.debug( 'read from: ' + repr( rseq ) )

        i, offset = self.seq_to_ioff( rseq )

        if i not in self.reader_fds:
            self.reader_fds[ i ] = self.open_fd_ro( self.i_fn( i ) )

        fd = self.reader_fds[ i ]

        logger.debug( "read from file {i} offset={offset}".format(
                i=i, offset=offset ) )

        os.lseek( fd, offset, os.SEEK_SET )
        buf = os.read( fd, self.block_size )

        if len( buf ) != self.block_size:
            return none, Err( 'IncompleteBlock', rseq )

        block_first_seq, length, chksum = struct.unpack( '>QII', buf[ -self.tail_size: ] )
        buf = buf[ :length ]

        return (block_first_seq, buf, chksum), None



    def checksum( self, data ):
        c = binascii.crc32( data ) & 0xffffffff
        return c


    def open_next_file_at_seq( self, seq ):

        seq_i, offset = self.seq_to_ioff( seq )

        self.i = seq_i

        self.curfn = self.i_fn( self.i )

        for i, fd in self.reader_fds.items():
            if i > self.i:
                logger.info( "close reader fd: " + repr( fd ) )
                os.close( fd )
                os.unlink( self.i_path( i ) )
                self.files.remove( self.i_fn( i ) )

        if len(self.files) == 0 or self.files[ -1 ] != self.curfn:
            self.files.append( self.curfn )

        if self.current_fd is not None:
            os.fsync( self.current_fd )
            os.close( self.current_fd )
            self.current_fd = None

        self.current_fd = self.open_fd( self.curfn )
        logger.info( "open next file: " + repr( self.curfn ) )

    def open_f_ro( self, fn ):
        return open( self.fn_path( fn ) )


    def open_fd( self, fn ):
        return os.open( self.fn_path( fn ),
                        os.O_RDWR | os.O_CREAT,
                        0644 )

    def open_fd_ro( self, fn ):
        return os.open( self.fn_path( fn ),
                        os.O_RDONLY )


    def i_path( self, i ):
        fn = self.i_fn( i )
        return self.fn_path( fn )


    def fn_path( self, fn ):
        assert '/' not in fn
        return os.path.join( self.hostdir, fn )


    def seq_to_ioff( self, seq ):
        return ( int(seq) / self.fsize + 1, seq % self.fsize )


    def i_fn( self, i ):
        return self.basename + ( self.fn_format % ( i, ) )


    def fn_to_i( self, fn ):
        i = fn[ len( self.basename ): ]
        return int( i )


    def in_one_block( self, seq1, seq2 ):
        return self.align( seq1 ) == self.align( seq2 )


    def align( self, seq ):
        return seq - seq % self.block_size


    def seq_seek( self, start, offset ):

        block_seq = self.align( start )
        size = start - block_seq + offset
        size = size / self.body_size * self.block_size \
                + size % self.body_size

        logger.debug( "Seek to " + repr( block_seq + size ) )
        return block_seq + size

