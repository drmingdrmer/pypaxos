#!/usr/bin/env python2.6
# coding: utf-8


import os, sys
import time
import shutil
import socket
import unittest
import urllib
import logging

import testframe
import paxos
import ftlog
import ftlogclient
from datatype import *

dd = testframe.dd

http = paxos.http
load = paxos.load
dump = paxos.dump
make_ts = paxos.make_ts

logger = paxos.logger
logger.setLevel( logging.INFO )
# logger.setLevel( logging.DEBUG )

class Base( testframe.TestFrame ):

    i = 0

    def _make_cluster( self, n, useletter=True ):

        if useletter:
            ks = 'abcdefghijklmnopqrstuvwxyz'
        else:
            ks = range( 0, 9 )

        self.cluster = {}
        for k in ks[ :n ]:
            Base.i += 1
            self.cluster[ k ] = { 'addrs':[ ( '127.0.0.1', 7801+Base.i ) ] }

    def setUp( self ):

        self._make_cluster( 3 )
        self.srvs = {}

        for ident in self.cluster:
            self.make_rlog_srv( ident )

        self.cli = ftlogclient.Client( self.cluster )
        time.sleep( 0.6 )

        self.leader = ( None, 0 )
        self._get_leader( notin=( None,  ))

        self.follower_ids = [ x for x in self.cluster
                              if x != self.leader ]


    def make_rlog_srv( self, ident, create=True, **argkv ):
        rlog = ftlog.ReliableLog( ident, self.cluster, logger=logger, **argkv )
        if create:
            rlog.init_paxos_sto()
            rlog.sto.set( 'sto_ready', True )

            rc, err = rlog.mylog.init( create=True )
            if err:
                raise Exception( 'rlog init failure: ' + repr( err ) )

        rc, err = rlog.start()
        if err:
            raise Exception( 'rlog startup failure: ' + repr( err ) )

        th = paxos.daemon_thread( rlog.serve_forever )
        self.srvs[ ident ] = { 'rlog': rlog,
                               'th': th, }

    def tearDown( self ):
        for ident in self.cluster:
            s = self.srvs[ ident ]
            s[ 'rlog' ].shutdown()
            s[ 'th' ].join()

            if os.path.isdir( s[ 'rlog' ].hostdir ):
                shutil.rmtree( s[ 'rlog' ].hostdir )

    def _write_log( self, ident, leader_ver, val ):

        rlog = self.srvs[ ident ][ 'rlog' ]

        rec = ftlog.LogRecord( 1, leader_ver, val )
        logbuf = dump( rec )

        return rlog.mylog.write( logbuf )


    def _test_read( self, seqvals, notin=() ):

        for seq, v in seqvals:
            for ident in self.cluster:

                if ident in notin:
                    continue

                _r, err = self.cli.read( seq, ident=ident )
                self.eq( None, err, 'read from ' + ident + ' seq=' + str(seq) )

                (commit_seq, ver, leader_ver, v_resp) = _r
                dd( "read from " + ident + " v=" + v_resp )
                self.eq( v, v_resp, 'read from ' + ident + ' seq=' + str(seq) )

    def _test_write( self, k='x', ident=None ):

        val = k + '-' + str(make_ts())
        ident = ident or self.leader

        resp, err = self.cli.write( val, ident=ident )

        self.eq( None, err )
        self.eq( 2, len( resp ) )
        seq, next_seq = resp

        dd( 'written seq, v=' + repr( (seq, val) ) )
        return seq, val

    def _shutdown_leader( self ):
        return self._shutdown( self.leader )

    def _shutdown( self, ident ):

        self.srvs[ ident ][ 'rlog' ].shutdown()
        self.srvs[ ident ][ 'th' ].join()
        dd( "shutted down:", ident )

        while True:
            resp, err = self.cli.iget( '/leader', ident=ident )
            if err and err.err == 'NetworkError':
                break
            time.sleep( 0.1 )

    def _get_leader( self, notin=None ):

        notin = notin or ( self.leader, None )

        ident = [ x for x in self.cluster.keys()
                  if x != self.leader ][ 0 ]

        while True:
            leader, err = self.cli.iget( '/leader', ident=ident )
            if leader and leader[ 0 ] not in notin:
                break
            time.sleep( 0.1 )

        if leader[ 0 ] is not None:
            # wait for leader itself to be aware of leadership
            while True:
                ll, err = self.cli.iget( '/leader', ident=leader[ 0 ] )
                if ll and ll[ 0 ] == leader[ 0 ]:
                    break
                time.sleep( 0.1 )

        self.leader = leader[ 0 ]
        self.full_leader = leader
        dd( 'got leader', leader, err )

class TestRW( Base ):

    def test_iget_leader( self ):
        resp, err = self.cli.iget( '/leader' )
        self.eq( None, err )
        self.true( resp[ 0 ] in self.cluster )
        self.true( resp[ 1 ] > 0 )


    def test_rw( self ):

        seq, val = self._test_write()
        time.sleep( 1 )

        for ident in self.cluster:

            r, err = self.cli.read( seq, ident=ident )
            dd( 'read r, err=' + repr( ( r, err ) ) )
            self.eq( None, err )
            commit_seq, ver, leader_ver, v = r

            self.eq( val, v, 'Get value from:' + ident
                     + ' Leader is ' + self.leader )


    def test_read_without_leader_constrain( self ):

        seq, val = self._test_write()
        time.sleep( 1 )

        for ident in self.cluster:

            (commit_seq, ver, leader_ver, v), err = self.cli.read( seq=0, ident=ident )

            dd( commit_seq, ver, leader_ver, v, err )
            self.eq( None, err )
            self.eq( val, v )

            self.eq( self.full_leader[1], leader_ver )
            self.eq( val, v )


    def test_read_with_leader_constrain( self ):

        seq, val = self._test_write()
        time.sleep( 1 )

        _, err = self.cli.read( seq=0, leader=self.full_leader, ident=self.leader )
        self.eq( None, err )

        for ident in self.follower_ids:
            _, err = self.cli.read( seq=0,
                                    leader=self.full_leader,
                                    ident=ident )
            dd( err )
            self.eq( None, err )

        for ident in self.follower_ids:
            _, err = self.cli.read( seq=0,
                                    leader=( ident, make_ts() ),
                                    ident=ident )
            dd( 'expected error:', err )
            self.neq( None, err )
            self.eq( 'IncorrectLeader', err.err )


    def test_leader_transition( self ):

        self._shutdown_leader()

        resp, err = self.cli.iget( '/leader', ident=self.leader )
        self.neq( None, err )
        self.eq( 'NetworkError', err.err )

        time.sleep( 3 )

        for ident in self.follower_ids:

            leader, err = self.cli.iget( '/leader', ident=ident )
            self.eq( None, err )
            leader = leader[ 0 ]
            self.neq( leader, self.leader )

    def test_work_with_2( self ):

        oldleader = self.leader

        self._shutdown_leader()
        self._get_leader()

        dd( "wait the only follower to update leader" )
        time.sleep( 1 )


        k, val = self._test_write()
        self._test_read( [ ( k, val ) ], notin=( oldleader, ) )

    def test_restore( self ):

        oldleader = self.leader

        self._shutdown_leader()
        self._get_leader()

        time.sleep( 0.6 )
        k, val = self._test_write()

        # restart old leader
        self.make_rlog_srv( oldleader )

        # catch up
        time.sleep( 1 )

        self._test_read( [ ( k, val ) ] )


    def test_node_down_during_request( self ):

        vals = []
        vals.append( self._test_write('x') )
        vals.append( self._test_write('y') )

        oldleader = self.leader

        self._shutdown_leader()
        self._get_leader()

        time.sleep( 0.6 )
        vals.append( self._test_write('z') )

        # restart old leader
        self.make_rlog_srv( oldleader )

        # catch up
        time.sleep( 1 )

        self._test_read( vals )

    def test_discard_non_accepted( self ):

        vals = []

        ident = self.follower_ids[ 0 ]

        vals.append( self._test_write() )

        # catchup discard lower leader_ver log
        self._write_log( ident, 100, 'abc' )

        # catchup discard higher leader_ver log if catchup from leader
        self._write_log( ident, make_ts()+1024000, 'abc' )

        vals.append( self._test_write() )
        vals.append( self._test_write() )

        dd( "wait for catchup" )
        time.sleep( 3 )

        self._test_read( vals )

class TestPersistent( Base ):

    def test_commit_seq_persistent( self ):

        vals = []
        vals.append( self._test_write() )

        time.sleep( 0.5 )
        commit_seq = self.srvs[ self.leader ][ 'rlog' ].commit_seq
        dd( commit_seq )

        old = self.leader
        self._shutdown_leader()
        self._shutdown( self.follower_ids[ 0 ] )
        self._shutdown( self.follower_ids[ 1 ] )

        self.make_rlog_srv( old )

        n = self.srvs[ old ][ 'rlog' ].commit_seq
        self.eq( commit_seq, n )


        # reset commit_seq to 0 if log broken
        self._shutdown( old )
        shutil.rmtree( self.srvs[ old ][ 'rlog' ].mylog.hostdir )
        self.make_rlog_srv( old )
        n = self.srvs[ old ][ 'rlog' ].commit_seq
        self.eq( 0, n )



class TestLeaderTransition( Base ):

    def test_refuse_p2_without_catchup( self ):

        f0 = self.follower_ids[ 0 ]
        ( seq, next_seq ), err = self._write_log( f0, 100, 'abc' )

        node = self.cluster[ f0 ]
        ip, port = node[ 'addrs' ][ 0 ]

        p2req = paxos.dump( { 'leader': self.full_leader,
                              'seq': next_seq,
                              'rec': ftlog.LogRecord( 1, self.full_leader[1], 'xyz' ) } )

        resp, err = paxos.http( ip, port, '/p2', body=p2req )

        self.neq( None, err )
        self.eq( 'InvalidPhase2Seq', err.err )
        self.eq( 0, err.val[ 0 ] )
        self.eq( next_seq, err.val[ 1 ] )


    def test_non_accepted_accumulate( self ):

        f0 = self.follower_ids[ 0 ]
        f1 = self.follower_ids[ 1 ]

        self._write_log( f0, 100, 'abc' )
        self._write_log( f0, 300, 'ABC' )
        self._write_log( f1, 200, 'xyz' )

        vals = [( 0, 'xyz' )]

        old = self.leader
        self._shutdown_leader()
        self._get_leader()

        self.make_rlog_srv( old )

        dd( "wait for catchup" )
        time.sleep( 2 )

        vals.append( self._test_write() )
        vals.append( self._test_write() )

        dd( "wait for catchup" )
        time.sleep( 3 )

        self._test_read( vals )


    def test_leader_with_greatest_log( self ):

        oldleader = self.leader
        followers = [ x for x in self.cluster.keys()
                      if x != self.leader ]

        vals = []

        vals.append( self._test_write('x') )
        self._shutdown( followers[ 0 ] )

        vals.append( self._test_write('y') )

        #    y  y
        # x  x  x
        # f0 f1 ldr

        self.srvs[ self.leader ][ 'rlog' ].pause_elect = True
        self.srvs[ self.leader ][ 'rlog' ].pause_serve_elect = True
        self.srvs[ self.leader ][ 'rlog' ].pause_log_rw = True

        # erase follower-1 logs
        self._shutdown( followers[ 1 ] )
        shutil.rmtree( self.srvs[ followers[ 1 ] ][ 'rlog' ].mylog.hostdir )

        #       y
        # x     x
        # f0 f1 ldr

        dd( "sleep to wait for leader to expire" )
        time.sleep( 3 )

        th0 = paxos.daemon_thread( self.make_rlog_srv, ( followers[ 0 ], ) )
        th1 = paxos.daemon_thread( self.make_rlog_srv,
                                   ( followers[ 1 ], ),
                                   { 'create':False, 'pause_elect':True } )


        dd( "sleep to wait for leader election" )
        time.sleep( 3 )

        _, err = self.cli.read( seq=0, ident=followers[ 1 ] )
        dd( err )
        self.neq( None, err )
        self.eq( 'ServerDisabled', err.err,
                 'without catchup with leader it does not work' )

        leader, err = self.cli.iget( '/leader', ident=followers[ 0 ] )
        dd( "After restart follower 0 and 1, leader=", leader, err )

        self.eq( None, leader[ 0 ],
                 'No catch up for broken log, no leader' )

        dd( "re-enable leader" )

        self.srvs[ self.leader ][ 'rlog' ].pause_elect = False
        self.srvs[ self.leader ][ 'rlog' ].pause_serve_elect = False
        self.srvs[ self.leader ][ 'rlog' ].pause_log_rw = False

        self._get_leader()

        # wait for catching up / re-exec
        time.sleep( 1 )

        dd( 'leader', self.leader )
        dd( 'follower', (followers, oldleader) )

        self._test_read( vals )




if __name__ == "__main__":

    # testframe.run_single( TestRW, 'test_rw' )
    # testframe.run_single( TestRW, 'test_work_with_2' )
    # testframe.run_single( TestRW, 'test_read_with_leader_constrain' )
    # testframe.run_single( TestRW, 'test_leader_with_greatest_log' )
    # testframe.run_single( TestRW, 'test_discard_non_accepted' )
    # testframe.run_single( TestLeaderTransition, 'test_refuse_p2_without_catchup' )
    testframe.run_single( TestPersistent, 'test_commit_seq_persistent' )
    sys.exit()

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    classes = (
            TestRW,
            TestLeaderTransition,
            TestPersistent,
    )

    for clz in classes:
        suite.addTests( loader.loadTestsFromTestCase( clz ) )

    testframe.TestRunner(verbosity=2).run(suite)
