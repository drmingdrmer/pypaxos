#!/usr/bin/env python2.6
# coding: utf-8


import os, sys
import time
import socket
import unittest

import testframe
import paxos

dd = testframe.dd

i = 0
ts_i = 0

def ts():
    global ts_i
    ts_i += 1
    return int( time.time()*1000 + time.timezone * 1000 ) + ts_i

class TestRndNumber( testframe.TestFrame ):

    def test_rnd_manager( self ):

        rm = paxos.RndManager()
        self.eq( ( 1, 2 ), rm.make_rnd( 1, 2 ), 'as tuple' )

        a = rm.make_rnd( 1, 2 )
        a1 = rm.make_rnd( 1, 2 )
        b = rm.make_rnd( 1, 3 )

        self.true( rm.eq( a, a1 ), 'equal' )
        self.true( rm.lt( a, b ), 'less than' )
        self.true( rm.gt( b, a ), 'greater than' )

        self.true( rm.lteq( a, b ), 'less than or equal' )
        self.true( rm.lteq( a, a1 ), 'less than or equal' )
        self.true( rm.gteq( b, a ), 'greater than or equal' )
        self.true( rm.gteq( a, a1 ), 'greater than or equal' )

Rnd3 = paxos.struct(
        'Rnd3',
        ( 'lsn', None ),
        ( 'ts', None ),
        ( 'prp_id', None ), )
Inst3 = paxos.struct(
        'Inst3',
        ( 'v', paxos.Value ),
        ( 'vrnd', Rnd3 ),
        ( 'last_rnd', Rnd3 ),
        ( 'expire', None ), )
Rec3 = paxos.struct(
        'Rec3',
        ( 'committed', paxos.ValueVersion ),
        ( 'instance', Inst3 ), )

class RM3( paxos.RndManager ):
    rec_clz = Rec3
    inst_clz = Inst3
    rnd_clz = Rnd3
    def next_rnd( self, lsn, ident ):
        return self.make_rnd( lsn, paxos.make_ts(), hash( ident ) % 1000 )

    def zero_rnd( self ):
        return self.make_rnd( 0, 0, 0 )

class TestRndManager( testframe.TestFrame ):

    def setUp( self ):
        self.mgr = RM3()

    def test_rnd_generated_by_millisecond( self ):
        rnd = self.mgr.next_rnd( 1, 'a' )
        self.eq( 1, rnd.lsn )
        self.eq( 0, (rnd.ts - paxos.make_ts())/100 )

    def test_zeor_rnd( self ):
        self.eq( ( 0, 0, 0 ), self.mgr.zero_rnd() )

    def test_zeor_inst( self ):
        i = self.mgr.zero_inst()
        self.eq( None, i.v )
        self.eq( self.mgr.zero_rnd(), i.vrnd )
        self.eq( self.mgr.zero_rnd(), i.last_rnd )
        self.eq( 0, i.expire )

    def test_zero_rec( self ):
        r = self.mgr.zero_rec()
        self.eq( ( None, 0 ), r.committed )


class Base( testframe.TestFrame ):

    def _make_cluster( self, n, useletter=True ):

        global i
        if useletter:
            ks = 'abcdefghijklmnopqrstuvwxyz'
        else:
            ks = range( 0, 9 )

        self.cluster = {}
        for k in ks[ :n ]:
            i += 1
            self.cluster[ k ] = { 'addrs':[ ( '127.0.0.1', 7801+i ) ] }


class TestFilePaxosStorage( testframe.TestFrame ):

    fn = 'tmp-storage'
    sto_class = paxos.FilePaxosStorage

    def _rnd( self ):
        try:
            rnd = self.sto_class.rndmgr.next_rnd('a')
        except Exception, e:
            rnd = self.sto_class.rndmgr.next_rnd( 10, 'a' )
        return rnd

    def _low_rnd( self ):
        rnd = self._rnd()
        rnd._replace( ts=rnd.ts-1 )
        return rnd

    def _hi_rnd( self ):
        rnd = self._rnd()
        rnd._replace( ts=rnd.ts+1 )
        return rnd

    def setUp( self ):
        if os.path.exists( self.fn ):
            os.unlink( self.fn )

        self.instid_commit = paxos.InstanceId( 'a', 1 )
        self.vcommit = paxos.Value( 'a', 'val', 0 )
        self.sto = self.sto_class( self.fn )

    def tearDown( self ):
        if os.path.exists( self.fn ):
            os.unlink( self.fn )

    def test_inited( self ):

        self.true( True is self.sto.corrupted )

        self.sto.init()

        dd( repr( self.sto.corrupted ) )
        self.true( False is self.sto.corrupted )

    def test_default_record( self ):

        d = self.sto.rndmgr.zero_rec()
        d = d.instance

        self.eq( None, d.v )
        self.eq( self.sto.rndmgr.zero_rnd(), d.vrnd )
        self.eq( 0, d.vrnd.ts )
        self.eq( 0, d.vrnd.prp_id )
        self.eq( self.sto.rndmgr.zero_rnd(), d.last_rnd )
        self.eq( 0, d.last_rnd.ts )
        self.eq( 0, d.last_rnd.prp_id )

    def test_get_inst_err( self ):

        instid = paxos.InstanceId( 'a', 10 )
        self._test_get_inst_err( instid )

        instid = paxos.InstanceId( 'a', 0 )
        self._test_get_inst_err( instid )

    def test_get_inst_ok( self ):
        instid = paxos.InstanceId( 'a', 1 )

        self._test_get_inst_ok( instid )

    def test_set_inst_inmem_ok( self ):
        instid = paxos.InstanceId( 'a', 1 )
        inst, err = self.sto.get_inst( instid )

        inst = self.sto.rndmgr.make_inst( paxos.Value( 1, { 1:1 }, 0 ),
                                          self._low_rnd(),
                                          self._hi_rnd(),
                                          0 )

        rst, err = self.sto.set_inst( instid, inst )
        self.eq( None, err )

        inst2, err = self.sto.get_inst( instid )
        self.eq( None, err )
        dd( 'inst to be set: ' + repr( inst ) )
        dd( 'inst retreived: ' + repr( inst2 ) )

        self.eq( inst2, inst, 'v retreived after set/get' )

    def test_set_inst_err( self ):

        instid = paxos.InstanceId( 'a', 1 )
        inst, err = self.sto.get_inst( instid )

        instid = instid._replace( ver=10 )
        rst, err = self.sto.set_inst( instid, inst )
        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )

        instid = instid._replace( ver=0 )
        rst, err = self.sto.set_inst( instid, inst )
        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )

    def test_set_inst_persistent( self ):
        inst = self.sto.rndmgr.make_inst( paxos.Value( 1, 2, 0 ),
                                          self._low_rnd(),
                                          self._hi_rnd(),
                                          0 )
        instid = paxos.InstanceId( 'a', 1 )
        rst, err = self.sto.set_inst( instid, inst, sync=True )

        sto = self.sto_class( self.fn )

        inst2, err = sto.get_inst( instid )
        self.eq( inst, inst2, 'equal with what is retreived from disk' )

        with open( self.fn ) as f:
            cont = f.read()
            dd( 'content saved on dist: ' + repr(cont) )


    def test_inst_lease( self ):
        inst = self.sto.rndmgr.inst_clz( v=paxos.Value( 1, 2, 100 ),
                                         vrnd=self.sto.rndmgr.rnd_clz( 3, 4, 5 ),
                                         last_rnd=self.sto.rndmgr.rnd_clz( 5, 6, 7 ),
                                         expire=0 )
        instid = paxos.InstanceId( 'a', 1 )
        rst, err = self.sto.set_inst( instid, inst )
        self.eq( None, err )
        self.neq( None, rst )

        rst, err = self.sto.get_inst( instid )
        self.eq( None, err )
        self.neq( None, rst, 'Get not expired inst' )
        self.eq( inst.v, rst.v )

        time.sleep( 0.2 )

        rst, err = self.sto.get_inst( instid )
        self.eq( None, err )
        self.neq( None, rst, 'Get expired inst' )
        self.eq( None, rst.v )
        self.eq( self.sto.rndmgr.zero_rnd(), rst.vrnd )
        self.eq( inst.last_rnd, rst.last_rnd )

    def test_commit_invalid_instid( self ):

        v = paxos.Value( 'a', 'val', 0 )

        instid = paxos.InstanceId( 'a', 0 )
        rc, err = self.sto.commit( instid, v )

        self.neq( None, err )
        self.eq( paxos.ERR['InvalidInstanceId'], err.err )


    def test_commit_ok( self ):
        self._commit_v()


    def test_commit_skip_version( self ):

        self._commit_v()

        v = paxos.Value( 'b', '1', 0 )
        instid = paxos.InstanceId( 'a', 10 )
        rc, err = self.sto.commit( instid, v )

        self.eq( None, err )
        self.true( rc )

        v = paxos.Value( 'c', '2', 0 )
        instid = paxos.InstanceId( 'a', 9 )
        rc, err = self.sto.commit( instid, v )

        self.neq( None, err )
        self.eq( paxos.ERR['InvalidInstanceId'], err.err )
        self.eq( ( 'a', 11 ), err.val[ 0 ] )
        self.eq( ( 'a', 9 ), err.val[ 1 ] )


    def test_commit_dup( self ):
        self._commit_v()
        self._commit_v()


    def test_commit_2version( self ):

        self._commit_v()

        v = paxos.Value( 'a', 'val2', 0 )
        instid = paxos.InstanceId( 'a', 2 )
        rc, err = self.sto.commit( instid, v )
        self.eq( None, err )
        self.true( rc )

    def test_inst_after_commit( self ):

        self._commit_v()
        next_instid = paxos.InstanceId( self.instid_commit.key, self.instid_commit.ver+1 )
        inst, err = self.sto.get_inst( self.instid_commit )
        self.neq( None, err )
        self.eq( None, inst )
        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )
        self.eq( next_instid, err.val[ 0 ] )

        inst, err = self.sto.get_inst( next_instid )
        self.eq( None, err )
        self.eq( None, inst.v )
        self.eq( self.sto.rndmgr.zero_rnd(), inst.vrnd )
        self.eq( self.sto.rndmgr.zero_rnd(), inst.last_rnd )


    def test_commit_persistent( self ):

        self._commit_v()

        v2 = paxos.Value( 'b', 'xxx', 0 )
        sto = self.sto_class( self.fn )

        # being able to commit verion=2 means it is persistently stored
        instid = paxos.InstanceId( 'a', 2 )
        rc, err = sto.commit( instid, v2 )
        self.eq( None, err )
        self.true( rc )


    def test_get_committed_empty( self ):
        vver, err = self.sto.get_committed( ('a', None) )
        self.eq( None, err )
        self.eq( (None, 0), vver )

        vver, err = self.sto.get_committed( ('a', 0) )
        self.eq( None, err )
        self.eq( (None, 0), vver )


    def test_get_committed_empty_invalid_ver( self ):
        vver, err = self.sto.get_committed( ('a', 1) )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )
        self.eq( None, vver )


    def test_get_committed_ver1( self ):

        self._commit_v()

        vver, err = self.sto.get_committed( ('a',0 ) )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )
        self.eq( None, vver )

        vver, err = self.sto.get_committed( ('a', 2) )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )
        self.eq( None, vver )

        (v, ver), err = self.sto.get_committed( ('a', 1) )
        self.eq( None, err )
        self.eq( self.vcommit, v )
        self.eq( 1, ver )

        (v, ver), err = self.sto.get_committed( ('a', None) )
        self.eq( None, err )
        self.eq( self.vcommit, v )
        self.eq( 1, ver )

    def test_getter_setter( self ):

        v, err = self.sto.get( 'k' )
        self.eq( None, v )
        self.eq( None, err )

        rc, err = self.sto.set( 'k', 1 )
        self.eq( None, rc )
        self.eq( None, err )

        v, err = self.sto.get( 'k' )
        self.eq( 1, v )
        self.eq( None, err )


    def _test_get_inst_err( self, instid ):
        inst, err = self.sto.get_inst( instid )

        self.eq( None, inst, 'Invalid version' )
        self.eq ( 'InvalidInstanceId', err.err )
        self.eq( instid, err.val[ 1 ] )


    def _test_get_inst_ok( self, instid ):
        inst, err = self.sto.get_inst( instid )

        self.eq( None, err, 'Valid version' )
        self.eq ( None, inst.v )
        self.eq ( self.sto.rndmgr.zero_rnd(), inst.vrnd )
        self.eq ( self.sto.rndmgr.zero_rnd(), inst.last_rnd )

    def _commit_v( self ):
        rc, err = self.sto.commit( self.instid_commit, self.vcommit )
        self.eq( None, err )
        self.true( rc )



class FilePaxosStorage3( paxos.FilePaxosStorage ):
    rndmgr = RM3()

class TestFilePaxosStorage_Rnd3( TestFilePaxosStorage ):
    sto_class = FilePaxosStorage3

class TestAcceptorWithCluster( Base ):

    def setUp( self ):
        global i

        self._make_cluster( 3 )

        i += 1
        self.cluster_wrong = {
                'a': { 'addrs': [ ( '127.0.0.1', 8601 + i ) ], },
        }

        self.srvs = {}

        ident = 'a'
        self.fn = 'tmp-' + str( ident )
        sto = paxos.FilePaxosStorage( self.fn )
        sto.init()
        self.acc = paxos.Acceptor( ident, self.cluster, sto )

        ts = paxos.make_ts()
        self.p1req = { 'cmd': 'phase1a',
                       'instid': [ 'a', 1 ],
                       'rnd': ( ts, 100),
        }
        self.p2req = { 'cmd': 'phase2a',
                       'instid': [ 'a', 1 ],
                       'rnd': ( ts, 100),
                       'v' : ( 'a', 'val', 0 ),
        }
        self.p3req = { 'cmd': 'phase3a',
                       'instid': [ 'a', 1 ],
                       'v' : ( 'a', 'val', 0 ),
        }

    def tearDown( self ):
        if os.path.exists( self.fn ):
            os.unlink( self.fn )


    def test_unpresent_cluster( self ):

        resp, err = self.acc.phase1a( self.p1req )
        self.eq( None, err, "No cluster is ok for phase1" )

        resp, err = self.acc.phase2a( self.p2req )
        self.eq( None, err, "No cluster is ok for phase2" )

        resp, err = self.acc.phase3a( self.p3req )
        self.eq( None, err, "No cluster is ok for phase3" )


    def test_unmatched_cluster( self ):

        self.acc.cluster_hashes = lambda *x: ( paxos._cluster_hash( self.cluster_wrong ), )

        ch = paxos._cluster_hash( self.cluster )

        self.p1req.setdefault( 'cluster_hash', ch )
        self.p2req.setdefault( 'cluster_hash', ch )
        self.p3req.setdefault( 'cluster_hash', ch )

        resp, err = self.acc.phase1a( self.p1req )
        self.neq( None, err, "err for phase1" )
        self.eq( paxos.ERR[ 'InvalidCluster' ], err.err )

        resp, err = self.acc.phase2a( self.p2req )
        self.neq( None, err, "err for phase2" )
        self.eq( paxos.ERR[ 'InvalidCluster' ], err.err )

        resp, err = self.acc.phase3a( self.p3req )
        self.neq( None, err, "err for phase3" )
        self.eq( paxos.ERR[ 'InvalidCluster' ], err.err )

    def test_matched_cluster( self ):

        self.acc.cluster_hashes = lambda *x: ( paxos._cluster_hash( self.cluster ), )

        ch = paxos._cluster_hash( self.cluster )

        self.p1req.setdefault( 'cluster_hash', ch )
        self.p2req.setdefault( 'cluster_hash', ch )
        self.p3req.setdefault( 'cluster_hash', ch )

        resp, err = self.acc.phase1a( self.p1req )
        self.eq( None, err, "matched cluster for phase1" )

        resp, err = self.acc.phase2a( self.p2req )
        self.eq( None, err, "matched cluster for phase2" )

        resp, err = self.acc.phase3a( self.p3req )
        self.eq( None, err, "matched cluster for phase3" )


class TestAcceptor( testframe.TestFrame ):

    fn = 'tmp-storage'

    def setUp( self ):
        if os.path.exists( self.fn ):
            os.unlink( self.fn )

        self.sto = paxos.FilePaxosStorage( self.fn )
        self.sto.init()
        # time_window is 10 times large as current ts
        self.acc = paxos.Acceptor( None, None, self.sto, time_window=time.time()*10000 )

    def tearDown( self ):
        if os.path.exists( self.fn ):
            os.unlink( self.fn )

    def test_phase1a_unpresented_instid( self ):

        resp, err = self.acc.phase1a( {} )

        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidField' ], err.err )
        self.eq( 'instid', err.val[ 0 ] )
        self.eq( None, err.val[ 1 ] )

    def test_phase1a_uncompatible_instid( self ):

        req = { 'instid': 'a',
                'rnd': [ 12, 'a' ], }

        resp, err = self.acc.phase1a( req )

        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidField' ], err.err )
        self.eq( 'instid', err.val[ 0 ] )
        self.eq( req[ 'instid' ], err.val[ 1 ] )

    def test_phase1a_invalid_rnd( self ):
        self._test_phase1a_invalid_rnd( 12 )
        self._test_phase1a_invalid_rnd( [ 12 ] )
        self._test_phase1a_invalid_rnd( [ 12, 1, 2 ] )

    def _test_phase1a_invalid_rnd( self, rnd ):

        req = { 'instid': [ 'a', 1 ],
                'rnd': rnd, }

        resp, err = self.acc.phase1a( req )

        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidField' ], err.err )
        self.eq( 'rnd', err.val[ 0 ] )
        self.eq( req[ 'rnd' ], err.val[ 1 ] )

    def test_phase1a_time_window_wrong( self ):

        # default time window
        self.acc = paxos.Acceptor( None, None, self.sto )

        req = { 'instid':[ 'a', 1 ],
                'rnd': [ 12, 'a' ],
        }

        resp, err = self.acc.phase1a( req )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidTs' ], err.err )


    def test_phase1a_time_window_correct( self ):

        # default time window
        self.acc = paxos.Acceptor( None, None, self.sto )

        req = { 'instid':[ 'a', 1 ],
                'rnd': [ paxos.make_ts(), 'a' ],
        }

        resp, err = self.acc.phase1a( req )
        self.eq( None, err )

    def test_phase1a_empty_acc_ok( self ):

        req = { 'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a' ], }

        resp, err = self.acc.phase1a( req )

        self.eq( None, err )
        self.eq( ( 'last_rnd', ), tuple( resp.keys() ) )
        self.eq( ( 0, 0 ), resp[ 'last_rnd' ] )

        # same rnd
        req2 = { 'instid': [ 'a', 1 ],
                 'rnd': [ 12, 'a' ], }

        resp, err = self.acc.phase1a( req2 )
        dd( 'resp from dup phase1a req: ' + repr( resp ) )

        self.eq( None, err )
        self.eq( ( 'last_rnd', ), tuple( resp.keys() ) )
        self.eq( tuple(req[ 'rnd' ]), resp[ 'last_rnd' ] )

        # greater rnd
        req3 = { 'instid': [ 'a', 1 ],
                 'rnd': [ 13, 'a' ], }

        resp, err = self.acc.phase1a( req3 )
        dd( 'resp from next phase1a req: ' + repr( resp ) )

        self.eq( None, err )
        self.eq( ( 'last_rnd', ), tuple( resp.keys() ) )
        self.eq( tuple(req[ 'rnd' ]), resp[ 'last_rnd' ] )

        # greater rnd again
        req4 = { 'instid': [ 'a', 1 ],
                 'rnd': [ 14, 'a' ], }

        resp, err = self.acc.phase1a( req4 )
        dd( 'resp from second next phase1a req: ' + repr( resp ) )

        self.eq( None, err )
        self.eq( ( 'last_rnd', ), tuple( resp.keys() ) )
        self.eq( tuple(req3[ 'rnd' ]), resp[ 'last_rnd' ] )

        # lower rnd again
        req5 = { 'instid': [ 'a', 1 ],
                'rnd': [ 10, 'a' ], }

        resp, err = self.acc.phase1a( req5 )
        dd( 'resp from lower phase1a req: ' + repr( resp ) )

        self.eq( None, err )
        self.eq( ( 'last_rnd', ), tuple( resp.keys() ) )
        self.eq( tuple(req4[ 'rnd' ]), resp[ 'last_rnd' ] )


    def test_phase1a_empty_invalid_rnd( self ):

        req = { 'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a', 3 ], }

        resp, err = self.acc.phase1a( req )

        dd( 'resp from phase1a req: ' + repr( resp ) )
        dd( 'err from phase1a req: ' + repr( err ) )

        self.eq( None, resp )
        self.neq( None, err )

        self.eq( paxos.ERR[ 'InvalidField' ], err.err )
        self.eq( 'rnd', err.val[ 0 ] )
        self.eq( req[ 'rnd' ], err.val[ 1 ] )

    def test_phase1a_empty_invalid_instid( self ):

        req = { 'instid': [ 'a', 0 ],
                'rnd': [ 12, 'a' ], }
        self._test_phase1a_invalid_instid( req )

        req = { 'instid': [ 'a', 10 ],
                'rnd': [ 12, 'a' ], }
        self._test_phase1a_invalid_instid( req )


    def test_phase1_peek_mode( self ):

        req = { 'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a' ],
                'v': ( 1, 2 ),
                'peek': True,
        }

        resp, err = self.acc.phase1a( req )
        inst, err = self.acc.sto.get_inst( req[ 'instid' ] )
        self.eq( None, err )
        self.eq( ( 0, 0 ), inst.last_rnd )

        resp, err = self.acc.phase1a( req )
        inst, err = self.acc.sto.get_inst( req[ 'instid' ] )
        self.eq( None, err )
        self.eq( ( 0, 0 ), inst.last_rnd )


    def test_phase2a_empty_invalid_instid( self ):

        req = { 'instid': [ 'a', 0 ],
                'rnd': [ 12, 'a' ],
                'v': ( 1, 2 ),
        }

        resp, err = self.acc.phase2a( req )
        self.eq( None, resp )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )
        self.eq( tuple(req[ 'instid' ]), err.val[ 1 ] )
        self.eq( ( 'a', 1 ), err.val[ 0 ] )


    def test_phase2a_empty_valid_instid( self ):

        req = { 'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a' ],
                'v': ( 1, 2, 0 ),
        }

        resp, err = self.acc.phase2a( req )
        dd( 'resp from valid phase2a req: ' + repr( resp ) )
        dd( 'err from valid phase2a req: ' + repr( err ) )
        self.neq( None, resp )
        self.eq( None, err )


    def test_phase2a_empty_invalid_value( self ):

        req = { 'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a' ],
                'v': 1,
        }

        resp, err = self.acc.phase2a( req )
        self.eq( None, resp )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidField' ], err.err )
        self.eq( 'v', err.val[ 0 ] )
        self.eq( req[ 'v' ], err.val[ 1 ] )

    def test_phase2a_empty_invalid_rnd( self ):

        req = { 'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a', 3 ],
                'v': ( 1, 2 ),
        }

        resp, err = self.acc.phase2a( req )
        self.eq( None, resp )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'InvalidField' ], err.err )
        self.eq( 'rnd', err.val[ 0 ] )
        self.eq( req[ 'rnd' ], err.val[ 1 ] )

    def test_classic_all( self ):

        # phase1
        req = { 'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a' ],
        }

        resp, err = self.acc.phase1a( req )
        self.eq( None, err )
        self.eq( ( 0, 0 ), resp[ 'last_rnd' ] )


        # phase2 accepted
        req = {
                'instid': [ 'a', 1 ],
                'rnd': [ 12, 'a' ],
                'v': ( 1, 2, 0 ),
        }

        resp, err = self.acc.phase2a( req )
        dd( 'resp from phase2a req: ' + repr( resp ) )
        dd( 'err from phase2a req: ' + repr( err ) )
        self.eq( None, err )
        self.true( resp[ 'accepted' ] )

        # phase2 not accepted
        req[ 'rnd' ] = [ 11, 'a' ]
        resp, err = self.acc.phase2a( req )
        dd( 'resp from phase2a req: ' + repr( resp ) )
        dd( 'err from phase2a req: ' + repr( err ) )
        self.eq( None, err )
        self.false( resp[ 'accepted' ] )

        # phase1 got v and vrnd
        req = {
                'instid': [ 'a', 1 ],
                'rnd': [ 13, 'a' ],
        }
        resp, err = self.acc.phase1a( req )
        dd( 'resp from phase2a req: ' + repr( resp ) )
        dd( 'err from phase2a req: ' + repr( err ) )
        self.eq( None, err )
        self.eq( ( 12, 'a' ), resp[ 'last_rnd' ] )
        self.eq( ( 1, 2 ), resp[ 'v' ][ :2 ] )
        self.eq( ( 12, 'a' ), resp[ 'vrnd' ] )

    def test_fast_accepted( self ):

        req = {
                'instid': [ 'a', 1 ],
                'rnd': [ 0, 0 ],
                'v': [ 'a', 'b', 0 ],
        }
        resp, err = self.acc.phase2a( req )
        self.eq( None, err )
        self.true( resp[ 'accepted' ] )

        req = {
                'instid': [ 'a', 1 ],
                'rnd': [ 1, 1 ],
                'v': [ 'a', 'b', 0 ],
        }
        resp, err = self.acc.phase2a( req )
        self.eq( None, err )
        self.true( resp[ 'accepted' ] )


    def test_fast_after_classic( self ):
        req = {
                'instid': [ 'a', 1 ],
                'rnd': [ 1, 1 ],
                'v': [ 'a', 'b', 0 ],
        }
        resp, err = self.acc.phase2a( req )
        self.eq( None, err )
        self.true( resp[ 'accepted' ] )

        req[ 'rnd' ] = [ 0, 0 ]
        resp, err = self.acc.phase2a( req )
        self.eq( None, err )
        self.false( resp[ 'accepted' ] )

    def test_fast_with_classic( self ):
        req = {
                'instid': [ 'a', 1 ],
                'rnd': [ 0, 0 ],
                'v': [ 'a', 'b', 0 ],
                'classic': {
                        'instid': [ 'a', 1 ],
                        'rnd': [ 1, 1 ],
                }
        }
        resp, err = self.acc.phase2a( req )
        self.eq( None, err )
        self.true( resp[ 'accepted' ] )
        self.neq( None, resp[ 'classic' ] )
        self.eq( None, resp[ 'classic' ][ 1 ] )
        self.eq( ( 0, 0 ), resp[ 'classic' ][ 0 ][ 'last_rnd' ] )


    def _test_phase1a_invalid_instid( self, req ):

        resp, err = self.acc.phase1a( req )

        self.eq( None, resp )
        self.neq( None, err )

        self.eq( paxos.ERR[ 'InvalidInstanceId' ], err.err )
        self.eq( tuple(req[ 'instid' ]), err.val[ 1 ] )
        self.eq( ( 'a', 1 ), err.val[ 0 ] )


class TestAcceptorCorruption( Base ):

    def setUp( self ):

        self._make_cluster( 3 )

        self.accs = {}
        for ident in self.cluster:

            self.accs[ ident ] = self._init_acc( ident )
            self.accs[ ident ][ 'sto' ].init()

        self.instid = paxos.InstanceId( 'k', 1 )
        self.instids = [ self.instid ]
        self.val = paxos.make_ts()
        self.new_val = paxos.make_ts() + 100
        self._make_proposer( self.val )

    def _make_proposer( self, vdata ):
        self.p = paxos.Proposer( 'x', self.cluster, self.instid, vdata )

    def _init_acc( self, ident ):

        fn = 'tmp-' + ident
        if os.path.exists( fn ):
            os.unlink( fn )

        sto = paxos.FilePaxosStorage( fn )
        acc = paxos.Acceptor( ident, self.cluster, sto )
        return { 'fn': fn,
                 'sto': sto,
                 'acc': acc, }

    def tearDown( self ):
        for ident in self.cluster:
            fn = self.accs[ ident ][ 'fn' ]
            if os.path.exists( fn ):
                os.unlink( fn )

    def test_no_repair_is_wrong( self ):

        v, rc = self._test_paxos_on_ab()
        self.eq( self.val, v.data, 'inited storages works' )


        self.accs[ 'b' ] = self._init_acc( 'b' )
        # not init()

        resp, err = self.accs[ 'b' ][ 'acc' ].phase1a(
                self.p.make_phase1_req() )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'Phase1Disabled' ], err.err,
                 'non-inited storage refuse phase1 req' )


        self.accs[ 'b' ][ 'sto' ].init()

        v, err = self.p.choose_v(
                self.rpc_p1_bc(
                        self.p.make_phase1_req() ) )
        self.eq( None, err )
        self.eq( self.val, v.data, 'broken storage lose value' )

    def _test_paxos_on_ab( self ):

        v, err = self.p.choose_v(
                self.rpc_p1_ab(
                        self.p.make_phase1_req() ) )
        self.eq( None, err, 'phase1 ok at first' )

        rc, err = self.p.is_phase2_ok(
                self.rpc_p2_ab(
                        self.p.make_phase2_req( v ) ) )
        self.eq( None, err, 'phase2 ok at first' )

        return v, rc

    def test_fix( self ):

        self._test_paxos_on_ab()

        self.accs[ 'b' ] = self._init_acc( 'b' )
        # not init()

        self.p.rnd._replace( ts=self.p.rnd.ts-10 )

        fixer = paxos.AcceptorRepair( self.accs[ 'b' ][ 'acc' ], self.rpc_p1_abc,
                                      self.instids )
        rc, err = fixer.fix()
        self.eq( None, err )

        v, err = self.p.choose_v(
                self.rpc_p1_bc(
                        self.p.make_phase1_req() ) )

        self.neq( None, err )
        self.eq( paxos.ERR[ 'Phase1Failure' ], err.err,
                 'after fix, no lower rnd works' )

        self._make_proposer( self.new_val )

        v, err = self.p.choose_v(
                self.rpc_p1_bc(
                        self.p.make_phase1_req() ) )

        self.eq( None, err, 'after fix, paxos works' )
        self.eq( self.val, v.data, 'val previously chosen returned' )



    def rpc_p1( self, req, ids ):
        resps = {}
        for k in ids:
            resps[ k ] = self.accs[ k ][ 'acc' ].phase1a( req )
        return resps

    def rpc_p2( self, req, ids ):
        resps = {}
        for k in ids:
            resps[ k ] = self.accs[ k ][ 'acc' ].phase2a( req )
        return resps

    def rpc_p1_ab( self, req ): return self.rpc_p1( req, 'ab' )
    def rpc_p1_bc( self, req ): return self.rpc_p1( req, 'bc' )
    def rpc_p1_abc( self, req ): return self.rpc_p1( req, 'abc' )

    def rpc_p2_ab( self, req ): return self.rpc_p2( req, 'ab' )



class TestProposerClassicMode( Base ):

    def setUp( self ):
        self._make_cluster( 3 )

        self.key = 'a'
        self.ver = 1
        self.prp_id = 'a'
        self.v = 'value'
        self.p = paxos.Proposer( self.prp_id, self.cluster,
                                 [ self.key, self.ver ], self.v )

    def tearDown( self ):
        pass

    def test_initial_state( self ):
        self.eq( self.prp_id, self.p.ident )
        self.eq( self.prp_id, self.p.v.prp_id )
        self.eq( self.v, self.p.v.data )
        self.eq( self.key, self.p.instid.key )
        self.eq( self.ver, self.p.instid.ver )

    def test_specifying_rnd( self ):
        rnd = ( 1, 2 )
        self.p = paxos.Proposer( self.prp_id, self.cluster,
                                 [ self.key, self.ver ], self.v, rnd=rnd )
        self.eq( rnd, self.p.rnd )

    def test_rnd( self ):
        self.neq( None, self.p.rnd )
        self.eq( 0, (paxos.make_ts() - self.p.rnd.ts)/1000 )

        r0 = self.p.rnd
        time.sleep( 0.1 )

        self.p.next_rnd()
        self.neq( r0, self.p.rnd )
        self.true( self.p.gt( self.p.rnd, r0 ) )
        self.eq( r0.prp_id, self.p.rnd.prp_id )

    def test_phase1_req( self ):

        req = self.p.make_phase1_req()

        self.eq( set( [ 'instid', 'rnd', 'cmd', 'cluster_hash' ] ), set( req.keys() ) )
        self.eq( self.key, req[ 'instid' ][ 0 ] )
        self.eq( self.ver, req[ 'instid' ][ 1 ] )
        self.neq( None, req[ 'rnd' ] )


    def test_check_phase1_empty_resp( self ):
        resps = {}
        self._expect_phase1failure( resps )


    def test_check_phase1_1_err_resp( self ):
        resps = {
                'a': ( None, 1 ),
        }
        self._expect_phase1failure( resps )


    def test_check_phase1_3_err_resp( self ):
        resps = {
                'a': ( None, 1 ),
                'b': ( None, 1 ),
                'c': ( None, 1 ),
        }
        self._expect_phase1failure( resps )


    def test_check_phase1_3_invalid_instanceid_err( self ):

        resps = {
                'a': ( None, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
                'b': ( None, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
                'c': ( None, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
        }
        self._expect_phase1failure( resps )
        self.true( 'a' in self.p.invalid_instance_ids )
        self.true( 'b' in self.p.invalid_instance_ids )
        self.true( 'c' in self.p.invalid_instance_ids )

    def test_check_phase1_3_invalid_instanceid_err_1_valid_resp( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
                'b': ( None, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
                'c': ( None, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
        }
        self._expect_phase1failure( resps )
        self.true( 'a' in self.p.invalid_instance_ids )
        self.true( 'b' in self.p.invalid_instance_ids )
        self.true( 'c' in self.p.invalid_instance_ids )

    def test_check_phase1_3_err_2_valid( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
                'c': ( None, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
        }
        self._expect_phase1failure( resps )
        self.true( 'a' in self.p.invalid_instance_ids )
        self.true( 'b' in self.p.invalid_instance_ids )
        self.true( 'c' in self.p.invalid_instance_ids )

    def test_phase1_ok( self ):

        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, None ),
                'c': ( None, paxos.Error( paxos.ERR['InvalidInstanceId'], ( 1, 2 ) ) ),
        }
        rc, err = self.p.is_phase1_ok( resps )
        dd( err )
        self.true( 'b' not in self.p.invalid_instance_ids )
        self.true( 'c' in self.p.invalid_instance_ids )
        self.true( rc )

        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) }, None ),
        }
        rc, err = self.p.is_phase1_ok( resps )
        dd( err )
        self.true( rc )

        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ) }, None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) }, None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) }, None ),
        }
        rc, err = self.p.is_phase1_ok( resps )
        dd( err )
        self.false( rc )

    def test_choose_value_no_value_acceppted( self ):

        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ), }, None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ), }, None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) }, None ),
        }
        v, err = self.p.choose_v_nocheck( resps )
        self.eq( self.prp_id, v.prp_id )
        self.eq( self.v, v.data )

    def test_proposer_vrnd_no_accepted( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ), },
                       None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ), },
                       None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) },
                       None ),
        }

        v, err = self.p.choose_v( resps )
        self.eq( None, err )
        self.eq( self.v, v.data )
        self.eq( self.p.rnd, self.p.vrnd )


    def test_proposer_vrnd_not_chosen( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ),
                         'v':( 'a', 1, 0 ),
                         'vrnd':paxos.Rnd( 1, 1 ) },
                       1 ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ),
                         'v':( 'b', 1, 0 ),
                         'vrnd':paxos.Rnd( 2, 1 ) },
                       1 ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) },
                       None ),
        }

        v, err = self.p.choose_v( resps )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'Phase1Failure' ], err.err )
        self.eq( None, v )
        self.eq( None, self.p.vrnd )


    def test_proposer_vrnd_1( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ),
                         'v':( 'a', 1, 0 ),
                         'vrnd':paxos.Rnd( 1, 1 ) },
                       None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ),
                         'v':( 'b', 1, 0 ) },
                       None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) },
                       None ),
        }

        v, err = self.p.choose_v_nocheck( resps )
        self.eq( 'a', v.prp_id )
        self.eq( 1, v.data )
        self.eq( resps[ 'a' ][ 0 ][ 'vrnd' ], self.p.vrnd )


    def test_proposer_vrnd_2( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ),
                         'v':( 'a', 1, 0 ),
                         'vrnd':paxos.Rnd( 1, 1 ) },
                       None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ),
                         'v':( 'b', 1, 0 ),
                         'vrnd':paxos.Rnd( 2, 1 ) },
                       None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) },
                       None ),
        }

        v, err = self.p.choose_v_nocheck( resps )
        self.eq( 'b', v.prp_id )
        self.eq( 1, v.data )
        self.eq( resps[ 'b' ][ 0 ][ 'vrnd' ], self.p.vrnd )

    def test_choose_value_vrnd_must_presents( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ), 'v':( 'a', 1, 0 ), 'vrnd':paxos.Rnd( 1, 1 ) }, None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ), 'v':( 'b', 1, 0 ) }, None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ) }, None ),
        }
        v, err = self.p.choose_v_nocheck( resps )
        self.eq( 'a', v.prp_id )
        self.eq( 1, v.data )

    def test_choose_value_chooose_greatest_vrnd( self ):
        resps = {
                'a': ( { 'last_rnd': paxos.Rnd( 1, 1 ), 'v':( 'a', 1, 0 ), 'vrnd':paxos.Rnd( 1, 1 ) }, None ),
                'b': ( { 'last_rnd': paxos.Rnd( 1, 1 ), 'v':( 'b', 1, 0 ) }, None ),
                'c': ( { 'last_rnd': paxos.Rnd( paxos.make_ts()+2000, 1 ),
                         'v':( 'c', 1, 0 ), 'vrnd':paxos.Rnd( 2, 1 ) }, None ),
        }
        v, err = self.p.choose_v_nocheck( resps )
        self.eq( 'c', v.prp_id )
        self.eq( 1, v.data )

    def test_phase2_req( self ):

        req = self.p.make_phase2_req( 'value_chosen' )

        self.eq( set( [ 'instid', 'rnd', 'v', 'cmd', 'cluster_hash' ] ), set( req.keys() ) )
        self.eq( self.key, req[ 'instid' ][ 0 ] )
        self.eq( self.ver, req[ 'instid' ][ 1 ] )
        self.eq( 'value_chosen', req[ 'v' ] )
        self.neq( None, req[ 'rnd' ] )

    def test_check_phase2_err( self ):
        resps = {
        }

        rc, err = self.p.is_phase2_ok( resps )
        self.false( rc )
        self.eq( paxos.ERR[ 'Phase2Failure' ], err.err )

        resps = {
                'a': ( None, 1 ),
                'b': ( None, 1 ),
                'c': ( None, 1 ),
        }
        rc, err = self.p.is_phase2_ok( resps )
        self.false( rc )
        self.eq( paxos.ERR[ 'Phase2Failure' ], err.err )

    def test_check_phase2_ok( self ):
        resps = {
                'a': ( { 'accepted':True }, 1 ),
                'b': ( None, 1 ),
                'c': ( None, 1 ),
        }
        rc, err = self.p.is_phase2_ok( resps )
        self.false( rc )
        self.eq( paxos.ERR[ 'Phase2Failure' ], err.err )

        resps = {
                'a': ( { 'accepted':True }, 1 ),
                'b': ( { 'accepted':True }, 1 ),
                'c': ( None, 1 ),
        }
        rc, err = self.p.is_phase2_ok( resps )
        self.false( rc )
        self.eq( paxos.ERR[ 'Phase2Failure' ], err.err )

        resps = {
                'a': ( { 'accepted':True }, None ),
                'b': ( { 'accepted':True }, None ),
                'c': ( None, 1 ),
        }
        rc, err = self.p.is_phase2_ok( resps )
        self.true( rc )

    def _expect_phase1failure( self, resps ):
        rc, err = self.p.is_phase1_ok( resps )
        self.eq( 'Phase1Failure', err.err )

class TestProposerFastMode( Base ):

    def setUp( self ):
        self._make_cluster( 5, useletter=False )

        self.key = 'a'
        self.ver = 1
        self.prp_id = 0
        self.vdata = 'value'
        self.p = paxos.Proposer( self.prp_id, self.cluster,
                                        [ self.key, self.ver ], self.vdata )

    def tearDown( self ):
        pass

    def test_no_value( self ):

        resps = {
                0: ( {  }, None ),
                1: ( {  }, None ),
                2: ( {  }, None ),
                3: ( {  }, None ),
                4: ( {  }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( self.p.v, v )

    def test_one_classic_value( self ):

        resps = {
                0: ( { 'vrnd':( 1, 1 ), 'v': 1 }, None ),
                1: ( {  }, None ),
                2: ( {  }, None ),
                3: ( {  }, None ),
                4: ( {  }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( 1, v )

    def test_two_different_classic_values( self ):

        resps = {
                0: ( { 'vrnd':( 1, 1 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 2, 1 ), 'v': 2 }, None ),
                2: ( {  }, None ),
                3: ( {  }, None ),
                4: ( {  }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( 2, v )

    def test_3_different_classic_values( self ):

        resps = {
                0: ( { 'vrnd':( 1, 1 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 2, 1 ), 'v': 2 }, None ),
                2: ( {  }, None ),
                3: ( {  }, None ),
                4: ( { 'vrnd':( 3, 1 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( 3, v )

    def test_classic_and_fast_1_4( self ):

        resps = {
                0: ( { 'vrnd':( 1, 1 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 0, 0 ), 'v': 2 }, None ),
                2: ( { 'vrnd':( 0, 0 ), 'v': 2 }, None ),
                3: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                4: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( 1, v )

    def test_classic_and_fast_2_3( self ):

        resps = {
                0: ( { 'vrnd':( 1, 1 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 2, 1 ), 'v': 4 }, None ),
                2: ( { 'vrnd':( 0, 0 ), 'v': 2 }, None ),
                3: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                4: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( 4, v )

    def test_fast_2_3( self ):

        resps = {
                0: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 0, 0 ), 'v': 2 }, None ),
                2: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                3: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                4: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( self.p.v, v )


    def test_fast_1_4( self ):

        resps = {
                0: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                2: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                3: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                4: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( 3, v )

    def test_fast_3_4( self ):

        resps = {
                0: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                2: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                3: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                4: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                5: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                6: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( self.p.v, v )

    def test_fast_2_5( self ):

        resps = {
                0: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                2: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                3: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                4: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                5: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                6: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( self.p.v, v )

    def test_fast_1_6( self ):

        resps = {
                0: ( { 'vrnd':( 0, 0 ), 'v': 1 }, None ),
                1: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                2: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                3: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                4: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                5: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
                6: ( { 'vrnd':( 0, 0 ), 'v': 3 }, None ),
        }

        v, err = self.p.coordinate_v( resps )
        self.eq( 3, v )



class TestClassicFunction( Base ):

    def setUp( self ):
        self._make_cluster( 3 )

        self.srvs = {}
        for ident, x in self.cluster.items():
            fn = 'tmp-' + str( ident )
            sto = paxos.FilePaxosStorage( fn )
            sto.init()
            srv = paxos.SingleAcceptorServer( x[ 'addrs' ][ 0 ],
                                              paxos.Acceptor( ident, self.cluster, sto ) )

            th = paxos.daemon_thread( srv.serve_forever )

            self.srvs[ ident ] = {
                    'fn': fn,
                    'srv': srv,
                    'th': th,
            }

        time.sleep( 0.1 )

    def tearDown( self ):

        for ident, x in self.srvs.items():
            x[ 'srv' ].shutdown()

        for ident, x in self.srvs.items():
            x[ 'th' ].join()

            if os.path.exists( x[ 'fn' ] ):
                os.unlink( x[ 'fn' ] )


    def test_raw_classic( self ):

        self.prp_id = 'a'
        self.instid = [ 'a', 1 ]

        p = paxos.Proposer( self.prp_id, self.cluster,
                                   self.instid, 100 )

        req = p.make_phase1_req()
        req[ 'cmd' ] = 'phase1a'
        resps = paxos.http_rpc( self.cluster, req )

        dd( 'phase1 resps:' + repr( resps ) )

        rc, err = p.is_phase1_ok( resps )
        self.eq( None, err )

        v, err = p.choose_v_nocheck( resps )
        dd( 'value chosen: ' + repr( v ) )
        self.eq( None, err )

        req = p.make_phase2_req( v )
        req[ 'cmd' ] = 'phase2a'
        resps = paxos.http_rpc( self.cluster, req )

        dd( 'phase2 resps: ' + repr( resps ) )

        for ident, x in resps.items():
            resp, err = x
            self.true( resp[ 'accepted' ] )

        rc, err = p.is_phase2_ok( resps )
        self.eq( None, err )


        # respect existent value
        time.sleep( 0.1 )
        p = paxos.Proposer( self.prp_id, self.cluster,
                                   self.instid, 101 )
        req = p.make_phase1_req()
        req[ 'cmd' ] = 'phase1a'
        resps = paxos.http_rpc( self.cluster, req )
        dd( 'phase1 resps:' + repr( resps ) )

        rc, err = p.is_phase1_ok( resps )
        self.eq( None, err )

        v, err = p.choose_v_nocheck( resps )
        dd( 'value chosen: ' + repr( v ) )
        self.eq( ( p.ident, 100, 0 ), v )
        self.eq( None, err )

    def test_classic( self ):

        argkv, v, err = self._test_classic()
        self.eq( argkv[ 'ident' ], v[0] )
        self.eq( argkv[ 'vdata' ], v[1] )

        prev_vdata = argkv[ 'vdata' ]
        argkv[ 'vdata' ] = ts()
        argkv, v, err = self._test_classic()
        self.eq( prev_vdata, v[ 1 ], 'respect existent value:data' )


    def _test_classic( self, **extra ):
        argkv = {
                'ident': 'a',
                'cluster': self.cluster,
                'instid': ('a', 1),
                'vdata': ts(),
        }
        argkv.update( extra )

        v, err = paxos.classic( **argkv )
        self.eq( None, err )

        return argkv, v, err


    def test_classic_argument_rst( self ):

        rst = {}
        _ts = ts()
        time.sleep( 0.01 ) # to make Proposer default rnd greater
        argkv, v, err = self._test_classic( rst=rst, rnd=( _ts, 'x' ))

        self.true( 'accepted_ids' in rst )
        self.eq( set( self.cluster.keys() ), set( rst[ 'accepted_ids' ] ) )

        self.true( 'vrnd' in rst )
        self.eq( ( _ts, 'x' ), rst[ 'vrnd' ] )


    def test_classic_specifying_rnd( self ):

        vdata = 102
        argkv, v, err = self._test_classic( vdata=vdata )
        self.eq( vdata, v[ 1 ] )

        vdata = 103
        v, err = paxos.classic( 'a', self.cluster, [ 'a', 1 ], vdata,
                                rnd=( 1, 2 ) )
        self.neq( None, err )
        self.eq( paxos.ERR[ 'Phase1Failure' ], err.err,
                 'low rnd does not work' )

class TestFastFunction( Base ):

    def setUp( self ):
        self._make_cluster( 5, useletter=False )
        self.prp_id = 'proposer_id'
        self.instid = [ 'a', 1 ]

        self.srvs = {}
        for ident, x in self.cluster.items():
            fn = 'tmp-fast-' + str( ident )
            sto = paxos.FilePaxosStorage( fn )
            sto.init()
            srv = paxos.SingleAcceptorServer( x[ 'addrs' ][ 0 ],
                                              paxos.Acceptor( ident, self.cluster, sto ))

            th = paxos.daemon_thread( srv.serve_forever )

            self.srvs[ ident ] = {
                    'fn': fn,
                    'srv': srv,
                    'th': th,
            }

        time.sleep( 0.1 )

    def tearDown( self ):

        for ident, x in self.srvs.items():
            x[ 'srv' ].shutdown()

        for ident, x in self.srvs.items():
            x[ 'th' ].join()

            if os.path.exists( x[ 'fn' ] ):
                os.unlink( x[ 'fn' ] )


    def test_fast( self ):

        fast_val = 'value_for_fast'

        v, err = paxos.fast( self.prp_id, self.cluster, self.instid, fast_val )
        dd( 'v chosen by fast: ' + repr( v ) )
        dd( 'err for fast: ' + repr( err ) )

        self.eq( None, err )
        self.eq( fast_val, v[ 1 ] )


# TODO test ts in req
# TODO remove InvalidTs err

if __name__ == "__main__":

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    classes = (
            TestRndNumber,
            TestRndManager,
            TestFilePaxosStorage,
            TestFilePaxosStorage_Rnd3,
            TestAcceptor,
            TestAcceptorCorruption,
            TestAcceptorWithCluster,
            TestProposerClassicMode,
            TestProposerFastMode,
            TestClassicFunction,
            TestFastFunction,
    )

    for clz in classes:
        suite.addTests( loader.loadTestsFromTestCase( clz ) )

    testframe.TestRunner(verbosity=2).run(suite)
