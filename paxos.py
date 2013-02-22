#!/usr/bin/env python2.6
# coding: utf-8

import os, sys
import copy
import hashlib
import socket
import time
import logging, traceback
import json
import threading
import BaseHTTPServer
import httplib

from collections import namedtuple

from datatype import *

REQUEST_TIMEOUT = 0.1
# time window must be unique globally. or time window based storage
# reparation won't work correctly.
TIME_WINDOW = 2000

# abbreviation psm paxos_statemachine

KEY = {
        # current leader and lease, by paxos
        'leader': '/leader',

        # cluster members
        'cluster': '/cluster',

        # one paxos group
        'group': '/group',

        # mark of a non-corrupted storage
        'inited': '/internal/inited',
}

ERR = dict( [ ( x, x ) for x in (
        'InvalidCluster',
        'InvalidInstanceId',
        'InvalidField',
        'InvalidTs',
        'PaxosStorageNotFound',
        'CorruptedPaxosStorage',
        'Phase1Failure',
        'Phase2Failure',
        'Phase1Disabled',
        'Phase2Disabled',
        'InvalidMessage',
        'UnknownError',
        'NetworkError',
        'LeaderExpired',
) ] )

def make_logger( name='pypaxos' ):

    DEFAULT_FORMAT = "[%(asctime)s tid=%(thread)-15d, %(lineno)d, %(levelname)-5s] %(message)s"
    logger = logging.getLogger( name )

    logger.setLevel( logging.WARN )

    hdr = logging.StreamHandler( sys.stdout )
    hdr.setFormatter( logging.Formatter( DEFAULT_FORMAT ) )
    logger.addHandler( hdr )

    return logger

logger = make_logger()

def classic( ident, cluster,
             instid, vdata,
             lease=0, rnd=None,
             rst=None ):

    rpc_proxy = make_rpc_proxy( cluster )

    p = Proposer( ident, cluster,
                  instid, vdata, lease, rnd=rnd )

    resps = rpc_proxy( p.make_phase1_req() )

    v, err = p.choose_v( resps )
    if err: return None, err

    resps = rpc_proxy( p.make_phase2_req( v ) )

    rc, err = p.is_phase2_ok( resps )
    if err: return None, err

    if rst is not None:
        rst[ 'accepted_ids' ] = p.accepted_ids[:]
        rst[ 'vrnd' ] = p.rnd

    return v, None


def fast( ident, cluster, instid, vdata, lease=0 ):

    p = Proposer( ident, cluster,
                  instid, vdata, lease )

    rpc_proxy = make_rpc_proxy( cluster )

    resps = rpc_proxy( p.make_fast_req() )

    rc, err = p.is_fast_phase2_ok( resps )
    if err: return None, err

    if rc:
        return p.v, None

    # NOTE: If fast accept fails, fall back to classic and continue to
    # check if classic phase 1 succeeded.
    resps = p.extract_fast_phase1_resps( resps )

    v, err = p.coordinate_v( resps )
    if err: return None, err

    resps = rpc_proxy( p.make_phase2_req( v ) )

    rc, err = p.is_phase2_ok( resps )
    if err: return None, err

    return v, None


class RndManager( object ):

    rec_clz = SingleVersionRecord
    inst_clz = Instance
    rnd_clz = Rnd

    def next_rnd( self, ident ):
        return self.make_rnd( make_ts(), hash( ident ) % 1000 )

    def make_rnd( self, *args ):
        return self.rnd_clz( *args )

    def make_inst( self, *args ):
        return self.inst_clz( *args )

    def make_rec( self, *args ):
        return self.rec_clz( *args )

    def gt( self, a, b ): return a > b
    def lt( self, a, b ): return a < b
    def eq( self, a, b ): return a == b
    def lteq( self, a, b ): return self.lt( a, b ) or self.eq( a, b )
    def gteq( self, a, b ): return self.gt( a, b ) or self.eq( a, b )

    def is_fast_rnd( self, rnd ):
        return rnd.ts == 0 and rnd.prp_id == 0

    def zero_rnd( self ):
        return self.make_rnd( 0, 0 )

    def zero_inst( self ):
        return self.inst_clz( v=None,
                              vrnd=self.zero_rnd(),
                              last_rnd=self.zero_rnd(),
                              expire=0, )

    def zero_rec( self ):
        return self.rec_clz( committed=ValueVersion( None, 0 ),
                             instance=self.zero_inst() )


class PaxosBase( object ):

    def __init__( self, ident, cluster,
                  logger=logger ):

        self.ident = str(ident)
        self.cluster = _cluster_normalize( cluster )
        self.cluster_hash = _cluster_hash( self.cluster )

        self.logger = logger


class Proposer( PaxosBase, RndManager ):

    rndmgr = RndManager()

    def __init__( self, ident, cluster,
                  instid, vdata, lease=0,
                  rnd=None,
                  **argkv ):

        PaxosBase.__init__( self, ident, cluster, **argkv )
        self.instid = InstanceId( *instid )

        # Identifier is part of value to choose, thus different proposers
        # always propose different value.
        self.v = Value( ident, vdata, lease, 0 )
        self.vrnd = None

        if rnd is not None:
            self.rnd = self.rndmgr.make_rnd( *rnd )
        else:
            self.rnd = None
            self.next_rnd()

        self.fast_rnd = self.rndmgr.zero_rnd()
        self.accepted_ids = []

        # Running paxos on committed instance or on instance id not following
        # latest committed, emits InvalidInstanceId error
        self.invalid_instance_ids = {}

        # Proposer rnd or greatest last_rnd returned from acceptors.
        self.greatest_rnd = None

    def next_rnd( self ):
        self.rnd = self.rndmgr.next_rnd( self.ident )
        self.greatest_rnd = self.rnd

    def make_phase1_req( self, **argkv ):
        return _dict_merge( { 'instid': self.instid,
                              'rnd': self.rnd,
                              'cmd': 'phase1a',
        }, self._req_args(), argkv )

    def choose_v( self, resps, quorum=None ):

        rc, err = self.is_phase1_ok( resps, quorum )
        if err: return None, err

        return self.choose_v_nocheck( resps )

    def is_phase1_ok( self, resps, quorum=None ):
        quorum = quorum or ( len(resps) / 2 + 1 )

        n = 0
        for ident, ( resp, err ) in resps.items():

            if err:
                self._deal_with_err( ident, err )
                continue

            last_rnd = resp.get( 'last_rnd' ) or self.zero_rnd()
            last_rnd = self.rndmgr.rnd_clz( *last_rnd )
            if self.rndmgr.gt( last_rnd, self.greatest_rnd ):
                self.greatest_rnd = last_rnd

            if self.rndmgr.gt( self.rnd, last_rnd ):
                n += 1

        total = len( resps )
        self.logger.debug( "Phase1 resp: {n} / {total}".format( n=n, total=total ) )
        if n < quorum:
            return None, Error( ERR['Phase1Failure'], ( total, quorum, n ) )

        return True, None


    def choose_v_nocheck( self, resps ):

        resps = [ resp for resp, err in resps.values()
                  if err is None ]

        max_vrnd_resp = max( resps, key=lambda x: x.get( 'vrnd', 0 ) )
        v = max_vrnd_resp.get( 'v' )
        self.vrnd = max_vrnd_resp.get( 'vrnd' ) or self.rnd

        if v is None:
            v = self.v
            self.logger.info( "Choose new value: " + repr( self.instid )
                              + ' ' + repr( v ) )
        else:
            self.logger.debug( "Choose existent value: " + repr( self.instid )
                              + ' ' + repr( v ) )

        v = Value( *v )
        return v, None

    def make_phase2_req( self, v, **argkv ):
        return _dict_merge( { 'instid': self.instid,
                              'rnd': self.rnd,
                              'v': v,
                              'cmd': 'phase2a',
        }, self._req_args(), argkv )

    def is_phase2_ok( self, resps, quorum=None ):
        return self._is_phase2_ok( resps, quorum )

    # -- fast paxos -- from here down

    def make_fast_req( self, **argkv ):
        # Fast starts with a phase2 request. In case phase 2 fails, make a
        # phase 1 for next classic round at once, within the same request.
        return _dict_merge( { 'instid': self.instid,
                              'rnd': self.fast_rnd,
                              'v': self.v,
                              'classic': self.make_phase1_req(),
                              'cmd': 'phase2a',
        }, self._req_args(), argkv )


    def is_fast_phase2_ok( self, resps ):
        return self._is_phase2_ok( resps, len(resps)*3/4+1 )

    def extract_fast_phase1_resps( self, resps ):
        rst = {}
        for ident, ( resp, err ) in resps.items():
            if err:
                rst[ ident ] = None, err

            if 'classic' in resp:
                rst[ ident ] = resp[ 'classic' ]
            else:
                rst[ ident ] = None, Error( ERR[ 'UnknownError' ], [] )
        return rst


    def coordinate_v( self, resps ):
        rc, err = self.is_phase1_ok( resps )
        if err: return None, err

        return self.coordinate_v_nocheck( resps )


    def coordinate_v_nocheck( self, resps ):

        rr = []
        for ident, ( resp, err ) in resps.items():

            if err:
                continue

            rr.append( resp )

        n_seen = len( rr )
        rr.sort( key=lambda x: x.get( 'vrnd', 0 ) )

        vrnd_last = rr[ -1 ].get( 'vrnd' ) or self.zero_rnd()

        # We assume that for each version, the first round is fast round, all
        # others are classic.

        if len( rr ) > 0 \
                and self.rndmgr.gt( vrnd_last, self.fast_rnd ):

            # The highest round  number which is larger than fast round must
            # be a classic round.

            v = rr[ -1 ][ 'v' ]
            return v, None

        # All latest rounds are fast rounds. To choose value that has been
        # chosen or would have been chosen by at least 3/4 accepters.

        latest_rnds = [ x for x in rr
                        if self.rndmgr.eq( x.get( 'vrnd' ), self.fast_rnd ) ]

        vals = {}
        for resp in latest_rnds:
            v0 = resp.get( 'v' )
            if type( v0 ) == type( [] ):
                v0 = tuple( v0 )
            vals[ v0 ] = vals.get( v0, 0 ) + 1

        if len( vals ) == 0:
            return self.v, None

        vals = vals.items()
        vals.sort( key=lambda x:x[ 1 ] )

        v_most, n = vals[ -1 ]
        n_total = len( resps )

        if ( ( n_total - n_seen ) + n ) > n_total * 3 / 4:
            v = v_most
        else:
            # no value had or would has been chosen
            v = self.v

        return v, None


    def _is_phase2_ok( self, resps, quorum=None ):
        quorum = quorum or ( len(resps) / 2 + 1 )

        self.logger.debug( repr(resps) )
        n = 0
        # TODO test accepted_ids
        self.accepted_ids = []

        for ident, ( resp, err ) in resps.items():

            if err:
                self._deal_with_err( ident, err )
                continue

            if resp[ 'accepted' ]:
                self.accepted_ids.append( ident )
                n += 1

        total = len( resps )

        self.logger.debug( "Phase2 resp: {n} / {total}".format( n=n, total=total ) )
        if n < quorum:
            return None, Error( ERR['Phase2Failure'], ( total, quorum, n ) )

        return True, None


    def _deal_with_err( self, ident, err ):
        if Error._compatible( err ):
            err = Error( *err )
            if ERR[ 'InvalidInstanceId' ] == err.err:
                self.invalid_instance_ids[ ident ] = err.val[ 0 ]

    def _req_args( self ):
        return _dict_merge( self._req_arg_chash(), )

    def _req_arg_chash( self ):
        if self.cluster_hash is not None:
            return { 'cluster_hash': self.cluster_hash }
        else:
            return {}

class Acceptor( PaxosBase ):

    def __init__( self, ident, cluster,
                  paxos_storage, time_window=None,
                  **argkv ):

        PaxosBase.__init__( self, ident, cluster, **argkv )

        if ident is not None and cluster is not None:
            assert ident in self.cluster

        self.sto = paxos_storage
        self.time_window = time_window or TIME_WINDOW

    def cluster_hashes( self ):
        # implementation based
        if self.cluster_hash is None:
            return ()
        else:
            return ( self.cluster_hash, )

    def _check_cluster( self, req ):

        if req.get('cluster_hash') is None:
            return True, None

        chashes = self.cluster_hashes()
        if len(chashes) == 0:
            return True, None
        
        if req[ 'cluster_hash' ] not in chashes:
            return None, Error( ERR[ 'InvalidCluster' ],
                                ( chashes, req[ 'cluster_hash' ] ) )
        return None, None

    def _common_check( self, req ):

        rc, err = self._check_cluster( req )
        if err: return None, err

        err = check_struct( req, 'instid', InstanceId )
        if err: return None, err

        if 'rnd' in req:
            err = check_struct( req, 'rnd', self.sto.rndmgr.rnd_clz )
            if err: return None, err

            rnd = self.sto.rndmgr.make_rnd( *req[ 'rnd' ] )
            
            if not self.sto.rndmgr.is_fast_rnd( rnd ):
                # fast rnd is all zero
                ms = rnd.ts
                now_ms = make_ts()

                if ms > now_ms + self.time_window or ms < now_ms - self.time_window:
                    return None, Error( ERR[ 'InvalidTs' ], ( now_ms, ms ) )

        if 'ts' in req:
            ms = req[ 'ts' ]
            now_ms = make_ts()
            if ms > now_ms + self.time_window or ms < now_ms - self.time_window:
                return None, Error( ERR[ 'InvalidTs' ], ( now_ms, ms ) )

        return True, None


    def phase1a( self, req ):

        if self.sto.corrupted:
            return None, Error( ERR[ 'Phase1Disabled' ], () )

        rc, err = self._common_check( req )
        if err: return None, err

        liid = InstanceId( *req[ 'instid' ] )
        s = self.sto

        with s.lock( liid ):

            o, err = s.get_inst( liid )
            if err: return None, err

            rnd = s.rndmgr.make_rnd( *req[ 'rnd' ] )
            last = o.last_rnd
            resp = { 'last_rnd': last }
            if o.v is not None:
                resp[ 'v' ] = o.v
                resp[ 'vrnd' ] = o.vrnd

            if s.rndmgr.gt( rnd, last ):
                if not req.get( 'peek', False ):
                    o = s.rndmgr.make_inst( o.v, o.vrnd, rnd, o.expire )
                    rc, err = s.set_inst( liid, o, sync=True )
                    if err: return None, err

        return resp, None

    def phase2a( self, req ):

        if self.sto.corrupted:
            return None, Error( ERR[ 'Phase2Disabled' ], () )

        rc, err = self._common_check( req )
        if err: return None, err

        liid = InstanceId( *req[ 'instid' ] )
        s = self.sto

        with s.lock( liid ):

            o, err = s.get_inst( liid )
            if err: return None, err

            err = check_struct( req, 'rnd', s.rndmgr.rnd_clz ) \
                    or check_struct( req, 'v', Value )

            if err: return None, err

            rnd = s.rndmgr.make_rnd( *req[ 'rnd' ] )
            v = Value( *req[ 'v' ] )
            last = o.last_rnd
            resp = {}

            if s.rndmgr.gt( rnd, last ):
                acceptable = True
            elif s.rndmgr.gteq( rnd, last ) and not s.rndmgr.is_fast_rnd( rnd ):
                acceptable = True
            elif s.rndmgr.gteq( rnd, last ) and s.rndmgr.is_fast_rnd( rnd ) and o.v is None:
                acceptable = True
            else:
                acceptable = False

            if acceptable:

                resp[ 'accepted' ] = True

                o = s.rndmgr.make_inst( req[ 'v' ], rnd, rnd, 0 )
                rc, err = s.set_inst( liid, o, sync=True )
                if err: return None, err

            else:
                resp[ 'accepted' ] = False

            if 'classic' in req:
                # a tuple
                resp[ 'classic' ] = self.phase1a( req[ 'classic' ] )

        return resp, None

    def phase3a( self, req ):

        # Phase 3 or commit does not need fix because entering phase 3 means
        # consensus already has been achieved.

        rc, err = self._common_check( req )
        if err: return None, err

        liid = InstanceId( *req[ 'instid' ] )
        s = self.sto

        with s.lock( liid ):
            rc, err = s.commit( liid, req[ 'v' ] )
            if err: return None, err
            return {}, None

    def get_committed( self, req ):

        err = check_struct( req, 'instid', InstanceId )
        if err: return None, err

        liid = InstanceId( *req[ 'instid' ] )
        return self.sto.get_committed( liid )

class AcceptorRepair( PaxosBase ):

    def __init__( self, acceptor, rpc_proxy, instids, **argkv ):
        super( AcceptorRepair, self ).__init__( None, None, **argkv )
        self.acceptor = acceptor
        self.rpc_proxy = rpc_proxy
        self.instids = instids

        self.phase1_fixed = False
        self.phase2_fixed = False


    def fix( self ):

        if not self.acceptor.sto.corrupted:
            return True, None

        rc, err = self.fix_phase1()
        if err:
            return rc, err

        rc, err = self.fix_phase2()
        return rc, err

    def fix_phase1( self ):

        # Sleep time_window * 2 to prevent from accepting phase1 request
        # earlier than any possible phase1 request before corrupted.

        if self.phase1_fixed:
            return None, None

        self.logger.info( "Phase1 fix..." )
        time.sleep( self.acceptor.time_window * 2 / 1000 )
        self.phase1_fixed = True
        return None, None

    def fix_phase2( self ):

        if self.phase2_fixed:
            return None, None

        self.logger.info( "Phase2 fix..." )
        for instid in self.instids:
            p = Proposer( self.acceptor.ident, self.acceptor.cluster, instid, None )

            # TODO accept does not save last_rnd if peek is True
            req = p.make_phase1_req( peek=True )
            resps = self.rpc_proxy( req )
            print resps
            v, err = p.choose_v( resps )
            if err:
                return None, err

            if v.data is not None:
                inst = Instance( v, p.vrnd, p.rnd, 0, )
                self.acceptor.sto.set_inst( instid, inst )

        self.acceptor.sto.init()

        self.phase2_fixed = True
        return None, None

class PaxosStorage( object ):

    def __init__( self ):
        self._lock = threading.RLock()

    def lock( self, instid ):
        return self._lock

    def get_inst( self, instid ): pass
    def set_inst( self, instid, inst, sync=True ): pass
    def get_committed( self, instid ): pass
    def commit( self, instid, v, sync=True ): pass

class FilePaxosStorage( PaxosStorage ):

    rndmgr = RndManager()

    def __init__( self, fn ):

        PaxosStorage.__init__( self )

        self.fn = fn
        self.dic = {}
        self.corrupted = False

        self._load()

    def _load( self ):

        try:
            with open( self.fn ) as f:
                cont = f.read()
        except ( OSError, IOError ) as e:
            self.corrupted = True
            return

        try:
            self.dic = load( cont )
        except ValueError as e:
            self.corrupted = True
            return

        if KEY[ 'inited' ] not in self.dic:
            self.corrupted = True

    def _save( self ):

        cont = dump( self.dic )

        n = make_ts()
        tmpfn = self.fn + str( n )
        with open( tmpfn, 'w' ) as f:
            f.write( cont )
            f.flush()
            os.fsync( f.fileno() )

        os.rename( tmpfn, self.fn )

    def init( self ):
        self.dic[ KEY[ 'inited' ] ] = 1
        self._save()
        self.corrupted = False

    def get( self, k ):
        return self.dic.get( k ), None

    def set( self, k, v, sync=True ):
        self.dic[ k ] = v
        if sync:
            self._save()

        return None, None

    def get_committed( self, instid ):

        key, ver = instid
        rec = self._get_rec( instid )

        if ver in ( None, rec.committed.ver ):
            return rec.committed, None
        else:
            return None, Error( ERR[ 'InvalidInstanceId' ],
                                ( InstanceId( key, rec.committed.ver ),
                                  instid,
                                ) )

    def commit( self, instid, v, sync=True ):

        # Single versioned record is safe to commit any instance with instid
        # greater than its own instid.

        rec = self._get_rec( instid )

        key, ver = instid
        if ( v, ver ) == rec.committed:
            if sync:
                self._save()
            return True, None

        if ver > rec.committed.ver:
            self.dic[ key ] = self.rndmgr.make_rec( ValueVersion( v, ver ),
                                                    self.rndmgr.zero_inst() )
        else:
            return None, Error( ERR[ 'InvalidInstanceId' ],
                                ( InstanceId( key, rec.committed.ver + 1 ),
                                  instid,
                                ) )
        if sync:
            self._save()

        return True, None


    def get_latest_inst( self, key ):

        if key not in self.dic:
            return None, None

        return self.dic[ key ], None

    def get_inst( self, instid ):

        rec = self._get_rec( instid )

        rst, err = self._is_instid_valid( rec, instid )
        if err is not None:
            return None, err

        inst = rec.instance
        v = inst.v
        if inst.expire != 0 and inst.expire < make_ts():
            inst = self.rndmgr.make_inst( None, self.rndmgr.zero_rnd(),
                                          inst.last_rnd, inst.expire )

        return inst, None

    def set_inst( self, instid, inst, sync=True ):

        key, ver = instid
        inst = self.rndmgr.make_inst( *inst )
        v = inst.v
        if v is not None and v.lease != 0:
            inst = self.rndmgr.make_inst( v, inst.vrnd, inst.last_rnd,
                                          v.lease + make_ts() )

        rec = self._get_rec( instid )

        rst, err = self._is_instid_valid( rec, instid )
        if err is not None:
            return None, err

        self.dic[ key ] = self.rndmgr.make_rec( rec.committed, inst )

        if sync:
            self._save()

        return True, None

    def _is_instid_valid( self, rec, instid ):

        key, ver = instid

        if ver != rec.committed.ver + 1:
            return None, Error( ERR[ 'InvalidInstanceId' ],
                                ( InstanceId( key, rec.committed.ver + 1 ),
                                  instid,
                                ) )
        else:
            return True, None

    def _get_rec( self, instid ):

        key, ver = instid

        if key in self.dic:
            rec = self.rndmgr.make_rec( *self.dic[ key ] )
        else:
            rec = self.rndmgr.zero_rec()

        return rec


class HttpHandler( BaseHTTPServer.BaseHTTPRequestHandler ):

    def do_GET( self ):

        try:
            p = self.path.split( '/', 2 )
            if p[1] == '':
                return self.resp( 400, {} )

            cl = self.headers.getheader('content-length')
            if cl is not None:
                cl = int( cl )
                body = self.rfile.read(cl)
                self.body = body
                try:
                    self.req = load( body )
                except Exception, e:
                    self.req = None
            else:
                self.req = None

            meth = getattr( self, 'h_' + p[1] )
            meth()

        except Exception, e:
            logger.error( traceback.format_exc() )
            logger.error( repr( e ) )


    def log_request( self, *args ):
        pass

    def resp( self, code, body ):
        try:
            self.send_response( code )
            self.end_headers()
            self.wfile.write( dump( body ) )
            self.wfile.close()
        except socket.error, e:
            logger.info( repr( e ) )
        except Exception, e:
            logger.error( traceback.format_exc() )
            logger.error( repr( e ) )

    def finish( self ):
        try:
            BaseHTTPServer.BaseHTTPRequestHandler.finish( self )
        except socket.error, e:
            logger.info( repr( e ) )

class SingleAcceptorHandler( HttpHandler ):

    def h_paxos( self ):
        req = self.req
        w = getattr(self.server.acceptor, req[ 'cmd' ])
        self.resp( 200, w( req ) )


class SingleAcceptorServer( BaseHTTPServer.HTTPServer ):

    handler_class = SingleAcceptorHandler

    def __init__( self, addr, acceptor,
                  *args, **argkv ):

        BaseHTTPServer.HTTPServer.__init__( self, addr,
                                            self.handler_class,
                                            *args, **argkv )
        self.acceptor = acceptor


def make_ts():
    return int( ( time.time() + time.timezone ) * 1000 )


def check_struct( dic, k, tp ):
    if not tp._compatible( dic.get( k ) ):
        return Error( ERR[ 'InvalidField' ],
                      ( k, dic.get( k ), ) )
    else:
        return None


def make_rpc_proxy( cluster ):

    cluster = copy.deepcopy( cluster )

    def _rpc( req ):
        return http_rpc( cluster, req )

    return _rpc


def http_rpc( dest_dic, req, timeout=None ):
    timeout = timeout or REQUEST_TIMEOUT
    r = {}
    data = dump( req )
    for ident, dest in dest_dic.items():

        ip, port = dest[ 'addrs' ][ 0 ]
        r[ ident ] = http( ip, port, '/paxos', headers={}, body=data, timeout=timeout )

    return r

def http( ip, port, uri, headers={}, body=None, timeout=None ):
    timeout = timeout or REQUEST_TIMEOUT
    try:
        c = httplib.HTTPConnection( ip, port, timeout=timeout )
        c.request( 'GET', uri, headers=headers, body=body )
        resp = c.getresponse()
        if resp.status == 200:
            x = load( resp.read() )
            if x[1] is None:
                return x[0], None
            else:
                return x[0], Error( *x[1] )
        else:
            return None, Error( 'HTTPError', (resp.status, ) )
    except (socket.error, httplib.BadStatusLine) as e:
        logger.debug( repr( e ) + ' while send http to ' + repr( ( ip, port, uri ) ) )
        return None, Error( ERR[ 'NetworkError' ], ( (ip, port), ) )

    except Exception, e:
        logger.error( traceback.format_exc() )
        logger.error( repr( e ) )
        return None, Error( 'XXX', [] )


def dump( obj ):
    return json.dumps( obj, encoding='utf-8' )


def load( s ):
    if s is None:
        return None

    return json.loads( s, encoding='utf-8' )


def _cluster_normalize( cluster ):

    if cluster is None:
        return None

    if type( cluster ) in ( type([]), type( () ) ):
        cluster =  dict( [ ( x, { 'addrs':[ x ] } )
                           for x in cluster ] )

    rst = {}
    for k, v in cluster.items():
        if type( v ) in ( type( () ), type( [] ) ):
            rst[ k ] = { 'addrs': v }
        elif type( v ) in ( type( '' ), type( u'' ) ):
            rst[ k ] = { 'addrs': [ v ] }
        else:
            rst[ k ] = v

    cluster = rst
    for k, v in cluster.items():
        v[ 'addrs' ] = [ _addr_ipport( x )
                         for x in v[ 'addrs' ] ]
        v[ 'addrs' ].sort()

    return cluster

def _cluster_hash( cluster ):

    if cluster is None:
        return None

    cluster = _cluster_normalize( cluster )
    ks = cluster.keys()
    ks.sort()

    s = [ ( k, cluster[ k ][ 'addrs' ] )
          for k in ks ]

    s = [ repr( x ) for x in s ]
    s = ' '.join( s )
    return hashlib.md5( s ).hexdigest()[ :8 ]

def _addr_ipport( addr ):

    if type( addr ) == type( () ):
        return addr
    else:
        ip, port = addr.split( ":", 1 )
        port = int( port )
        return ip, port


def daemon_thread( f, args=(), argkv={} ):
    t = threading.Thread( target=f, args=args, kwargs=argkv )
    t.daemon = True
    t.start()
    return t

def _dict_merge( a, *b ):
    for x in b:
        a.update( x )
    return a
