#!/usr/bin/env python2.6
# coding: utf-8

import os, sys
import time
import copy
import traceback
import urllib
import threading
import Queue

import datatype
from datatype import *
import paxos
import log
import ftlogclient

# Queues:
#
#   local-write-queue
#
#       wait-p2-queue
#
#           p2-send-queue * n
#
#       wait-commit-queue
#
#           commit-queue * n

LogRecord = datatype.struct(
        'LogRecord',
        ( 'ver', None ),
        ( 'leader_ver', None ),
        ( 'data', None ),
)

class LogRequestHandler( paxos.SingleAcceptorHandler ):

    def do_GET( self ):
        rlog = self.server.rlog
        rlog.logger.debug( rlog.ident + ' received request: ' + repr(self.path) )
        r = paxos.SingleAcceptorHandler.do_GET( self )
        return r

    def resp_err( self, err ):
        err = Error( *err )
        return self.resp_udf_err( err.err, *err.val )

    def resp_udf_err( self, err, *vals ):
        self.resp( 200, ( None, ( err, tuple(vals) ) ) )

    def h_paxos( self ):
        rlog = self.server.rlog
        if rlog.pause_serve_elect:
            return self.resp_udf_err( 'PaxosDisabled' )

        return paxos.SingleAcceptorHandler.h_paxos( self )

    def h_iget( self ):

        # get internal info

        k = self.path.split( '/' )[ 2 ]
        k = urllib.unquote_plus( k )

        if k == '/leader':
            self.resp( 200, ( self.server.rlog.leader, None ) )
            return

        inst, err = self.server.rlog.sto.get_inst( (k, 1) )
        if inst.v is None:
            self.resp( 200, ( None, None ) )
        else:
            self.resp( 200, ( inst.v.data, None ) )


    def h_read( self ):

        # no need to guarantee it is still the same leader.  See README for
        # detailed explaination.
        rlog = self.server.rlog
        if rlog.pause_log_rw:
            return self.resp_udf_err( 'ServerDisabled', *rlog.status )

        rlog.logger.debug( rlog.ident + ' request read: ' + repr(self.req) )

        r, err = rlog._hdl_read( self.req )
        return self.resp( 200, (r, err) )


    def h_write( self ):

        rlog = self.server.rlog
        if rlog.pause_log_rw:
            return self.resp_udf_err( 'ServerDisabled', *rlog.status )


        leader = Leader( *rlog.leader )
        myident = rlog.ident

        with rlog.leader_lock:
            # if leader changes, no log should be written

            if leader.ident != myident:
                rlog.logger.debug( rlog.ident + ' is not leader: ' + repr( leader ) )
                self.resp_udf_err( 'NotLeader', leader, myident )
                return

            rec = LogRecord( 1, leader.ver, self.body )

            logbuf = paxos.dump( rec )
            (seq, next_seq), err = rlog.mylog.write( logbuf )

        # There is no more than 2 valid leader in system. Thus we do not need
        # to lock.

        p2req = paxos.dump( { 'leader': leader,
                              'seq': seq,
                              'rec': rec } )

        (n_acc, errs), err = rlog.send_mes( '/p2', p2req )
        n_acc += 1 # 1 for leader itself

        quorum = len(rlog.cluster) / 2 + 1
        if n_acc < quorum:
            self.resp_udf_err( 'QuorumError', *errs )
            return

        rlog.commit_nolock( seq, rec )

        commit_req = paxos.dump( { 'seq': seq,
                                   'next_seq': next_seq,
                                   'rec': rec } )

        (n_acc, errs), err = rlog.send_mes( '/commit', commit_req )
        self.resp( 200, ( ( seq, next_seq ), None ) )


    def h_p2( self ):

        req = self.req
        rlog = self.server.rlog
        if rlog.pause_log_rw:
            return self.resp_udf_err( 'ServerDisabled', *rlog.status )
        localleader = Leader(*rlog.leader)
        fromleader = Leader( *req[ 'leader' ] )
        rec = LogRecord( *req[ 'rec' ] )
        ver, leader_ver, v = rec

        if rlog.catchingup:
            self.resp_udf_err( 'CatchingUp' )
            return

        with rlog.leader_lock:

            if fromleader != localleader:
                self.resp_udf_err( 'WrongLeader', localleader, fromleader )
                return

            if rlog.p2_seq < req[ 'seq' ]:
                rlog.catchup_queue.put( ( localleader.ident, localleader ) )
                self.resp_udf_err( 'InvalidPhase2Seq', rlog.p2_seq, req[ 'seq' ] )
                return

            if req[ 'seq' ] == rlog.mylog.seq:
                logbuf = paxos.dump( rec )
                (seq, next_seq), err = rlog.mylog.write( logbuf )
                if err:
                    return self.resp_err( err )

                rlog.p2_seq = next_seq

                self.resp( 200, ( None, None ) )
                return

            if req[ 'seq' ] < rlog.mylog.seq:
                (myseq, mynext_seq, myrec), err = rlog.mylog.read( req[ 'seq' ] )
                if err:
                    rlog.logger.debug( repr( err ) + ' while  p2' )
                    return self.resp_err( err )
                else:
                    myrec = LogRecord( *paxos.load( myrec ) )
                    rlog.logger.debug( rlog.ident + ' p2 existent rec: ' + repr( rec ) )
                    rlog.logger.debug( rlog.ident + ' p2 existent my : ' + repr( myrec ) )
                    if myrec.leader_ver == rec.leader_ver:
                        self.resp( 200, ( None, None ) )
                        return

            rlog.catchup_queue.put( ( localleader.ident, localleader ) )
            rlog.logger.debug( 'incorrect seq:' + repr( ( rlog.mylog.seq, req[ 'seq' ] ) ) )
            self.resp_udf_err( 'IncorrectSeq', rlog.mylog.seq, req[ 'seq' ] )

    def h_commit( self ):

        # follower only

        rlog = self.server.rlog
        if rlog.pause_log_rw:
            return self.resp_udf_err( 'ServerDisabled', *rlog.status )
        req = self.req
        seq = req[ 'seq' ]
        rec = LogRecord( *req[ 'rec' ] )

        if rlog.catchingup:
            self.resp_udf_err( 'CatchingUp' )
            return

        # every commit message from any leader even from a previous leader, is
        # legal. but we still need to keep the order.
        with rlog.leader_lock:

            _, err = rlog.commit_nolock( seq, rec )

            if err:
                leader = rlog.leader
                if leader.ident is not None:
                    rlog.catchup_queue.put( ( leader.ident, leader ) )
                self.resp_err( err )
                return

            self.resp( 200, ( None, None ) )


class Server( paxos.SingleAcceptorServer ): handler_class = LogRequestHandler


class ReliableLog( paxos.PaxosBase ):

    server_class = Server

    def __init__( self, ident, cluster,
                  lease=2000, hostdir=None,
                  pause_elect=False,
                  pause_serve_elect=False,
                  pause_log_rw=False,
                  **argkv ):

        paxos.PaxosBase.__init__( self, ident, cluster, **argkv )

        if ident is not None and cluster is not None:
            assert ident in self.cluster

        self.lease = lease
        self.hostdir = hostdir or ('./ftlog-' + self.ident)

        if not os.path.isdir( self.hostdir ):
            os.makedirs( self.hostdir, mode=0755 )

        self.mylog = log.Log( hostdir=os.path.join(self.hostdir, 'logs'),
                              fsize=1024*1024, create=False )

        self.leader = paxos.Leader( None, 0 )
        self.expire = 1
        self.rlog_running = True
        self.pause_log_rw = pause_log_rw # debug purpose only
        self.pause_elect = pause_elect # debug purpose only
        self.pause_serve_elect = pause_serve_elect # debug purpose only

        self.leader_queue = Queue.Queue( 1024 )
        self.leader_queue.put( self.leader )
        self.catchup_queue = Queue.Queue( 1024 )
        self.catchingup = False

        self.status = ()

        self.p2_seq = 0
        self.commit_seq = 0

        self.leader_lock = threading.RLock()

        self.sto = paxos.FilePaxosStorage( os.path.join(self.hostdir, 'PAXOS') )

        self.acc = paxos.Acceptor( ident, self.cluster, self.sto )

        addr = self.cluster[ ident ][ 'addrs' ][ 0 ]
        self.srv = self.server_class( addr, self.acc )
        self.srv.rlog = self


    def init_paxos_sto( self ):
        self.sto.init()

    def fix_paxos_sto_if_corrupted( self ):

        if self.sto.corrupted:

            self.logger.info( self.ident + ' Paxos Storage corrupted, repairing...' )

            cluster = self.cluster.copy()
            del cluster[ self.ident ]

            rpc_proxy = paxos.make_rpc_proxy( cluster )

            repair = paxos.AcceptorRepair( self.acc, rpc_proxy=rpc_proxy,
                                           instids=[ (paxos.KEY[ 'leader' ], 1) ] )
            rc, err = repair.fix()
            if err:
                self.logger.error( self.ident + ' Paxos Storage repair failed: '
                              + repr( err ) )
                return rc, err

            self.logger.error( self.ident + ' Paxos Storage repair finished' )
            self.sto.init()

        return None, None

    def fix_log_sto_if_corrupted( self ):

        if not self.mylog.inited:
            self.logger.warn( self.ident + ' Log storage corrupted' )
            self.sto.set( 'sto_ready', False )

        sto_ready, err = self.sto.get( 'sto_ready' )
        sto_ready = sto_ready or False

        if not sto_ready:

            self.status = ( 'LogRepair', )

            if self.mylog.err:
                self.logger.warn( self.ident + ' Log storage repairing...' )
                rc, err = self.mylog.repair()
                if err:
                    self.logger.error( self.ident + ' Log storage repair failed:' + repr( err ) )
                    return None, err
                else:
                    self.logger.error( self.ident + ' Log storage repair finished' )
            else:
                self.logger.info( self.ident + ' Log storage init...' )
                rc, err = self.mylog.init()
                if err:
                    self.logger.error( self.ident + ' Log storage init failed:' + repr( err ) )
                    return None, err

            self.status = ( 'LogRepair', 'FindLeader' )

            while self.rlog_running:

                self.logger.info( self.ident + " Wait for leader to catch up with" )
                leader = self.get_sto_leader()

                if leader.ident is None:
                    time.sleep( 1 )
                    continue

                self.status = ( 'LogRepair', 'CatchupWithLeader', )
                self.logger.info( self.ident + " Got leader: " + repr( leader ) )
                rc, err = self.catchup( leader.ident, leader=leader )
                if err:
                    self.logger.warn( self.ident + ' Failure log repair catchup with: '
                                 + leader.ident + ': ' + repr( err ) )
                    time.sleep( 1 )
                else:
                    break

            self.sto.set( 'sto_ready', True )
            self.logger.info( self.ident + ' Log storage repair finished' )

        return None, None

    def start( self ):

        self.logger.info( self.ident + ' Starting...' )

        rc, err = self.fix_paxos_sto_if_corrupted()
        if err:
            return None, err

        self.pause_log_rw = True
        self.status = ('Startup', )
        self.srv_th = paxos.daemon_thread( self.srv.serve_forever )
        self.catchup_th = paxos.daemon_thread( self.catchup_worker )

        rc, err = self.fix_log_sto_if_corrupted()
        if err:
            return None, err

        commit_seq, err = self.sto.get( 'commit_seq' )

        if not err and commit_seq is not None:

            if commit_seq > self.mylog.seq:
                commit_seq  = self.mylog.seq

            self.commit_seq = commit_seq
            self.p2_seq = self.commit_seq


        self.pause_log_rw = False
        self.status = ()

        self.ldr_sw_th = paxos.daemon_thread( self.leader_switch_worker )
        self.cmt_th = paxos.daemon_thread( self.commit_worker )
        self.logger.info( self.ident + ' Starting up finished' )

        return None, None

    def get_sto_leader( self ):

        instid = paxos.InstanceId( paxos.KEY[ 'leader' ], 1 )
        inst, err = self.sto.get_inst( instid )

        if inst.v is None:
            leader = paxos.Leader( None, 0 )
        else:
            leader = paxos.Leader( *inst.v.data )
        return leader


    def elect( self, now, is_new ):

        instid = paxos.InstanceId( paxos.KEY[ 'leader' ], 1 )
        rpc_proxy = paxos.make_rpc_proxy( self.cluster )

        p = paxos.Proposer( self.ident, self.cluster,
                      instid, ( self.ident, now ),
                      lease=self.lease, rnd=( now, self.ident ) )

        resps = rpc_proxy( p.make_phase1_req() )

        v, err = p.choose_v( resps )
        if err: return None, err

        resps = rpc_proxy( p.make_phase2_req( v ) )

        rc, err = p.is_phase2_ok( resps )
        if err: return None, err

        return v, None


    def _hdl_read( self, kvs ):

        seq = kvs[ 'seq' ]
        if 'leader' in kvs:
            leader = Leader( *kvs[ 'leader' ] )
        else:
            leader = Leader( None, 0 )

        # accept request only when I am the expected leader

        if leader.ident is not None and leader != self.leader:
            return None, Error( 'IncorrectLeader', ( self.leader, leader ) )

        (seq, next_seq, buf), err = self.mylog.read( seq )

        if err:
            self.logger.debug( repr( err ) )
            return None, err

        rec = LogRecord( *paxos.load( buf ) )

        return (self.commit_seq,) + rec, None


    def serve_forever( self ):

        cur_leader = paxos.Leader( None, 0 )

        while self.rlog_running:

            if self.pause_elect:
                time.sleep( 0.1 )
                continue

            now = paxos.make_ts()
            new_leader = self.get_sto_leader()

            if new_leader.ident in ( None, self.ident ):

                v, err = self.elect( now, is_new=new_leader.ident is None )

                if err is None:
                    new_leader = paxos.Leader( *v.data )

            self.logger.debug( self.ident + ' leader chosen:' + repr( new_leader ) )
            if new_leader != cur_leader:
                self.leader_queue.put( new_leader )
                self.logger.info( self.ident + ' leader pushed to queue: ' + repr( new_leader ) )

            cur_leader = new_leader

            k = 'commit_seq'
            with self.sto.lock( k ):
                self.sto.set( k, self.commit_seq )

            if cur_leader.ident is None:
                time.sleep( 0.1 )
            else:
                time.sleep( 0.5 )


    def commit_worker( self ):

        while self.rlog_running:

            if self.pause_elect:
                time.sleep( 0.1 )
                continue

            if self.leader.ident == self.ident \
                    and self.mylog.seq > self.commit_seq:

                commit_seq = self.commit_seq
                time.sleep( 0.1 )
                if self.leader.ident == self.ident \
                        and commit_seq == self.commit_seq:

                    rc, err = self.reexec()
                    self.logger.info( self.ident + ' reexec_nolock err: ' + repr( err ) )

            time.sleep( 0.1 )


    def leader_switch_worker( self ):

        while self.rlog_running:

            try:
                leader = self.leader_queue.get( timeout=0.1 )
            except Queue.Empty, e:
                continue

            with self.leader_lock:
                self.p2_seq = self.commit_seq

            if leader.ident not in ( None, self.ident ):

                self.leader = leader
                self.catchup_queue.put( (leader.ident, leader) )

            elif leader.ident == self.ident:

                rc, err = self.leader_catchup()
                if err:
                    self.logger.error( self.ident + ' Leader Catch up failed:' + repr( err ) )
                    return None, err

                self.leader = leader

            else:
                self.leader = leader

    def catchup_worker( self ):

        while self.rlog_running:

            try:
                src_ident, leader = self.catchup_queue.get( timeout=0.1 )
            except Queue.Empty, e:
                continue

            rc, err = self.catchup( src_ident, leader )
            if err:
                time.sleep( 0.5 )


    def leader_catchup( self ):

        self.logger.info( self.ident + ' Start accumulate_catchup' )

        while self.rlog_running:
            rc, err = self.accumulate_catchup()
            if err:
                self.logger.warn( self.ident + ' Failure accumulate_catchup: ' + repr( err ) )
                time.sleep( 0.5 )
            else:
                self.logger.info( self.ident + ' OK accumulate_catchup' )
                return None, None

        return None, None

    def accumulate_catchup( self ):

        with self.leader_lock:

            n_finished = 0

            for ident, node in self.cluster.items():
                if ident == self.ident:
                    continue

                rc, err = self.catchup_nolock( ident, leader=None )
                if err:
                    self.logger.info( self.ident + ' Failure catchup with: ' + ident
                                 + ': ' + repr( err ) )
                else:
                    self.logger.info( self.ident + ' OK catchup with: ' + ident )
                    n_finished += 1

                n_quorum = len( self.cluster ) / 2 + 1


                # 1 for leader itself
                if n_finished + 1 >= n_quorum:
                    return None, None

            return None, paxos.Error( 'QuorumError', (
                    len( self.cluster ), n_quorum, n_finished ) )


    def catchup( self, src_ident, leader=None ):

        self.catchingup = True

        try:
            with self.leader_lock:

                if self.leader.ident == self.ident:
                    return None, paxos.Error( 'ImLeader', () )

                rc, err = self.catchup_nolock( src_ident, leader=leader )
                if err:
                    self.catchup_queue.put( ( src_ident, leader ) )
                return rc, err
        finally:
            self.catchingup = False


    def __del__( self ):
        self.shutdown()

    def shutdown( self ):

        self.logger.info( self.ident + ' shutting down...' )
        self.rlog_running = False

        self.srv.shutdown()
        self.logger.debug( self.ident + ' shutdown ok...' )
        self.srv.server_close()
        self.logger.debug( self.ident + ' server closed ok...' )

        self.logger.debug( self.ident + ' joining srv_th' )

        self.srv_th.join()
        self.logger.debug( self.ident + ' srv_th joined' )

        self.ldr_sw_th.join()
        self.logger.debug( self.ident + ' flr_th joined' )

        self.cmt_th.join()
        self.logger.debug( self.ident + ' ldr_th joined' )

        self.catchup_th.join()
        self.logger.debug( self.ident + ' catchup_th joined' )

        self.logger.info( self.ident + ' shutted down' )

    def reexec( self ):
        with self.leader_lock:
            return self.reexec_nolock()

    def reexec_nolock( self ):

        seq = self.commit_seq

        while self.rlog_running:

            (seq, next_seq, buf), err = self.mylog.read( seq )
            if err:
                if err.err == 'OutofRange':
                    self.logger.debug( repr( err ) + ' while read local log' )
                    return None, None
                else:
                    self.logger.error( repr( err ) + ' while read local log' )
                    return None, err

            rec = LogRecord( *paxos.load( buf ) )
            rc, err = self.reexcecute_rec_nolock( seq, next_seq, rec )
            self.logger.info( self.ident + ' re-exec result: ' + repr( (rc, err) ) )
            if err:
                self.logger.info( repr( err ) + ' while reexec' )
                # maybe not enough quorum
                return None, err

            seq = next_seq

        return None, None

    def reexcecute_rec_nolock( self, seq, next_seq, rec ):

        self.logger.debug( self.ident + ' Leader re-exe: ' + repr( ( seq, rec ) ) )

        leader = self.leader
        p2req = paxos.dump( { 'leader': leader,
                              'seq': seq,
                              'rec': rec } )

        (n_acc, errs), err = self.send_mes( '/p2', p2req )
        n_acc += 1 # 1 for leader itself


        quorum = len(self.cluster) / 2 + 1
        if n_acc < quorum:
            err = ( 'QuorumError', () )
            return None, err

        self.commit_nolock( seq, rec )

        commit_req = paxos.dump( { 'seq': seq,
                                   'next_seq': next_seq,
                                   'rec': rec } )

        (n_acc, errs), err = self.send_mes( '/commit', commit_req )

        return None, None


    def catchup_nolock( self, src_ident, leader=None ):

        self.logger.debug( self.ident + ' catch up with:' + repr( src_ident ) )

        assert src_ident != self.ident

        seq = self.commit_seq

        cl = ftlogclient.Client( cluster=self.cluster )
        rst = {
                'from': seq,
                'end': seq,
                'n': 0,
        }

        while True:

            resp, err = cl.read( seq, leader=leader, ident=src_ident )

            if err:
                if err.err == 'OutofRange':
                    self.logger.info( self.ident + ' OK catchup finished: ' + repr( rst ) )
                    return None, None
                else:
                    self.logger.info( self.ident + ' Failure catchup /read: ' + repr( err ) )
                    return None, err
            else:
                self.logger.debug( self.ident + ' OK /read: ' + repr( resp ) )

            commit_seq, ver, leader_ver, buf = resp

            # json.load results in unicode string
            buf = str( buf )
            rec = LogRecord( ver, leader_ver, buf )
            logbuf = paxos.dump( rec )

            if seq == self.mylog.seq:
                (_, next_seq), err = self.mylog.write( logbuf )
                if err:
                    return None, err
                self.p2_seq = next_seq

                self.logger.debug( self.ident + ' OK catch up new:' + repr( ( seq, logbuf ) ) )
                rst[ 'end' ] = next_seq
                rst[ 'n' ] += 1
            else:

                (seq, next_seq, mylogbuf), err = self.mylog.read( seq )
                if err:
                    return None, err
                if logbuf == mylogbuf:
                    self.logger.debug( self.ident + ' OK catch up existent:' + repr( ( seq, logbuf ) ) )
                    self.p2_seq = next_seq
                    rst[ 'end' ] = next_seq
                    rst[ 'n' ] += 1
                else:
                    myrec = LogRecord( *paxos.load( mylogbuf ) )

                    # not possible same leader different log
                    assert myrec.leader_ver != rec.leader_ver

                    if myrec.leader_ver < rec.leader_ver \
                            or leader is not None:

                        # When catching up with leader, safe to discard local
                        # log.  because all possible accepted instances must
                        # present on leader.

                        self.logger.info( self.ident + " remote log has higher "
                                     "leader_ver or is leader, reset mine: "
                                     + repr( (seq, rec, myrec) ) )

                        # Discard all log after this seq. Those log can not be
                        # chosen by paxos group.
                        self.mylog.reset( seq )

                        ( _, next_seq ), err = self.mylog.write( logbuf )
                        if err:
                            return None, err
                        self.p2_seq = next_seq
                        rst[ 'end' ] = next_seq
                        rst[ 'n' ] += 1
                        self.logger.debug( self.ident + ' OK catch up new:' + repr( ( seq, logbuf ) ) )

                    else:
                        # myrec.leader_ver > rec.leader_ver
                        self.logger.info( self.ident + " remote non-leader log "
                                     "has lower leader_ver, stop catchup: "
                                     + repr( (seq, rec, myrec) ) )

                        return None, None

            if commit_seq > self.commit_seq:
                rc, err = self.commit_nolock( seq, rec )
                if err:
                    return None, err

            seq = next_seq


    def commit_nolock( self, seq, rec ):

        self.logger.debug( self.ident + " to commit:" + repr( ( seq, rec ) ) )
        if self.commit_seq > seq:
            # already committed
            return None, None

        if self.commit_seq < seq:
            err = ( 'InvalidSeq', (self.commit_seq, seq) )
            self.logger.debug( self.ident + " " + repr( err ) + ' while commit: ' + repr( ( seq, rec ) ) )
            # there might be a commit message gets lost
            return None, err

        # if self.commit_seq == seq:

        (myseq, mynext_seq, myrecbuf), err = self.mylog.read( seq )
        if err:
            return None, err

        myrec = LogRecord( *paxos.load( myrecbuf ) )

        # if leader matches, not possible that data does not match.
        if rec.leader_ver == myrec.leader_ver:
            self.commit_seq = mynext_seq

            self.logger.debug( self.ident + ' committed:' + repr(rec) )
            return None, None

        else:
            self.logger.debug( self.ident + ' rec not the same, mine: ' + repr( myrec ) )
            self.logger.debug( self.ident + ' rec not the same, to commit: ' + repr( rec ) )
            return None, ( 'WrongLeader', () )


    def send_mes( self, uri, body ):

        n_acc = 0
        errs = []
        for ident, node in self.cluster.items():

            if ident in self.ident:
                continue

            ip, port = node[ 'addrs' ][ 0 ]
            resp, err = paxos.http( ip, port, uri, body=body )
            self.logger.debug( 'From ' + ident
                          + ' ' + uri
                          + ' resp: ' + repr( ( resp, err ) ) )
            if err is None:
                n_acc += 1
            else:
                errs.append( ( ident, err ) )

        self.logger.debug( 'n_acc:' + repr(n_acc) )
        return (n_acc, errs), None

if __name__ == "__main__":

    import logging

    # paxos.logger.setLevel( logging.DEBUG )
    paxos.logger.setLevel( logging.INFO )

    ident = sys.argv[ 1 ]
    init = len(sys.argv)>2 and sys.argv[ 2 ] == 'init'

    cluster = { 'a': '127.0.0.1:8801',
                'b': '127.0.0.1:8802',
                'c': '127.0.0.1:8803', }

    rl = ReliableLog( ident, cluster, logger=paxos.logger )
    if init:
        rl.init_paxos_sto()
        rl.sto.set( 'sto_ready', True )

        rc, err = rl.mylog.init( create=True )
        if err:
            raise Exception( 'rlog init failure: ' + repr( err ) )
    else:
        rc, err = rl.start()
        if err:
            raise Exception( 'rlog startup failure: ' + repr( err ) )

        rl.serve_forever()
