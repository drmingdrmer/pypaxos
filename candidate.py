#!/usr/bin/env python2.6
# coding: utf-8

import os, sys
import time
import paxos


class Candidate( paxos.PaxosBase ):

    server_class = paxos.SingleAcceptorServer

    def __init__( self, ident, cluster,
                  lease=2000, fpref='./tmp-',
                  **argkv ):

        paxos.PaxosBase.__init__( self, ident, cluster, **argkv )
        if ident is not None and cluster is not None:
            assert ident in self.cluster

        self.lease = lease
        self.leader = paxos.Leader( None, 0 )
        self.leader_rst = None
        self.expire = 1
        self.elect_running = True

        self.sto = paxos.FilePaxosStorage( fpref + str( ident ) )
        if self.sto.corrupted:
            time.sleep( paxos.TIME_WINDOW * 2 / 1000.0 )
            self.sto.init()

        addr = self.cluster[ ident ][ 'addrs' ][ 0 ]
        self.srv = self.server_class( addr,
                                      paxos.Acceptor( ident,
                                                self.cluster,
                                                self.sto ), )

        self.srv_th = paxos.daemon_thread( self.srv.serve_forever )
        self.elect_th = paxos.daemon_thread( self.elect_forever )

    def as_leader( self, block=True ):

        while True:
            now = paxos.make_ts()
            if self.expire > now:
                return self.leader.ident, self.leader.ver, self.expire - now
            elif block:
                time.sleep( 0.2 )
            else:
                return None, 0, 0

    def elect_forever( self ):

        instid = paxos.InstanceId( paxos.KEY[ 'leader' ], 1 )

        while self.elect_running:

            now = paxos.make_ts()

            inst, err = self.sto.get_inst( instid )
            vlocal = inst.v

            if vlocal is None or vlocal.data[0] == self.ident:
                rst = {}
                v, err = paxos.classic( self.ident, self.cluster,
                                        instid, ( self.ident, now ),
                                        lease=self.lease,
                                        rnd=( now, self.ident ),
                                        rst=rst )

                if err is None:
                    self.leader = paxos.Leader(*v.data)
                    if self.leader.ident == self.ident:
                        if vlocal is None:
                            self.leader_rst = rst
                        # 'now' is before the time leadership actually
                        # established.
                        self.expire = now + self.lease
                    vlocal = v

            if vlocal is not None:
                self.leader = paxos.Leader(*vlocal.data)
            else:
                self.leader = paxos.Leader( None, 0 )

            time.sleep( 0.5 )

    def __del__( self ):
        self.shutdown()

    def shutdown( self ):
        self.elect_running = False
        self.srv.shutdown()
