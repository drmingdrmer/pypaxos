#!/usr/bin/env python2.6
# coding: utf-8

import socket
import httplib
import urllib
import paxos
from datatype import *

class Client( paxos.PaxosBase ):

    def __init__( self, cluster, **argkv ):
        super( Client, self ).__init__( None, cluster, **argkv )
        self.leader = Leader( None, 0 )
        self.lease = 0

    def get_node( self, ident ):
        ident = ident or self.leader.ident
        ident = ident or self.cluster.keys()[ 0 ]
        return self.cluster[ ident ]

    def get_addr( self, ident ):
        node = self.get_node( ident )
        return node[ 'addrs' ][ 0 ]

    def http( self, args, ident=None, **argkv ):

        for ii in range( 3 ):

            full_args = self.get_addr( ident ) + args
            r, err = paxos.http( *full_args, timeout=0.5, **argkv )

            if err and err.err in ('NotLeader', 'NetworkError') and ident is None:
                _, _err = self._fetch_leader()
                if _err:
                    return r, err

                continue
            else:
                return r, err

        return r, err

    def _fetch_leader( self ):

        for ident in self.cluster:

            r, err = self.iget( '/leader', ident=ident )
            if err:
                continue
            else:
                leader = Leader( *r )
                if leader.ident is not None:
                    self.leader = leader
                    return None, None

        return None, Error( 'LeaderNotFound', () )

    def read( self, seq, leader=None, ident=None ):

        req = { 'seq':seq }
        if leader is not None:
            req[ 'leader' ] = leader
        bd = paxos.dump( req )
        return self.http( ( '/read', ), body=bd, ident=ident )

    def write( self, string, ident=None ):
        return self.http( ( '/write', ), body=str( string ), ident=ident )

    def iget( self, k, ident=None ):
        args = ( '/iget/'+escape( k ), )
        return self.http( args, ident=ident )


def escape( s ):
    return urllib.quote_plus( s )

if __name__ == "__main__":

    import sys
    addr = sys.argv[ 1 ]
    c = Client( { 0:addr } )

    func = getattr( c, sys.argv[ 2 ] )
    print func( *sys.argv[ 3: ], ident=0 )
