#!/usr/bin/env python2.6
# coding: utf-8


import os, sys
import time
import socket
import unittest

import testframe
import paxos
import candidate

dd = testframe.dd

i = 0
ts_i = 0

def ts():
    global ts_i
    ts_i += 1
    return int( time.time()*1000 ) + ts_i

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


class TestCandidate( Base ):

    def setUp( self ):
        self._make_cluster( 3 )
        self.candidates = {}
        for ident in self.cluster:
            self.candidates[ ident ] = candidate.Candidate( ident, self.cluster, lease=2000 )

        time.sleep( 0.2 )

    def test_candidate( self ):

        for ii in range( 3 ):

            leaders = []
            rst = None
            ver = None

            for ident, cand in self.candidates.items():
                leader, version, life = cand.as_leader( block=False )
                if leader is not None:
                    leaders.append( leader )
                    rst = cand.leader_rst
                    ver = version


            dd( "found leader:" + repr( leaders ) )
            self.eq( 1, len( leaders ), 'only one leader' )
            self.eq( ver, rst[ 'vrnd' ][ 0 ],
                     'vrnd with which leader established is leader version' )
            time.sleep( 0.3 )

        self.candidates[ leaders[ 0 ] ].close()
        del self.candidates[ leaders[ 0 ] ]

        time.sleep( 4 )

        for ii in range( 3 ):

            leaders = []

            for ident, cand in self.candidates.items():
                leader, version, life = cand.as_leader( block=False )
                if leader is not None:
                    leaders.append( leader )

            dd( "found leader:" + repr( leaders ) )
            self.eq( 1, len( leaders ), 'only one leader' )
            time.sleep( 0.2 )

if __name__ == "__main__":

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    classes = (
            TestCandidate,
    )

    for clz in classes:
        suite.addTests( loader.loadTestsFromTestCase( clz ) )

    testframe.TestRunner(verbosity=2).run(suite)
