#!/usr/bin/env python2.6
# coding: utf-8

if __name__ == "__main__":

    import sys, time, paxos

    candidate = paxos.Candidate( sys.argv[1], { 'a': '127.0.0.1:8801',
                                                'b': '127.0.0.1:8802',
                                                'c': '127.0.0.1:8803', } )

    while True:
        leader, ver, lease = candidate.as_leader( block=True )
        print 'leader, version, lease:', leader, ver, lease
        time.sleep( 0.5 )
