#!/usr/bin/env python2.6
# coding: utf-8

if __name__ == "__main__":

    import sys, time, paxos

    candidate = paxos.Candidate( sys.argv[1], { 'a': '127.0.0.1:8801',
                                                'b': '127.0.0.1:8802',
                                                'c': '127.0.0.1:8803', } )

    while True:
        print 'leader, version       :', candidate.leader
        time.sleep( 0.5 )
