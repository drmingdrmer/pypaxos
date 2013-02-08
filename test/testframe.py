#!/usr/bin/env python2.6
# coding: utf-8


import os, sys
import unittest
import traceback
import shutil
import re

_DEBUG_ = False

# stop on any error or failure
class MyTestResult( unittest._TextTestResult ):
    def addError( self, case, exc ):
        super( MyTestResult, self ).addError( case, exc )
        self.stop()

    def addFailure( self, case, exc ):
        super( MyTestResult, self ).addFailure( case, exc )
        self.stop()

class TestRunner( unittest.TextTestRunner ):
    def _makeResult(self):
        return MyTestResult(self.stream, self.descriptions, self.verbosity)

class TestFrame( unittest.TestCase ):

    def setUp( self ):
        pass

    def tearDown( self ):
        pass

    def setup_case_env( self ):
        pass

    def cleanup_case_env( self ):
        pass

    def eq( self, exp, act, msg = '' ):
        self.assertEqual( exp, act, msg + ' Expected: ' + repr( exp ) + ' Actual: ' + repr( act ) )

    def true( self, act, msg = '' ):
        self.assertTrue( act, msg + ' Expected to be True: ' + repr( act ) )

    def false( self, act, msg = '' ):
        self.assertTrue( not act, msg + ' Expected to be False: ' + repr( act ) )

    def neq( self, exp, act, msg = '' ):
        self.assertNotEqual( exp, act, msg + ' Expected not to be: ' + repr( exp ) + ' Actual: ' + repr( act ) )

    def assert_dict_match( self, expected, v, message = '' ):
        expKeys = set( expected.keys() )
        vkeys = set( v.keys() )

        if expKeys - vkeys != set():
            self.fail( 'lack of keys:' + str( expKeys - vkeys ) +\
                    ' ' + str( vkeys ) + ' ' + message )

        for ( key, rex ) in expected.items():
            # if not v.haskey( key ):
                # self.fail( 'expect key but' )
            if not re.match( rex, str(v[ key ]) ):
                self.fail( 'unmatched key=%s expect=%s but=%s in test %s' % ( key, rex, v[ key ], message ) )

def runtest( clz ):
    suite = unittest.TestLoader().loadTestsFromTestCase( clz )
    TestRunner(verbosity=2).run(suite)


def run_single( clz, methodname ):
    return run_suite( clz, methodname )


def run_suite( clz, *names ):
    suite = unittest.TestSuite( map( clz, names ) )
    TestRunner( verbosity = 2 ).run( suite )


def dd( *msg ):
    if not _DEBUG_:
        return

    os.write( 1, "\n>>> " )
    for m in msg:
        os.write( 1, str( m ) + ' ' ),
    os.write( 1, "\n" )


import getopt
optlist, params = getopt.gnu_getopt(
        sys.argv[ 1: ],
        'v'
)
opts = dict( optlist )

if '-v' in opts:
    _DEBUG_ = True


if __name__ == "__main__":

    import test.test_paxos
    import test.test_rpc

    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    for mod in ( test.test_paxos,
                 test.test_rpc,
    ):
        suite.addTests( loader.loadTestsFromModule( mod ) )

    TestRunner(verbosity=2).run(suite)
