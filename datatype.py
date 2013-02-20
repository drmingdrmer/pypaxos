#!/usr/bin/env python2.6
# coding: utf-8

from collections import namedtuple

def struct( name, *fields ):
    # fields = [ ( 'name', Type ), ... ]

    fields = [ (n, t) for n, t in fields ]
    typedic = dict( fields )

    names = ' '.join( [ x[0] for x in fields ] )

    clz = namedtuple( name, names )
    class clz_wrapper( clz ):
        def __new__( _clz, *args, **argkv ):
            lft = clz._fields[ len( args ): ]
            args += tuple( [ argkv[ k ] for k in lft ] )

            _args = []
            i = 0
            for fieldname in clz._fields:
                a = args[ i ]
                caster = typedic.get( fieldname )
                if caster is not None \
                        and a is not None:
                    a = caster( *a )
                _args.append( a )
                i += 1

            return clz.__new__( _clz, *_args )

        @classmethod
        def _compatible( _clz, v ):

            if isinstance( v, _clz ):
                return True

            return type( v ) in ( type( () ), type( [] ) ) \
                    and len( v ) == len( clz._fields )

    return clz_wrapper


Leader = struct(
        'Leader',
        ( 'ident', None ),
        ( 'ver', None ), )

Error = struct(
        'Error',
        ( 'err', None ),
        ( 'val', None ), )

Rnd = struct(
        'Rnd',
        ( 'ts', None ),
        ( 'prp_id', None ), )

Value = struct(
        'Value',
        ( 'prp_id', None ),
        ( 'data', None ),
        ( 'lease', None ), )

ValueVersion = struct(
        'ValueVersion',
        ( 'v', Value ),
        ( 'ver', None ), )

InstanceId = struct(
        'InstanceId',
        ( 'key', None ),
        ( 'ver', None ), )

Instance = struct(
        'Instance',
        ( 'v', Value ),
        ( 'vrnd', Rnd ),
        ( 'last_rnd', Rnd ),
        ( 'expire', None ), )

StateMachineId = struct(
        'StateMachineId',
        ( 'group_id', None ),
        ( 'node_id', None ), )

StateMachine = struct(
        'StateMachine',
        ( 'statemachine_id', StateMachineId ),
        ( 'storage', None ),
        ( 'acceptor', None ), )

SingleVersionRecord = struct(
        'SingleVersionRecord',
        ( 'committed', ValueVersion ),
        ( 'instance', Instance ), )

Group = struct(
        'Group',
        ( 'quorum', None ),
        ( 'cluster', None ), )
