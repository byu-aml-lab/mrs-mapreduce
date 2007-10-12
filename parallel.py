#!/usr/bin/env python

PING_INTERVAL = 5.0

def run_master(inputs, output, options):
    """Mrs Master
    """
    map_tasks = options.map_tasks
    if map_tasks == 0:
        map_tasks = len(inputs)
    if reduce_tasks == 0:
        reduce_tasks = 1
    if options.map_tasks != len(inputs):
        raise NotImplementedError("For now, the number of map tasks "
                "must equal the number of input files.")

    import sys
    import master, rpc

    master_rpc = master.MasterRPC()
    rpc_thread = rpc.RPCThread(master_rpc, options.port)
    rpc_thread.start()

    port = rpc_thread.server.server_address[1]
    print >>sys.stderr, "Listening on port %s" % port

    from time import sleep
    sleep(300)

    return 0

def run_slave(mapper, reducer, uri, options):
    """Mrs Slave

    The uri is of the form scheme://username:password@host/target with
    username and password possibly omitted.
    """
    import slave, rpc
    import select, xmlrpclib

    # Create an RPC proxy to the master's RPC Server
    master = xmlrpclib.ServerProxy(uri)

    # Start up a worker thread.  This thread will die when we do.
    worker = slave.Worker()
    worker.start()

    # Startup a slave RPC Server
    slave_rpc = slave.SlaveRPC(worker)
    server = rpc.new_server(slave_rpc, options.port)
    server_fd = server.fileno()
    host, port = server.server_address

    # Register with master.
    if not master.signin(slave_rpc.cookie, port):
        import sys
        print >>sys.stderr, "Master rejected signin."
        return -1

    while slave_rpc.alive:
        rlist, wlist, xlist = select.select([server_fd], [], [], PING_INTERVAL)
        if server_fd in rlist:
            server.handle_request()
        else:
            # try to ping master
            try:
                # TODO: consider setting socket.setdefaulttimeout()
                master_alive = master.ping()
            except:
                master_alive = False
            if not master_alive:
                print >>sys.stderr, "Master failed to respond to ping."
                return -1
    return 0

# vim: et sw=4 sts=4
