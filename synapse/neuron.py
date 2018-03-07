import os
import sys
import fcntl
import random
import socket
import logging
import itertools
import threading
import collections
import multiprocessing

import synapse.glob as s_glob
import synapse.exc as s_exc
import synapse.common as s_common
import synapse.dyndeps as s_dyndeps
import synapse.eventbus as s_eventbus

import synapse.lib.kv as s_kv
import synapse.lib.net as s_net
import synapse.lib.config as s_config
import synapse.lib.msgpack as s_msgpack
import synapse.lib.threads as s_threads

import synapse.lib.crypto.ecc as s_ecc
import synapse.lib.crypto.vault as s_vault
import synapse.lib.crypto.tinfoil as s_tinfoil

logger = logging.getLogger(__name__)

defport = 65521 # the default neuron port

NEURON_PROTO_VERSION = (1, 0)

class SessBoss:
    '''
    Mixin base class for session managers.
    '''
    def __init__(self, auth, roots=()):

        self._boss_auth = auth

        self.roots = list(roots)

        root = s_vault.Cert.load(auth[1].get('root'))
        self.roots.append(root)

        self._my_static_prv = s_ecc.PriKey.load(auth[1].get('ecdsa:prvkey'))

        self.cert = s_vault.Cert.load(auth[1].get('cert'))
        self.certbyts = self.cert.dump()

    def valid(self, cert):

        if not any([r.signed(cert) for r in self.roots]):
            return False

        tock = cert.tokn.get('expires')
        if tock is None:
            logger.warning('SessBoss: cert has no "expires" value')
            return False

        tick = s_common.now()
        if tock < tick:
            logger.warning('SessBoss: cert has expired')
            return False

        return True

class Cell(s_config.Configable, s_net.Link, SessBoss):
    '''
    A Cell is a micro-service in a neuron cluster.

    Args:
        dirn (str): Path to the directory backing the Cell.
        conf (dict): Configuration data.
    '''
    _def_port = 0

    def __init__(self, dirn, conf=None):

        s_net.Link.__init__(self)
        s_config.Configable.__init__(self)

        self.dirn = dirn
        s_common.gendir(dirn)

        # config file in the dir first...
        self.loadConfPath(self._path('config.json'))
        if conf is not None:
            self.setConfOpts(conf)

        self.reqConfOpts()

        self.plex = s_net.Plex()
        self.kvstor = s_kv.KvStor(self._path('cell.lmdb'))
        self.kvinfo = self.kvstor.getKvDict('cell:info')

        # open our vault
        self.vault = s_vault.Vault(self._path('vault.lmdb'))
        self.root = self.vault.genRootCert()

        # setup our certificate and private key
        auth = self._genSelfAuth()
        roots = self.vault.getRootCerts()
        SessBoss.__init__(self, auth, roots)

        self.cellinfo = {}
        self.cellauth = auth
        self.cellpool = None
        self.celluser = CellUser(auth, roots=roots)

        addr = self.getConfOpt('bind')
        port = self.getConfOpt('port')

        def onlink(link):
            sess = CellSess(link, self)

            link.onrx(sess.rx)

            # fini cuts both ways
            sess.onfini(link.fini)
            link.onfini(sess.fini)

        addr, port = self.plex.listen((addr, port), onlink)

        host = self.getConfOpt('host')
        self.celladdr = (host, port)

        # add it to our neuron reg info...
        self.cellinfo['addr'] = self.celladdr

        # lock cell.lock
        self.lockfd = s_common.genfile(self._path('cell.lock'))

        try:
            fcntl.lockf(self.lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            logger.exception('Failed to obtain lock for [%s]', self.lockfd.name)
            raise

        self.onfini(self._onCellFini)
        self.onfini(self.finiCell)

        self.neuraddr = self.cellauth[1].get('neuron')
        if self.neuraddr is not None:
            self.cellpool = CellPool(auth, self.neuraddr, neurfunc=self._onNeurSess)
            self.onfini(self.cellpool.fini)

        # Give implementers the chance to hook into the cell
        self.postCell()

        logger.debug('Cell is done initializing')

    def _onNeurSess(self, sess):

        def retn(ok, retn):

            if not ok:
                logger.warning('%s cell:reg %r' % (self.__class__.__name__, ok))

            # either way, try again soon...
            if not sess.isfini:
                s_glob.sched.insec(60, cellreg)

        def cellreg():

            if sess.isfini:
                return

            sess.callx(('cell:reg', self.cellinfo), retn)

        cellreg()

    def _genCellName(self, name):
        return name

    def _genSelfAuth(self):

        path = self._path('cell.auth')
        if os.path.isfile(path):
            with open(path, 'rb') as fd:
                return s_msgpack.un(fd.read())

        name = self._genCellName('root')
        root = self.vault.genUserAuth(name)
        with open(path, 'wb') as fd:
            fd.write(s_msgpack.en(root))

        path = self._path('user.auth')

        name = self._genCellName('user')
        user = self.vault.genUserAuth(name)
        with open(path, 'wb') as fd:
            fd.write(s_msgpack.en(user))

        return root

    def _onCellFini(self):
        self.plex.fini()
        self.kvstor.fini()
        self.vault.fini()
        self.lockfd.close()

    def postCell(self):
        '''
        Module implementers may over-ride this method to initialize the cell
        *after* the configuration data has been loaded.

        Returns:
            None
        '''
        pass

    def finiCell(self):
        '''
        Module implementors may over-ride this method to automatically tear down
        resources created during postCell().
        '''
        pass

    def handlers(self):
        '''
        Module implementors may over-ride this method to provide the
        ``<mesg>:<func>`` mapping required for the Cell link layer.

        Returns:
            dict: Dictionary mapping endpoints to functions.
        '''
        return {
            'cell:ping': self._onCellPing,
        }

    def genUserAuth(self, name):
        '''
        Generate an auth blob that is valid for this Cell.

        Args:
            name (str): Name of the user to generate the auth blob for.

        Returns:
            ((str, dict)): A user auth tufo.
        '''
        return self.vault.genUserAuth(name)

    def getCellAddr(self):
        '''
        Return a (host, port) address tuple for the Cell.
        '''
        return self.celladdr

    def getCellAuth(self):
        '''
        Return the auth structure for this Cell.

        Returns:
            ((str,dict)): Auth tufo for this Cell.
        '''
        return self.cellauth

    def getRootCert(self):
        '''
        Get the root certificate for the cell.

        Returns:
            s_vault.Cert: The root Cert object for the cell.
        '''
        return self.root

    def getCellDict(self, name):
        '''
        Get a KvDict with a given name.

        Args:
            name (str): Name of the KvDict.

        Notes:
            Module implementers may use the ``getCellDict()`` API to get
            a KvDict object which acts like a Python dictionary, but will
            persist data across process startup/shutdown.  The keys and
            values are msgpack encoded prior to storing them, allowing the
            persistence of complex data structures.

        Returns:
            s_kv.KvDict: A persistent KvDict.
        '''
        return self.kvstor.getKvDict('cell:dict:' + name)

    def getCellSet(self, name):
        '''
        Get a KvList with a given name.
        '''
        return self.kvstor.getKvSet('cell:set:' + name)

    def _onCellPing(self, chan, mesg):
        data = mesg[1].get('data')
        chan.txfini(data)

    def _path(self, *paths):
        '''
        Join a path relative to the cell persistence directory.
        '''
        return os.path.join(self.dirn, *paths)

    def getCellPath(self, *paths):
        '''
        Get a file path underneath the underlying Cell path.

        Args:
            *paths: Paths to join together.

        Notes:
            Does not protect against path traversal.
            This does not make any required paths.

        Returns:
            str: Path under the cell
        '''
        return os.path.join(self.dirn, 'cell', *paths)

    def getCellDir(self, *paths):
        '''
        Get (and make) a directory underneath the underlying Cell path.

        Args:
            *paths: Paths to join together

        Notes:
            Does not protect against path traversal.

        Returns:
            str: Path under the cell
        '''
        return s_common.gendir(self.dirn, 'cell', *paths)

    def initConfDefs(self):
        self.addConfDefs((
            ('ctor', {
                'ex': 'synapse.cells.axon',
                'doc': 'The path to the cell constructor'}),

            ('bind', {'defval': '0.0.0.0', 'req': 1,
                'doc': 'The IP address to bind'}),

            ('host', {'defval': socket.gethostname(),
                'ex': 'cell.vertex.link',
                'doc': 'The host name used to connect to this cell. This should resolve over DNS. Defaults to the result of socket.gethostname().'}),

            ('port', {'defval': 0,
                'doc': 'The TCP port the Cell binds to (defaults to dynamic)'}),
        ))

class Neuron(Cell):
    '''
    A neuron node is the "master cell" for a neuron cluster.
    '''
    def postCell(self):
        self.cells = self.getCellDict('cells')

    def handlers(self):
        return {
            'cell:get': self._onCellGet,
            'cell:reg': self._onCellReg,
            'cell:init': self._onCellInit,
            'cell:list': self._onCellList,
        }

    def _genCellName(self, name):
        host = self.getConfOpt('host')
        return '%s@%s' % (name, host)

    def _onCellGet(self, chan, mesg):
        name = mesg[1].get('name')
        info = self.cells.get(name)
        chan.txfini((True, info))

    @s_glob.inpool
    def _onCellReg(self, chan, mesg):

        peer = chan.getLinkProp('cell:peer')
        if peer is None:
            enfo = ('NoCellPeer', {})
            chan.tx((False, enfo))
            return

        info = mesg[1]

        self.cells.set(peer, info)
        self.fire('cell:reg', name=peer, info=info)

        logger.info('cell registered: %s %r', peer, info)

        chan.txfini((True, True))
        return

    def _onCellList(self, chan, mesg):
        cells = self.cells.items()
        chan.tx((True, cells))

    @s_glob.inpool
    def _onCellInit(self, chan, mesg):

        # for now, only let root provision...
        root = 'root@%s' % (self.getConfOpt('host'),)

        peer = chan.getLinkProp('cell:peer')
        if peer != root:
            logger.warning('cell:init not allowed for: %s' % (peer,))
            return chan.tx((False, None))

        name = mesg[1].get('name').split('@')[0]
        auth = self.genCellAuth(name)
        chan.tx((True, auth))

    def getCellInfo(self, name):
        '''
        Return the info dict for a given cell by name.
        '''
        return self.cells.get(name)

    def getCellList(self):
        '''
        Return a list of (name, info) tuples for the known cells.
        '''
        return self.cells.items()

    def genCellAuth(self, name):
        '''
        Generate or retrieve an auth/provision blob for a cell.

        Args:
            name (str): The unqualified cell name (ex. "axon00")
        '''
        host = self.getConfOpt('host')
        full = '%s@%s' % (name, host)

        auth = self.vault.genUserAuth(full)

        auth[1]['neuron'] = self.getCellAddr()

        return auth

    def initConfDefs(self):
        Cell.initConfDefs(self)
        self.addConfDefs((
            ('port', {'defval': defport, 'req': 1,
                'doc': 'The TCP port the Neuron binds to (defaults to %d)' % defport}),
        ))


class CryptSeq:
    '''
    Applies and verifies sequence numbers of encrypted messages coming and going
    '''
    def __init__(self, rx_key, tx_key, initial_rx_seq=0, initial_tx_seq=0):
        self._rx_tinh = s_tinfoil.TinFoilHat(rx_key)
        self._tx_tinh = s_tinfoil.TinFoilHat(tx_key)
        self._rx_sn = itertools.count(initial_rx_seq)
        self._tx_sn = itertools.count(initial_tx_seq)

    def encrypt(self, mesg):
        seqn = next(self._tx_sn)
        rv = self._tx_tinh.enc(s_msgpack.en((seqn, mesg)))
        return rv

    def decrypt(self, ciphertext):

        plaintext = self._rx_tinh.dec(ciphertext)
        if plaintext is None:
            logger.error('Message decryption failure')
            raise s_exc.CryptoErr(mesg='Message decryption failure')

        seqn = next(self._rx_sn)

        sn, mesg = s_msgpack.un(plaintext)
        if sn != seqn:
            logger.error('Message out of sequence: got %d expected %d', sn, seqn)
            raise s_exc.CryptoErr(mesg='Message out of sequence', expected=seqn, got=sn)

        return mesg

class Sess(s_net.Link):
    '''
    Manages network session establishment and maintainance

    We use NIST SP 56A r2 "C(2e, 2s, ECC DH)", a scheme where both parties have 2 key pairs:  static and ephemeral.

    Sequence diagram U: initiator, V: listener, Ec:  public ephemeral initiator key, ec: private ephemeral initiator

    U -> V:  Ec, initiator cert
    V -> U:  Es, listener cert, encrypted message ("helo")

    The first encrypted message is sent in order to as quickly as possible identify a failure.
    '''

    def __init__(self, link, boss, lisn=False):

        s_net.Link.__init__(self, link)
        self.chain(link)
        self._sess_boss = boss
        self.is_lisn = lisn    # True if we are the listener.
        self._crypter = None  # type: CryptSeq
        self._my_ephem_prv = None  # type: s_ecc.PriKey
        self._tx_lock = threading.Lock()

    def handlers(self):
        return {
            'helo': self._onMesgHelo,
            'xmit': self._onMesgXmit,
            'fail': self._onMesgFail
        }

    def _tx_real(self, mesg):

        if self._crypter is None:
            raise s_exc.NotReady(mesg='Crypter not set')

        with self._tx_lock:
            data = self._crypter.encrypt(mesg)
            self.link.tx(('xmit', {'data': data}))

    def _onMesgFail(self, link, mesg):
        logger.error('Remote peer issued error: %r.', mesg)
        self.txfini()

    def _send_fail(self, exc):
        self.link.tx(('fail', {'exception': repr(exc)}))

    def _onMesgXmit(self, link, mesg):

        if self._crypter is None:
            logger.warning('xmit message before session establishment complete')
            raise s_common.NotReady()

        ciphertext = mesg[1].get('data')
        try:
            newm = self._crypter.decrypt(ciphertext)
        except Exception as e:
            self._send_fail(s_common.getexcfo(e))
            logger.exception('decryption')
            self.txfini()
            return

        try:
            self.taskplex.rx(self, newm)
        except Exception as e:
            self._send_fail(s_common.getexcfo(e))
            logger.exception('xmit taskplex error')
            self.txfini()

    @s_glob.inpool
    def _initiateSession(self):
        '''
        (As the initiator) start a new session

        Send ephemeral public and my certificate
        '''
        if self.is_lisn:
            raise Exception('Listen link cannot initiate a session')
        self._my_ephem_prv = s_ecc.PriKey.generate()
        self.link.tx(('helo', {'version': NEURON_PROTO_VERSION,
                               'ephem_pub': self._my_ephem_prv.public().dump(),
                               'cert': self._sess_boss.certbyts}))

    def _handSessMesg(self, mesg):
        '''
        Validate and set up the crypto from a helo message
        '''
        if self._crypter is not None:
            raise s_exc.ProtoErr('Received two client helos')

        if self.is_lisn:
            self._my_ephem_prv = s_ecc.PriKey.generate()

        version = mesg[1].get('version')
        if version != NEURON_PROTO_VERSION:
            raise s_exc.ProtoErr('Found peer with missing or incompatible version')

        peer_cert = s_vault.Cert.load(mesg[1].get('cert'))
        peer_ephem_pub = s_ecc.PubKey.load(mesg[1].get('ephem_pub'))

        if not self._sess_boss.valid(peer_cert):
            clsn = self.__class__.__name__
            raise s_exc.CryptoErr(mesg='%s got bad cert (%r)' % (clsn, peer_cert.iden(),))

        peer_static_pub = s_ecc.PubKey.load(peer_cert.tokn.get('ecdsa:pubkey'))
        km = s_ecc.doECDHE(self._my_ephem_prv, peer_ephem_pub,
                           self._sess_boss._my_static_prv, peer_static_pub, info=b'session')

        to_initiator_symkey, to_listener_symkey = km[:32], km[32:]

        if self.is_lisn:
            self._crypter = CryptSeq(to_listener_symkey, to_initiator_symkey)
        else:
            self._crypter = CryptSeq(to_initiator_symkey, to_listener_symkey)
            # Decrypt the first i.e. test message
            first_msg_ct = mesg[1].get('first_mesg')
            self._crypter.decrypt(first_msg_ct)

        return peer_cert

    @s_glob.inpool
    def _onMesgHelo(self, link, mesg):
        '''
        Handle receiving the session establishment message from the peer.

        send back our ephemerical public, our cert, and, if the listener, an encrypted message
        '''
        try:
            peer_cert = self._handSessMesg(mesg)
        except Exception as e:
            logger.exception('Exception encountered handling session message.')
            self._send_fail(s_common.getexcfo(e))
            self.txfini()
            return

        if self.is_lisn:
            # This would be a good place to stick version or info stuff
            first_message = {}
            with self._tx_lock:
                self.link.tx(('helo', {'version': NEURON_PROTO_VERSION,
                                       'ephem_pub': self._my_ephem_prv.public().dump(),
                                       'cert': self._sess_boss.certbyts,
                                       'first_mesg': self._crypter.encrypt(first_message)}))

        user = peer_cert.tokn.get('user')
        self.setLinkProp('cell:peer', user)

        self.fire('sess:txok')

        self._my_ephem_prv = None

class UserSess(Sess):
    '''
    The session object for a CellUser.
    '''
    def __init__(self, chan, prox):
        Sess.__init__(self, chan, prox, lisn=False)
        self._sess_prox = prox
        self._txok_evnt = threading.Event()
        self.on('sess:txok', self._setTxOk)

        self.taskplex = s_net.ChanPlex()
        self.taskplex.setLinkProp('repr', 'UserSess.taskplex')

    def _setTxOk(self, mesg):
        self._txok_evnt.set()

    def waittx(self, timeout=None):
        self._txok_evnt.wait(timeout=timeout)
        return self._txok_evnt.is_set()

    def call(self, mesg, timeout=None):
        '''
        Call a Cell endpoint which returns a single value.
        '''
        with self.task(mesg, timeout=timeout) as chan:
            return chan.next(timeout=timeout)

    def callx(self, mesg, func):

        if self.isfini:
            return func(False, ('IsFini', {}))

        chan = self.chan()

        def rx(link, data):
            chan.setLinkProp('callx:retn', True)
            chan.fini()
            func(*data) # ok, retn

        chan.onrx(rx)

        def fini():

            if chan.getLinkProp('callx:retn') is not None:
                return

            func(False, ('LinkTimeOut', {}))

        chan.onfini(fini)
        chan.tx(mesg)

    def task(self, mesg=None, timeout=None):
        '''
        Open a new channel within our session.
        '''
        chan = self.taskplex.open(self)
        chan.setq()

        if mesg is not None:
            chan.tx(mesg)

        return chan

    def chan(self):
        return self.taskplex.open(self)

class CellSess(Sess):
    '''
    The session object for the Cell.
    '''
    def __init__(self, chan, cell):

        Sess.__init__(self, chan, cell, lisn=True)
        self._sess_cell = cell

        def onchan(chan):
            chan.setLinkProp('cell:peer', self.getLinkProp('cell:peer'))
            chan.onrx(self._sess_cell.rx)

        self.taskplex = s_net.ChanPlex(onchan=onchan)
        self.taskplex.setLinkProp('repr', 'CellSess.taskplex')

        self.onfini(self.taskplex.fini)

class CellUser(SessBoss, s_eventbus.EventBus):

    def __init__(self, auth, roots=()):
        s_eventbus.EventBus.__init__(self)
        SessBoss.__init__(self, auth, roots=roots)

    def open(self, addr, timeout=None):
        '''
        Synchronously opens the Cell at the remote addr and return a UserSess Link.

        Args:
            addr ((str,int)): A (host, port) address tuple
            timeout (int/float): Connection timeout in seconds.

        Raises:
            CellUserErr: Raised if a timeout or link negotiation fails.  May have
            additional data in the ``excfo`` field.

        Returns:
            UserSess: The connected Link.
        '''
        with s_threads.RetnWait() as retn:

            def onlink(ok, link):

                if not ok:
                    erno = link
                    errs = os.strerror(erno)
                    return retn.errx(OSError(erno, errs))

                sess = UserSess(link, self)
                sess._initiateSession()

                retn.retn(sess)

            s_glob.plex.connect(tuple(addr), onlink)

            isok, sess = retn.wait(timeout=timeout)
            if not isok:
                raise s_common.CellUserErr(mesg='retnwait timed out or failed', excfo=sess)

        if not sess.waittx(timeout=timeout):
            raise s_common.CellUserErr(mesg='waittx timed out or failed')

        return sess

    def getCellSess(self, addr, func):
        '''
        A non-blocking way to form a session to a remote Cell.

        Args:
            addr (tuple): A address, port tuple.
            func: A callback function which takes a (ok, retn) args

        Returns:
            None
        '''
        def onsock(ok, retn):

            if not ok:
                return func(False, retn)

            link = retn
            sess = UserSess(link, self)

            def txok(x):
                sess.setLinkProp('sess:txok', True)
                func(True, sess)

            def fini():

                # if we dont have a peer, we were not successful
                if sess.getLinkProp('cell:peer') is not None:
                    return

                func(False, ('IsFini', {}))

            sess.on('sess:txok', txok)
            sess.onfini(fini)

            sess._initiateSession()

        s_glob.plex.connect(tuple(addr), onsock)

def getCellCtor(dirn, conf=None):
    '''
    Find the ctor option for a Cell and resolve the function.

    Args:
        dirn (str): The path to the Cell directory. This may contain the the
         ctor in the ``config.json`` file.
        conf (dict): Configuration dictionary for the cell. This may contain
         the ctor in the ``ctor`` key.

    Returns:
        ((str, function)): The python path to the ctor function and the resolved function.

    Raises:
        ReqConfOpt: If the ctor cannot be resolved from the cell path or conf
        NoSuchCtor: If the ctor function cannot be resolved.
    '''
    ctor = None

    if conf is not None:
        ctor = conf.get('ctor')

    path = s_common.genpath(dirn, 'config.json')

    if ctor is None and os.path.isfile(path):
        subconf = s_common.jsload(path)
        ctor = subconf.get('ctor')

    if ctor is None:
        raise s_common.ReqConfOpt(mesg='Missing ctor, cannot divide',
                                  name='ctor')

    func = s_dyndeps.getDynLocal(ctor)
    if func is None:
        raise s_common.NoSuchCtor(mesg='Cannot resolve ctor',
                                  name=ctor)

    return ctor, func

def divide(dirn, conf=None):
    '''
    Create an instance of a Cell in a subprocess.

    Args:
        dirn (str): Path to the directory backing the Cell.
        conf (dict): Configuration data.

    Returns:
        multiprocessing.Process: The Process object which was created to run the Cell
    '''
    ctx = multiprocessing.get_context('spawn')
    proc = ctx.Process(target=main, args=(dirn, conf))
    proc.start()

    return proc

def main(dirn, conf=None):
    '''
    Initialize and execute the main loop for a Cell.

    Args:
        dirn (str): Directory backing the Cell data.
        conf (dict): Configuration dictionary.

    Notes:
        This ends up calling ``main()`` on the Cell, and does not return
         anything. It cals sys.exit() at the end of its processing.
    '''
    try:

        # Configure logging since we may have come in via
        # multiprocessing.Process as part of a Daemon config.
        s_common.setlogging(logger,
                            os.getenv('SYN_TEST_LOG_LEVEL', 'WARNING'))

        dirn = s_common.genpath(dirn)
        ctor, func = getCellCtor(dirn, conf=conf)

        cell = func(dirn, conf)

        addr = cell.getCellAddr()
        logger.warning('cell divided: %s (%s) addr: %r' % (ctor, dirn, addr))

        cell.main()
        sys.exit(0)
    except Exception as e:
        logger.exception('main: %s (%s)' % (dirn, e))
        sys.exit(1)

class CellPool(s_eventbus.EventBus):
    '''
    A CellPool maintains sessions with a neuron and cells.
    '''
    def __init__(self, auth, neuraddr, neurfunc=None):
        s_eventbus.EventBus.__init__(self)

        self.neur = None
        self.neuraddr = neuraddr
        self.neurfunc = neurfunc

        self.auth = auth
        self.user = CellUser(auth)
        self.names = collections.deque() # used for round robin..

        self.ctors = {}
        self.cells = s_eventbus.BusRef()
        self.neurok = threading.Event()

        self._fireNeurLink()
        self.onfini(self.cells.fini)

    def neurwait(self, timeout=None):
        '''
        Wait for the neuron connection to be ready.

        Returns:
            bool: True on ready, False on timeout.
        '''
        return self.neurok.wait(timeout=timeout)

    def items(self):
        return self.cells.items()

    def _fireNeurLink(self):

        if self.isfini:
            return

        def fini():
            if not self.isfini:
                self._fireNeurLink()

        def onsess(ok, sess):

            if not ok:
                if self.isfini:
                    return
                s_glob.sched.insec(2, self._fireNeurLink)
                return

            sess.onfini(fini)

            self.neur = sess
            self.neurok.set()
            if self.neurfunc:
                self.neurfunc(sess)

        self.user.getCellSess(self.neuraddr, onsess)

    def add(self, name, func=None):
        '''
        Add a named cell to the pool.

        Func will be called back with each new Sess formed.
        '''
        self.names.append(name)

        def retry():
            if not self.isfini:
                s_glob.sched.insec(2, connect)

        def onsess(ok, retn):
            if self.isfini:
                return

            if not ok:
                logger.warning('CellPool.add(%s) onsess error: %r' % (name, retn))
                return retry()

            sess = retn

            sess.onfini(connect)
            self.cells.put(name, sess)
            self.fire('cell:add', name=name, sess=sess)

            if func is not None:
                try:
                    func(sess)
                except Exception as e:
                    logger.exception('CellPool.add(%s) callback failed' % (name,))

        def onlook(ok, retn):
            if self.isfini:
                return

            if not ok:
                logger.warning('CellPool.add(%s) onlook error: %r' % (name, retn))
                return retry()

            addr = retn.get('addr')
            self.user.getCellSess(addr, onsess)

        def connect():
            if self.isfini:
                return

            self.lookup(name, onlook)

        connect()

    def get(self, name):
        return self.cells.get(name)

    def lookup(self, name, func):

        if self.neur is None:
            return func(False, ('NotReady', {}))

        mesg = ('cell:get', {'name': name})
        self.neur.callx(mesg, func)

    def any(self):

        items = self.cells.items()
        if not items:
            return False, ('NotReady', {})

        return True, random.choice(items)

if __name__ == '__main__':
    main(sys.argv[1])
