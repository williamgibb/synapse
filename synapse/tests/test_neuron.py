import io
import threading

import synapse.link as s_link
import synapse.async as s_async
import synapse.daemon as s_daemon
import synapse.neuron as s_neuron
import synapse.common as s_common
import synapse.session as s_session
import synapse.telepath as s_telepath

from synapse.common import *
from synapse.tests.common import *

class FooBar:
    def __init__(self):
        pass

    def foo(self, x):
        return x + 20

    def bar(self, x):
        raise Exception('hi')

    def getsess(self, prop):
        sess = s_session.current()
        return sess.get(prop)

    def setsess(self, prop, valu):
        sess = s_session.current()
        sess.set(prop,valu)

#class TestNeuron(SynTest):
class TempDisabled:

    def getNeuNet(self):

        dmon = s_daemon.Daemon()
        link = dmon.listen('tcp://127.0.0.1:0/')

        neu0 = s_neuron.Neuron()
        neu1 = s_neuron.Neuron()
        neu2 = s_neuron.Neuron()

        dmon.onfini( neu0.fini )
        dmon.onfini( neu1.fini )
        dmon.onfini( neu2.fini )

        dmon.share('neu0',neu0)
        dmon.share('neu1',neu1)
        dmon.share('neu2',neu2)

        port = link[1].get('port')

        neup0 = s_telepath.openurl('tcp://127.0.0.1:%d/neu0' % port)
        neup1 = s_telepath.openurl('tcp://127.0.0.1:%d/neu1' % port)
        neup2 = s_telepath.openurl('tcp://127.0.0.1:%d/neu2' % port)

        wai0 = TestWaiter(neu1, 1, 'neu:link:up')
        neu0.link( neup1 )
        wai0.wait()

        wai0 = TestWaiter(neu1, 1, 'neu:link:up')
        neu0.link( neup2 )
        wai0.wait()

        dmon.onfini( neup0.fini )
        dmon.onfini( neup1.fini )
        dmon.onfini( neup2.fini )

        return dmon,(neu0,neu1,neu2),(neup0,neup1,neup2)

    #def test_neuron_keepstate(self):

        #fd = io.BytesIO()
        #neu = s_neuron.Daemon(statefd=fd)

        #ident = neu.getNeuInfo('ident')

        #neu.setNeuInfo('rsakey',rsa1)
        #neu.addNeuCortex('woot.0','ram:///',tags='hehe,haha')
        #neu.addNeuCortex('woot.1','ram:///')
        #neu.delNeuCortex('woot.1')

        #cert = neu.genPeerCert()

        #neu.fini()

        #fd.flush()
        #fd.seek(0)

        #neu = s_neuron.Daemon(statefd=fd)

        #self.assertEqual( neu.getNeuInfo('ident'), ident )
        #self.assertEqual( neu.getNeuInfo('rsakey'), rsa1 )
        #self.assertEqual( neu.getNeuInfo('peercert'), cert )

        #self.assertIsNone( neu.metacore.getCortex('woot.1') )
        #self.assertIsNotNone( neu.metacore.getCortex('woot.0') )

        #neu.fini()

    #def test_neuron_route_basics(self):

        #neuron = s_neuron.Daemon()

        #ident = neuron.ident
        #peers = [ s_common.guid() for i in range(3) ]

        #neuron.addPeerGraphEdge( ident, peers[0] )
        #neuron.addPeerGraphEdge( peers[0], ident )

        #neuron.addPeerGraphEdge( peers[0], peers[1] )
        #neuron.addPeerGraphEdge( peers[1], peers[0] )

        #neuron.addPeerGraphEdge( peers[1], peers[2] )
        #neuron.addPeerGraphEdge( peers[2], peers[1] )

        #route = neuron._getPeerRoute( peers[2] )

        #self.assertListEqual( route, [ ident, peers[0], peers[1], peers[2] ] )
        #neuron.fini()

    #def test_neuron_signer(self):
        #neu1 = s_neuron.Daemon()
        #neu2 = s_neuron.Daemon()
        #neu3 = s_neuron.Daemon()

        #neu1.setNeuInfo('rsakey',rsa1)
        #neu2.setNeuInfo('rsakey',rsa2)
        #neu3.setNeuInfo('rsakey',rsa3)

        #cert1 = neu1.genPeerCert(signer=True)

        # make the new cert everybodys signer
        #neu2.addPeerCert(cert1)
        #neu3.addPeerCert(cert1)

        #cert2 = neu2.genPeerCert()
        #cert3 = neu3.genPeerCert()

        #self.assertFalse( neu3.loadPeerCert( cert2 ) )
        #self.assertFalse( neu2.loadPeerCert( cert3 ) )

        #cert2 = neu1.signPeerCert( cert2 )
        #cert3 = neu1.signPeerCert( cert3 )

        #self.assertTrue( neu2.loadPeerCert( cert3 ) )
        #self.assertTrue( neu3.loadPeerCert( cert2 ) )

    #def test_neuron_authmod(self):
        #neu1 = s_neuron.Daemon()
        #self.assertTrue( neu1.getAuthAllow( 'hehe', 'haha' ) )

        #authdef = ('synapse.tests.test_neuron.FakeAuth',(),{})
        #neu1.setNeuInfo('authmod',authdef)

        #self.assertFalse( neu1.getAuthAllow( 'hehe', 'haha' ) )

    #def test_neuron_shares(self):
        #neu1 = s_neuron.Daemon()
        #foobar = 'synapse.tests.test_neuron.FooBar'
        #neu1.addNeuShare('foobar',foobar,(),{})

    #def newp_neuron_poolsize(self):
        #neu = s_neuron.Daemon()
        #neu.setNeuInfo('poolsize',4)
        #self.assertEqual( neu.neuboss.size, 4 )

    def test_neuron_route(self):

        neu0 = s_neuron.Neuron()
        neu1 = s_neuron.Neuron()
        neu2 = s_neuron.Neuron()

        neu0.link( neu1 )
        neu0.link( neu2 )

        wait0 = TestWaiter(neu1, 1, 'woot')
        neu2.route( neu1.getIden(), 'woot', x=10)

        events = wait0.wait()
        self.assertEqual( events[0][1].get('x'), 10 )

        neu0.fini()
        neu1.fini()
        neu2.fini()

    def test_neuron_storm(self):

        neu0 = s_neuron.Neuron()
        neu1 = s_neuron.Neuron()
        neu2 = s_neuron.Neuron()

        neu0.link( neu1 )
        neu0.link( neu2 )

        wai0 = TestWaiter(neu1, 1, 'woot')

        neu2.storm('woot', x=10)

        events = wai0.wait()
        self.assertEqual( events[0][1].get('x'), 10 )

        neu0.fini()
        neu1.fini()
        neu2.fini()

    def test_neuron_ping(self):
        dmon,neu,pxy = self.getNeuNet()

        dend = s_neuron.Dendrite(pxy[1])
        info = dend.ping(neu[2].getIden())

        self.assertIsNotNone( info.get('shared') )
        dmon.fini()

    def test_neuron_call(self):

        dmon,neu,pxy = self.getNeuNet()

        neu[2].share('foo',FooBar())

        dend = s_neuron.Dendrite(pxy[1])

        dest = (pxy[2].getIden(),None)
        foo = dend.open(dest, 'foo')

        self.assertEqual( foo.foo(10), 30 )

        e = threading.Event()
        data = {}
        def jobdone(job):
            data['ret'] = s_async.jobret(job)
            e.set()

        foo.foo(20, onfini=jobdone)
        e.wait(timeout=3)

        self.assertEqual( data.get('ret'), 40 )
        dmon.fini()

    def test_neuron_sess(self):
        dmon,neu,pxy = self.getNeuNet()

        iden2 = neu[2].getIden()
        neu[2].share('foo',FooBar())

        dend0 = s_neuron.Client(pxy[1])
        dend1 = s_neuron.Client(pxy[1])

        self.assertIsNotNone( dend0.getSidByIden(iden2) )

        foo0 = dend0.open((iden2,None),'foo')
        foo1 = dend1.open((iden2,None),'foo')

        self.assertIsNone( foo0.getsess('hehe') )
        self.assertIsNone( foo1.getsess('hehe') )

        foo0.setsess('hehe','lulz')

        self.assertIsNone( foo1.getsess('hehe') )
        self.assertEqual( foo0.getsess('hehe'), 'lulz' )

        dmon.fini()

