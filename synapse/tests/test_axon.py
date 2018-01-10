import io
import logging

import synapse.axon as s_axon
import synapse.daemon as s_daemon
import synapse.telepath as s_telepath

import synapse.lib.tufo as s_tufo
import synapse.lib.service as s_service

from synapse.tests.common import *

craphash = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
asdfhash = '6a204bd89f3c8348afd5c77c717a097a'
asdfhash_iden = '1c753abfe85b4cbe46584fa5b1834fa4'

logger = logging.getLogger(__name__)

class AxonTest(SynTest):
    def test_axon_basics(self):
        with self.getTestDir() as axondir:
            with s_axon.Axon(axondir) as axon:  # type: s_axon.Axon
                self.false(axon.has('md5', craphash))
                self.false(axon.has('md5', asdfhash))

                iden0 = axon.alloc(8)

                self.nn(axon.chunk(iden0, b'asdfasdf'))

                self.raises(NoSuchIden, axon.chunk, guid(), b'asdfasdf')

                self.true(axon.has('md5', asdfhash))
                self.false(axon.has('md5', craphash))

                byts = b''.join(axon.bytes('md5', asdfhash))

                self.eq(byts, b'asdfasdf')

                byts = b''.join(axon.bytes('guid', asdfhash_iden))

                self.eq(byts, b'asdfasdf')

                # Some basic tests for getAxonInfo structure
                axfo = axon.getAxonInfo()
                self.len(2, axfo)
                self.eq(axfo[0], axon.iden)
                # We have a link tufo
                link = axfo[1].get('link')
                self.isinstance(link, tuple)
                self.len(2, link)
                self.nn(link[1].get('url'))
                # We have an opts dictionary with a copy of the config opts
                opts = axfo[1].get('opts')
                self.isinstance(opts, dict)
                self.eq(opts, axon.getConfOpts())

                # We have some stats too and their expected keys
                iden1 = axon.alloc(8)
                axon.chunk(iden1, b'hehe')

                ekeys = [
                    'heap:used',
                    'heap:atomsize',
                    'inprog',
                    'clones:ready',
                    'clones:clonesready',
                    'clones:clonehosts',
                    'sync:size',
                    'sync:idens',
                    'sync:poffs',
                    'thrs:len',
                    'time',
                ]
                tick = now()
                stfo = axon.getAxonStatus()

                form, pprop = s_tufo.ndef(stfo)
                sprops = s_tufo.props(stfo)
                self.eq(pprop, axon.iden)
                self.eq(form, 'axon:stats')
                self.sorteq(sprops.keys(), ekeys)

                self.isinstance(sprops.get('clones:ready'), bool)
                self.false(sprops.get('clones:ready'))

                self.isinstance(sprops.get('clones:clonesready'), tuple)
                self.eq(sprops.get('clones:clonesready'), ())

                self.isinstance(sprops.get('clones:clonehosts'), tuple)
                self.eq(sprops.get('clones:clonehosts'), ())

                self.isinstance(sprops.get('heap:atomsize'), int)
                self.gt(sprops.get('heap:atomsize'), 0)

                self.isinstance(sprops.get('heap:used'), int)
                self.gt(sprops.get('heap:used'), 0)

                self.ge(sprops.get('heap:atomsize'), sprops.get('heap:used'))

                self.isinstance(sprops.get('inprog'), dict)
                inprogd = sprops.get('inprog')
                self.len(1, inprogd)
                progd = inprogd.get(iden1)
                self.eq(progd.get('size'), 8)
                self.eq(progd.get('maxoff') - progd.get('off'), 8)
                self.eq(progd.get('cur') - progd.get('off'), 4)

                self.isinstance(sprops.get('sync:idens'), tuple)
                self.len(0, sprops.get('sync:idens'))

                self.isinstance(sprops.get('sync:poffs'), dict)
                self.len(0, sprops.get('sync:poffs'))

                self.isinstance(sprops.get('sync:size'), int)
                self.gt(sprops.get('sync:size'), 0)

                self.isinstance(sprops.get('thrs:len'), int)
                self.eq(sprops.get('thrs:len'), 0)

                self.isinstance(sprops.get('time'), int)
                self.lt(sprops.get('time') - tick, 1000)

                blob = axon.chunk(iden1, b'haha')
                self.nn(blob)
                stfo = axon.getAxonStatus()
                self.notin(iden1, stfo[1].get('axon:stats:inprog'))

                axon.fini()

                axon = s_axon.Axon(axondir)

                self.true(axon.has('md5', asdfhash))
                self.true(axon.has('guid', asdfhash_iden))
                self.false(axon.has('md5', craphash))

                byts = b''.join(axon.bytes('md5', asdfhash))

                self.eq(byts, b'asdfasdf')

                self.none(axon.wants('md5', asdfhash, 8))
                self.nn(axon.wants('md5', craphash, 8))

                with self.assertRaises(NoSuchFile):
                    _ = [byts for byts in axon.bytes('md5', craphash)]

    def test_axon_restrictions(self):
        with self.getTestDir() as axondir:
            with s_axon.Axon(axondir) as axon:
                iden0 = axon.alloc(8)
                self.nn(axon.chunk(iden0, b'asdfasdf'))

                # set the axon as read-only and ensure we cannot write new blobs to it
                axon.setConfOpt('axon:ro', 1)
                axfo = axon.getAxonInfo()
                self.eq(axfo[1].get('opts').get('axon:ro'), 1)
                self.raises(NotSupported, axon.alloc, 8)

                # But we can still read from axon:ro=1 axons
                byts = b''.join(axon.bytes('guid', asdfhash_iden))
                self.eq(byts, b'asdfasdf')

                # non-clones cannot sync events
                self.raises(NotSupported, axon.sync, ())

                # clones cannot alloc new blobs directly
                axon.setConfOpt('axon:clone', 1)
                self.raises(NotSupported, axon.alloc, 8)

    def test_axon_bytesize(self):
        opts = {'axon:bytemax': s_axon.megabyte}
        with self.getTestDir() as axondir:
            with s_axon.Axon(axondir, **opts) as axon:
                iden = axon.alloc(1024)
                self.nn(iden)
                # Exceed bytemax by 1 byte - we should fail to allocate that space.
                self.raises(NotEnoughFree, axon.alloc, s_axon.megabyte - 1023)
                # But we still have some space left to alloc more blobs
                iden2 = axon.alloc(1024)
                self.nn(iden2)

    def test_axon_telepath(self):
        with self.getTestDir() as dirname:
            with s_daemon.Daemon() as dmon:
                link = dmon.listen('tcp://127.0.0.1:0/')
                port = link[1].get('port')

                with s_axon.Axon(dirname) as axon:
                    dmon.share('axon', axon, fini=True)

                    with s_telepath.openurl('tcp://127.0.0.1/axon', port=port) as prox:
                        with io.BytesIO(b'vertex') as fd:
                            blob = prox.eatfd(fd)
                            self.eq(blob[1]['axon:blob:sha256'],
                                    'e1b683e26a3aad218df6aa63afe9cf57fdb5dfaf5eb20cddac14305d67f48a02')

    def test_axon_eatbytes(self):
        self.thisHostMustNot(platform='windows')

        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                blob0 = axon.eatbytes(b'visi')
                with io.BytesIO(b'vertex') as fd:
                    blob1 = axon.eatfd(fd)

                port = axon.getAxonInfo()[1].get('link')[1].get('port')

                with s_telepath.openurl('tcp://127.0.0.1/axon', port=port) as prox:
                    blob2 = prox.eatbytes(b'hurr')
                    with io.BytesIO(b'durr') as fd:
                        blob3 = prox.eatfd(fd)

                blob4 = axon.eatbytes(b'')

        self.eq(blob0[1].get('axon:blob'), '442f602ecf8230b2a59a44b4f845be27')
        self.eq(blob1[1].get('axon:blob'), 'd4552906c1f6966b96d27e6fc79441b5')
        self.eq(blob2[1].get('axon:blob'), '0d60960570ef6da0a15f68c24b420334')
        self.eq(blob3[1].get('axon:blob'), '97c11d1057f75c9c0b79090131709f62')
        self.eq(blob4[1].get('axon:blob'), '370c1098a47904ea9caeb9f5f71459ba')

    def test_axon_eatfd_empty(self):
        self.thisHostMustNot(platform='windows')

        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                with io.BytesIO(b'') as fd:
                    blob0 = axon.eatfd(fd)

        self.eq(blob0[1].get('axon:blob'), '370c1098a47904ea9caeb9f5f71459ba')

class AxonHostTest(SynTest):
    def test_axon_host(self):

        self.thisHostMustNot(platform='windows')

        with self.getTestDir() as datadir:
            with open(os.path.join(datadir, 'foo'), 'w') as fd:
                fd.write('useless file to skip')

            host = s_axon.AxonHost(datadir)
            usage = host.usage()

            props = {
                'axon:bytemax': s_axon.megabyte * 10,
            }

            self.eq(host.usedspace, 0)

            axfo = host.add(**props)

            self.nn(usage.get('total'))

            self.eq(host.usedspace, s_axon.megabyte * 10)

            axon = host.axons.get(axfo[0])

            iden = axon.alloc(100)
            blob = axon.chunk(iden, b'V' * 100)

            self.nn(blob)

            self.none(axon.has('md5', None))

            self.true(axon.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axon.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axon.has('sha256', blob[1].get('axon:blob:sha256')))

            host.fini()

            host = s_axon.AxonHost(datadir)
            self.eq(host.usedspace, s_axon.megabyte * 10)

            axon = host.axons.get(axfo[0])

            self.true(axon.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axon.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axon.has('sha256', blob[1].get('axon:blob:sha256')))

            props = {
                'axon:syncmax': s_axon.megabyte * 10,
            }
            self.raises(NotEnoughFree, host.add, **props)

            host.fini()

    def test_axon_host_maxsize_limit(self):
        self.thisHostMustNot(platform='windows')

        with self.getTestDir() as datadir:
            bytemax = s_axon.megabyte * 50
            maxsize = s_axon.megabyte * 100

            props = {
                'axon:bytemax': bytemax,
                'axonhost:maxsize': maxsize
            }

            with s_axon.AxonHost(datadir, **props) as host:  # type: s_axon.AxonHost

                # Make a 50mb axon
                axfo0 = host.add()
                self.nn(axfo0)

                # We'd exceed maxsize by 5mb so fail there
                self.raises(NotEnoughFree, host.add, **{'axon:bytemax': s_axon.megabyte * 55})

                # We can still make a 10mb Axon
                axfo1 = host.add(**{'axon:bytemax': s_axon.megabyte * 10})
                self.nn(axfo1)

    def test_axon_host_free_limit(self):
        self.skipLongTest()
        self.thisHostMustNot(platform='windows')

        with self.getTestDir() as datadir:

            with s_axon.AxonHost(datadir) as host:  # type: s_axon.AxonHost

                usage = host.usage()
                free = usage.get('free')
                max_axons = 8

                _t = int(free / max_axons)

                bytemax = int(_t * 0.99)

                host.setConfOpt('axon:bytemax', bytemax)

                axons = {}
                for i in range(max_axons):
                    axfo = host.add()
                    axons[i] = axfo
                self.assertEqual(len(axons), max_axons)

                # Create one more axon which will exceed the free space available on the host
                self.raises(NotEnoughFree, host.add)

    def test_axon_host_clone(self):
        self.skipLongTest()
        self.thisHostMustNot(platform='windows')

        busurl = 'local://%s/axons' % guid()

        dmon = s_daemon.Daemon()
        dmon.listen(busurl)

        dmon.share('axons', s_service.SvcBus(), fini=True)

        with self.getTestDir() as datadir:
            dir0 = gendir(datadir, 'host0')
            dir1 = gendir(datadir, 'host1')
            dir2 = gendir(datadir, 'host2')

            host0 = s_axon.AxonHost(dir0, **{'axon:hostname': 'host0',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host2 = s_axon.AxonHost(dir2, **{'axon:hostname': 'host2',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })

            props = {
                'axon:bytemax': s_axon.megabyte * 50,
            }

            axfo0 = host0.add(**props)
            axon0 = s_telepath.openlink(axfo0[1].get('link'))  # type: s_axon.Axon
            self.true(axon0._waitClonesReady(timeout=16))
            self.notin(axfo0[0], host0.cloneaxons)

            # get refs to axon0's clones
            ciden1 = host1.cloneaxons[0]
            axonc1 = host1.axons.get(ciden1)  # type: s_axon.Axon
            self.eq(axonc1.getConfOpt('axon:clone:iden'), axfo0[0])
            ciden2 = host2.cloneaxons[0]
            axonc2 = host2.axons.get(ciden2)  # type: s_axon.Axon
            self.eq(axonc2.getConfOpt('axon:clone:iden'), axfo0[0])

            # Ensure axon:clone events have fired on the cores
            iden = axon0.alloc(100)
            cv = b'V' * 100
            blob = axon0.chunk(iden, cv)

            # We need to check the clones of axon0 to ensure that the clones have the data too
            time.sleep(1)

            self.nn(blob)
            self.true(axon0.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axon0.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axon0.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axon0.iterblob(blob))
            self.eq(axonbyts, cv)

            self.true(axonc1.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axonc1.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axonc1.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axonc1.iterblob(blob))
            self.eq(axonbyts, cv)

            self.true(axonc2.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axonc2.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axonc2.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axonc2.iterblob(blob))
            self.eq(axonbyts, cv)

            # We can get stats from an Axon proxy object
            iden1 = axon0.alloc(8)
            axon0.chunk(iden1, b'hehe')

            ekeys = [
                'heap:used',
                'heap:atomsize',
                'inprog',
                'clones:ready',
                'clones:clonesready',
                'clones:clonehosts',
                'sync:size',
                'sync:idens',
                'sync:poffs',
                'thrs:len',
                'time',
            ]
            tick = now()
            stfo = axon0.getAxonStatus()

            form, pprop = s_tufo.ndef(stfo)
            sprops = s_tufo.props(stfo)
            self.eq(pprop, axfo0[1].get('opts').get('axon:iden'))
            self.eq(form, 'axon:stats')
            self.sorteq(sprops.keys(), ekeys)

            self.isinstance(sprops.get('clones:ready'), bool)
            self.true(sprops.get('clones:ready'))

            self.isinstance(sprops.get('clones:clonesready'), tuple)
            self.len(2, sprops.get('clones:clonesready'))

            self.isinstance(sprops.get('clones:clonehosts'), tuple)
            self.eq(sprops.get('clones:clonehosts'), ('host1', 'host2'))

            self.isinstance(sprops.get('heap:atomsize'), int)
            self.gt(sprops.get('heap:atomsize'), 0)

            self.isinstance(sprops.get('heap:used'), int)
            self.gt(sprops.get('heap:used'), 0)

            self.ge(sprops.get('heap:atomsize'), sprops.get('heap:used'))

            self.isinstance(sprops.get('inprog'), dict)
            inprogd = sprops.get('inprog')
            self.len(1, inprogd)
            progd = inprogd.get(iden1)
            self.eq(progd.get('size'), 8)
            self.eq(progd.get('maxoff') - progd.get('off'), 8)
            self.eq(progd.get('cur') - progd.get('off'), 4)

            self.isinstance(sprops.get('sync:idens'), tuple)
            self.len(2, sprops.get('sync:idens'))

            self.isinstance(sprops.get('sync:poffs'), dict)
            self.len(2, sprops.get('sync:poffs'))

            self.isinstance(sprops.get('sync:size'), int)
            self.gt(sprops.get('sync:size'), 0)

            self.isinstance(sprops.get('thrs:len'), int)
            self.eq(sprops.get('thrs:len'), 2)

            self.isinstance(sprops.get('time'), int)
            self.lt(sprops.get('time') - tick, 1000)

            blob = axon0.chunk(iden1, b'haha')
            self.nn(blob)
            stfo = axon0.getAxonStatus()
            self.notin(iden1, stfo[1].get('axon:stats:inprog'))

            # Ensure clones have status and they don't have sync data
            cstfo = axonc1.getAxonStatus()
            cform, cpprop = s_tufo.ndef(cstfo)
            csprops = s_tufo.props(cstfo)

            self.isin(cpprop, sprops.get('clones:clonesready'))
            self.notin('sync:idens', csprops)
            self.notin('sync:size', csprops)
            self.notin('sync:poffs', csprops)
            self.isin('time', csprops)

            # The axonhosts also have stats
            tick = now()
            hstfo = host0.getAxonHostStatus()
            hform, hpprop = s_tufo.ndef(hstfo)
            hprops = s_tufo.props(hstfo)
            self.eq(host0.iden, hpprop)
            ekeys = (
                'time',
                'host:info',
                'axons'
            )
            self.sorteq(hprops.keys(), ekeys)
            self.eq(hprops.get('host:info').get('count'), 1)
            self.len(1, hprops.get('axons'))
            self.lt(hprops.get('time') - tick, 1000)

            # Now write a large amount of data to axon0 and ensure that data is replicated
            blobs = []
            n = 5
            tststr = 'deadb33f' * 100000
            for i in range(n):
                byts = tststr + 'lol' * i
                byts = byts.encode()
                blob = axon0.eatbytes(byts)
                blobs.append(blob)
            self.eq(len(blobs), n)
            time.sleep(2)
            for i, blob in enumerate(blobs):
                form, pprop = s_tufo.ndef(blob)
                ret = axonc1.byiden(pprop)
                self.nn(ret)
                ret = axonc2.byiden(pprop)
                self.nn(ret)

            # Add axons to the other two hosts and see that they clone over to the other hosts
            axfo1 = host1.add(**props)
            axon1 = s_telepath.openlink(axfo1[1].get('link'))  # type: s_axon.Axon
            self.true(axon1._waitClonesReady(timeout=16))

            axfo2 = host2.add(**props)
            axon2 = s_telepath.openlink(axfo2[1].get('link'))  # type: s_axon.Axon
            self.true(axon2._waitClonesReady(timeout=16))

            self.eq(len(host0.axons), 3)
            self.eq(len(host1.axons), 3)
            self.eq(len(host2.axons), 3)

            self.eq(len(host0.cloneaxons), 2)
            self.eq(len(host1.cloneaxons), 2)
            self.eq(len(host2.cloneaxons), 2)

            # Fini the proxy objects
            axonc1.fini()
            axonc2.fini()
            axon0.fini()
            axon1.fini()
            axon2.fini()

            # Fini the hosts
            host0.fini()
            host1.fini()
            host2.fini()

            # Ensure the axonhost fini'd its objects
            self.true(host0.axonbus.isfini)
            for axon in host0.axons.values():
                self.true(axon.isfini)

            # Deleting sync file for host1's axon from host0
            tgtfn = '{}.off'.format(ciden1)
            for fdir, dirs, fns in os.walk(datadir):
                for fn in fns:
                    if fn == tgtfn:
                        tgtfp = os.path.join(fdir, fn)
                        logger.warning('Deleting {}'.format(tgtfp))
                        os.unlink(tgtfp)

            host0 = s_axon.AxonHost(dir0, **{'axon:hostname': 'host0',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host2 = s_axon.AxonHost(dir2, **{'axon:hostname': 'host2',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })

            axonc1 = host1.axons.get(ciden1)  # type: s_axon.Axon
            logger.warning('Waiting for clone to occur')
            time.sleep(3)

            blob = blobs[-1]
            form, pprop = s_tufo.ndef(blob)

            ret = axonc1.byiden(pprop)
            self.nn(ret)
            logger.warning('Checking byts')
            cbyts = b''.join([blob for blob in axonc1.iterblob(ret)])
            self.eq(cbyts, byts)

            # Fini the hosts
            host0.fini()
            host1.fini()
            host2.fini()

        dmon.fini()

    def test_axon_host_bounce_sync(self):
        self.skipLongTest()
        self.thisHostMustNot(platform='windows')

        busurl = 'local://%s/axons' % guid()

        dmon = s_daemon.Daemon()
        dmon.listen(busurl)
        sbus = s_service.SvcBus()

        dmon.share('axons', sbus, fini=True)

        with self.getTestDir() as datadir:
            dir0 = gendir(datadir, 'host0')
            dir1 = gendir(datadir, 'host1')

            host0 = s_axon.AxonHost(dir0, **{'axon:hostname': 'host0',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })

            props = {
                'axon:bytemax': s_axon.megabyte * 50,
                'axon:clones': 1,
            }

            axfo0 = host0.add(**props)
            axon0 = s_telepath.openlink(axfo0[1].get('link'))  # type: s_axon.Axon
            self.true(axon0._waitClonesReady(timeout=16))
            self.notin(axfo0[0], host0.cloneaxons)

            # get refs to axon0's clones
            ciden1 = host1.cloneaxons[0]
            axonc1 = host1.axons.get(ciden1)  # type: s_axon.Axon
            self.eq(axonc1.getConfOpt('axon:clone:iden'), axfo0[0])

            # Ensure axon:clone events have fired on the cores
            iden = axon0.alloc(100)
            cv = b'V' * 100
            blob = axon0.chunk(iden, cv)

            # We need to check the clones of axon0 to ensure that the clones have the data too
            time.sleep(1)

            self.nn(blob)
            r = axon0.has('md5', blob[1].get('axon:blob:md5'))
            self.true(r)
            self.true(axon0.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axon0.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axon0.iterblob(blob))
            self.eq(axonbyts, cv)

            self.true(axonc1.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axonc1.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axonc1.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axonc1.iterblob(blob))
            self.eq(axonbyts, cv)

            # Now we teardown axon1 proxy/host1 and bring them back up
            p0 = s_telepath.openurl(busurl)
            sbusprox = s_service.SvcProxy(p0)
            r = sbusprox.getSynSvcsByTag('class.synapse.axon.Axon')
            self.len(2, r)

            logger.warning('Tearing down host1 objects')
            axonc1.fini()
            host1.fini()

            # Ask the svcbus what axons we have
            waiter = sbusprox.waiter(2, 'syn:svc:fini')
            waiter.wait(2)
            r = sbusprox.getSynSvcsByTag('class.synapse.axon.Axon')
            self.len(1, r)

            # Write data to axon0
            logger.debug('Writing buf2')
            iden = axon0.alloc(100)
            cv = b'v' * 100
            blob = axon0.chunk(iden, cv)
            time.sleep(1)

            self.nn(blob)
            self.true(axon0.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axon0.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axon0.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axon0.iterblob(blob))
            self.eq(axonbyts, cv)
            logger.warning('Bringing host1 back up')
            # Bring host1 back up - he axon0 should start sending sync events over to axonc1
            # and it should eventutally get cv / blob available to it
            waiter = sbusprox.waiter(2, 'syn:svc:init')
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            waiter.wait(2)
            # We should have both our axons available again on the svcbus
            r = sbusprox.getSynSvcsByTag('class.synapse.axon.Axon')
            self.len(2, r)

            ciden1 = host1.cloneaxons[0]
            axonc1 = host1.axons.get(ciden1)  # type: s_axon.Axon
            self.eq(axonc1.getConfOpt('axon:clone:iden'), axfo0[0])
            logger.warning('Sleeping for cloning')
            time.sleep(3)

            logger.debug('Checking axonc1 for data')
            self.true(axonc1.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axonc1.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axonc1.has('sha256', blob[1].get('axon:blob:sha256')))
            logger.debug('Checking axonc1 bytes')
            axonbyts = b''.join(_byts for _byts in axonc1.iterblob(blob))
            self.eq(axonbyts, cv)

            # Now we teardown axon1 proxy/host1 and bring them back up AGAIN
            logger.warning('Tearing down host1 objects for a second time')
            axonc1.fini()
            host1.fini()

            # Write data to axon0
            logger.debug('Writing buf3')
            iden = axon0.alloc(100)
            cv = b'!' * 100
            blob = axon0.chunk(iden, cv)
            time.sleep(1)

            self.nn(blob)
            self.true(axon0.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axon0.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axon0.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axon0.iterblob(blob))
            self.eq(axonbyts, cv)
            logger.warning('Bringing host1 back up a second time')
            # Bring host1 back up - he axon0 should start sending sync events over to axonc1
            # and it should eventutally get cv / blob available to it
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            ciden1 = host1.cloneaxons[0]
            axonc1 = host1.axons.get(ciden1)  # type: s_axon.Axon
            self.eq(axonc1.getConfOpt('axon:clone:iden'), axfo0[0])
            logger.warning('Sleeping')
            time.sleep(3)

            logger.debug('Checking axonc1 for data')
            self.true(axonc1.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axonc1.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axonc1.has('sha256', blob[1].get('axon:blob:sha256')))
            logger.debug('Checking axonc1 bytes')
            axonbyts = b''.join(_byts for _byts in axonc1.iterblob(blob))
            self.eq(axonbyts, cv)

            # Now we teardown axon1 proxy/host1 and bring them back up AGAIN
            logger.warning('Tearing down host1 objects for a third time')
            axonc1.fini()
            host1.fini()

            logger.warning('Tearing down host0 object as well before recreating it')
            axon0.fini()
            host0.fini()

            # Deleting sync file for host1's axon from host0
            tgtfn = '{}.off'.format(ciden1)
            for fdir, dirs, fns in os.walk(datadir):
                for fn in fns:
                    if fn == tgtfn:
                        tgtfp = os.path.join(fdir, fn)
                        logger.warning('Deleting {}'.format(tgtfp))
                        os.unlink(tgtfp)

            w = sbusprox.waiter(2, 'syn:svc:init')
            host0 = s_axon.AxonHost(dir0, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            w.wait(1)
            svcfo = sbusprox.getSynSvcsByTag(s_axon.axontag)[0]
            axon0 = s_telepath.openlink(svcfo[1].get('link'))  # type: s_axon.Axon

            # Write data to axon0
            logger.debug('Writing buf4')
            iden = axon0.alloc(100)
            cv = b'?' * 100
            blob = axon0.chunk(iden, cv)
            time.sleep(1)

            self.nn(blob)
            self.true(axon0.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axon0.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axon0.has('sha256', blob[1].get('axon:blob:sha256')))
            axonbyts = b''.join(_byts for _byts in axon0.iterblob(blob))
            self.eq(axonbyts, cv)
            logger.warning('Bringing host1 back up a third time')
            # Bring host1 back up - he axon0 should start sending sync events over to axonc1
            # and it should eventutally get cv / blob available to it
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            ciden1 = host1.cloneaxons[0]
            axonc1 = host1.axons.get(ciden1)  # type: s_axon.Axon
            self.eq(axonc1.getConfOpt('axon:clone:iden'), axfo0[0])
            logger.warning('Sleeping')
            time.sleep(3)

            logger.debug('Checking axonc1 for data')
            self.true(axonc1.has('md5', blob[1].get('axon:blob:md5')))
            self.true(axonc1.has('sha1', blob[1].get('axon:blob:sha1')))
            self.true(axonc1.has('sha256', blob[1].get('axon:blob:sha256')))
            logger.debug('Checking axonc1 bytes')
            axonbyts = b''.join(_byts for _byts in axonc1.iterblob(blob))
            self.eq(axonbyts, cv)

            # Fini the proxy objects
            axon0.fini()
            axonc1.fini()
            sbusprox.fini()

            # Fini the hosts
            host0.fini()
            host1.fini()

        dmon.fini()

    def test_axon_clone_large(self):
        self.skipLongTest()
        self.thisHostMustNot(platform='windows')

        busurl = 'local://%s/axons' % guid()

        dmon = s_daemon.Daemon()
        dmon.listen(busurl)

        dmon.share('axons', s_service.SvcBus(), fini=True)

        with self.getTestDir() as datadir:
            dir0 = gendir(datadir, 'host0')
            dir1 = gendir(datadir, 'host1')
            dir2 = gendir(datadir, 'host2')
            dir3 = gendir(datadir, 'host3')

            host0 = s_axon.AxonHost(dir0, **{'axon:hostname': 'host0',
                                             'axon:axonbus': busurl,
                                             'axon:clones': 1,
                                             'axonhost:autorun': 2,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:clones': 1,
                                             'axonhost:autorun': 2,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host2 = s_axon.AxonHost(dir2, **{'axon:hostname': 'host2',
                                             'axon:axonbus': busurl,
                                             'axon:clones': 1,
                                             'axonhost:autorun': 2,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host3 = s_axon.AxonHost(dir3, **{'axon:hostname': 'host3',
                                             'axon:axonbus': busurl,
                                             'axon:clones': 1,
                                             'axonhost:autorun': 2,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })

            time.sleep(3)
            ta = [len(host.axons) for host in [host0, host1, host2, host3]]
            tc = [len(host.cloneaxons) for host in [host0, host1, host2, host3]]
            total_axons = sum(ta)
            total_clones = sum(tc)
            self.eq(total_axons, 16)
            self.eq(total_clones, 8)

            host0.fini()
            host1.fini()
            host2.fini()
            host3.fini()
        dmon.fini()

    def test_axon_autorun(self):

        self.thisHostMustNot(platform='windows')

        with self.getTestDir() as dirname:
            opts = {
                'axonhost:autorun': 2,
                'axon:bytemax': s_axon.megabyte,
            }
            host = s_axon.AxonHost(dirname, **opts)
            self.eq(len(host.axons), 2)
            host.fini()

    def test_axon_host_spinbackup(self):
        self.skipLongTest()
        self.thisHostMustNot(platform='windows')

        hstcfg = {
            "vars": {
                "hcfg0": {
                    "axon:hostname": "host0",
                    "axon:bytemax": 1024000000,
                    "axonhost:maxsize": 10240000000,
                    "axonhost:autorun": 1,
                    "axon:clones": 1,
                },
                "hcfg1": {
                    "axon:hostname": "host1",
                    "axon:bytemax": 1024000000,
                    "axonhost:maxsize": 10240000000,
                    "axonhost:autorun": 1,
                    "axon:clones": 1,
                }
            },
            "ctors": [
                [
                    "host0",
                    "ctor://synapse.axon.AxonHost(dir0, **hcfg0)"
                ],
                [
                    "host1",
                    "ctor://synapse.axon.AxonHost(dir1, **hcfg1)"
                ]
            ],
            "share": [
                [
                    "host0",
                    {
                        "onfini": True
                    }
                ],
                [
                    "host1",
                    {
                        "onfini": True
                    }
                ]
            ]
        }

        svccfg = {
            "comment": "dmon file for axon stresstest",
            "ctors": [
                [
                    "axonbus",
                    "ctor://synapse.lib.service.SvcBus()",
                    {}
                ]
            ],
            "share": [
                [
                    "axonbus",
                    {
                        "onfini": True
                    }
                ]
            ]
        }

        with self.getTestDir() as dirname:
            hostdir0 = gendir(dirname, 'host0')
            hostdir1 = gendir(dirname, 'host1')
            hstcfg['vars']['dir0'] = hostdir0
            hstcfg['vars']['dir1'] = hostdir1

            with s_daemon.Daemon() as svcdmon:
                svcdmon.loadDmonConf(svccfg)
                link = svcdmon.listen('tcp://127.0.0.1:0/')
                port = link[1].get('port')
                busurl = 'tcp://127.0.0.1:{}/axonbus'.format(port)
                hstcfg['vars']['hcfg0']['axon:axonbus'] = busurl
                hstcfg['vars']['hcfg1']['axon:axonbus'] = busurl
                svcbus = s_service.openurl('tcp://127.0.0.1:0/axonbus', port=port)  # type: s_service.SvcProxy

                with s_daemon.Daemon() as axondmon:
                    w = svcbus.waiter(6, 'syn:svc:init')
                    axondmon.loadDmonConf(hstcfg)
                    w.wait(15)
                    first_axons = svcbus.getSynSvcsByTag(s_axon.axontag)
                    self.eq(len(first_axons), 4)

                # Close the proxy
                svcbus.fini()

            # Spin the AxonHost back up
            # This does exercise a behavior in the AxonHost to always give
            # preference for its :axonbus configuration option over that of
            # its Axon's. While this scenario is present in the unit test,
            # in a real migration which could involve changing the :axonbus,
            # this ensures that the children Axons of the AxonHost are updated
            # to point to the new bus.

            with s_daemon.Daemon() as svcdmon:
                svcdmon.loadDmonConf(svccfg)
                link = svcdmon.listen('tcp://127.0.0.1:0/')
                port = link[1].get('port')
                busurl = 'tcp://127.0.0.1:{}/axonbus'.format(port)
                hstcfg['vars']['hcfg0']['axon:axonbus'] = busurl
                hstcfg['vars']['hcfg1']['axon:axonbus'] = busurl
                svcbus = s_service.openurl('tcp://127.0.0.1:0/axonbus', port=port)  # type: s_service.SvcProxy

                with s_daemon.Daemon() as axondmon:
                    w = svcbus.waiter(6, 'syn:svc:init')
                    axondmon.loadDmonConf(hstcfg)
                    w.wait(15)
                    axons = svcbus.getSynSvcsByTag(s_axon.axontag)
                    self.eq(len(axons), 4)
                    # Ensure these are the same axons we had first created
                    self.eq({axn[1].get('name') for axn in axons}, {axn[1].get('name') for axn in first_axons})

                # Close the proxy
                svcbus.fini()

class AxonClusterTest(SynTest):
    def test_axon_cluster(self):
        self.skipLongTest()
        self.thisHostMustNot(platform='windows')

        busurl = 'local://%s/axons' % guid()

        dmon = s_daemon.Daemon()
        dmon.listen(busurl)

        dmon.share('axons', s_service.SvcBus(), fini=True)
        svcprox = s_service.openurl(busurl)

        axcluster = s_axon.AxonCluster(svcprox)

        with self.getTestDir() as datadir:

            dir0 = gendir(datadir, 'host0')
            dir1 = gendir(datadir, 'host1')
            dir2 = gendir(datadir, 'host2')

            host0 = s_axon.AxonHost(dir0, **{'axon:hostname': 'host0',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host2 = s_axon.AxonHost(dir2, **{'axon:hostname': 'host2',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })

            props = {
                'axon:clones': 1,
                'axon:bytemax': s_axon.megabyte,
            }

            axfo0 = host0.add(**props)

            axcluster._waitWrAxons(1, 4)

            # Ensure our axfo0 was cloned to someone in the cluster
            axon0 = s_telepath.openlink(axfo0[1].get('link'))  # type: s_axon.Axon
            axon0._waitClonesReady(timeout=16)
            foundclone = False
            if host1.cloneaxons:
                foundclone = True
            if host2.cloneaxons:
                foundclone = True
            self.true(foundclone)
            axon0.fini()  # fini the proxy object

            self.false(axcluster.has('md5', craphash))
            self.false(axcluster.has('md5', asdfhash))

            buf = b'asdfasdf'
            iden = axcluster.alloc(len(buf))
            self.nn(axcluster.chunk(iden, buf))

            self.false(axcluster.has('md5', craphash))
            self.true(axcluster.has('md5', asdfhash))
            self.true(axcluster.has('guid', asdfhash_iden))

            blobs = axcluster.find('md5', craphash)
            self.eq(len(blobs), 0)

            time.sleep(0.2)  # Yield to axon threads
            blobs = axcluster.find('md5', asdfhash)
            # We have two blobs for the same hash since the clone of axfo0 is up on host1/host2
            self.eq(len(blobs), 2)

            blob = blobs[0]
            byts = b''.join(axcluster.iterblob(blob))
            self.eq(byts, buf)

            blob[1].pop('.axon')
            byts = b''.join(axcluster.iterblob(blob))
            self.eq(byts, buf)

            self.nn(axcluster.wants('md5', craphash, len(buf)))
            self.none(axcluster.wants('md5', asdfhash, len(buf)))

            # Eat bytes via AxonMixin APIs
            byts = 'pennywise'.encode()

            blob = axcluster.eatbytes(byts)
            self.nn(blob)
            self.isin('.new', blob[1])
            blob = axcluster.eatbytes(byts)
            self.notin('.new', blob[1])

            buf = io.BytesIO('dancing clown'.encode())
            blob = axcluster.eatfd(buf)
            self.nn(blob)
            self.isin('.new', blob[1])
            blob = axcluster.eatfd(buf)
            self.notin('.new', blob[1])

            w = svcprox.waiter(5, 'syn:svc:fini')

            host0.fini()
            host1.fini()
            host2.fini()

            w.wait(1)

            # Ensure we have fini'd and removed all the axon proxy objects
            self.len(0, axcluster.axons)

        svcprox.fini()
        dmon.fini()

    def test_axon_cluster_cortex(self):
        self.skipLongTest()
        self.thisHostMustNot(platform='windows')

        localguid = guid()
        busurl = 'local://%s/axons' % localguid
        hahaurl = 'local://%s/haha' % localguid

        dmon = s_daemon.Daemon()
        dmon.listen(busurl)

        dmon.share('axons', s_service.SvcBus(), fini=True)
        dmon.share('haha', {})

        svcprox = s_service.openurl(busurl)

        axcluster = s_axon.AxonCluster(svcprox)

        with self.getTestDir() as datadir:

            dir0 = gendir(datadir, 'host0')
            dir1 = gendir(datadir, 'host1')
            dir2 = gendir(datadir, 'host2')

            host0 = s_axon.AxonHost(dir0, **{'axon:hostname': 'host0',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host1 = s_axon.AxonHost(dir1, **{'axon:hostname': 'host1',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })
            host2 = s_axon.AxonHost(dir2, **{'axon:hostname': 'host2',
                                             'axon:axonbus': busurl,
                                             'axon:bytemax': s_axon.megabyte * 100,
                                             })

            props = {
                'axon:clones': 1,
                'axon:bytemax': s_axon.megabyte,
            }

            axfo0 = host0.add(**props)

            axcluster._waitWrAxons(1, 4)

            # Ensure our axfo0 was cloned to someone in the cluster
            axon0 = s_telepath.openlink(axfo0[1].get('link'))  # type: s_axon.Axon
            axon0._waitClonesReady(timeout=16)
            foundclone = False
            if host1.cloneaxons:
                foundclone = True
            if host2.cloneaxons:
                foundclone = True
            self.true(foundclone)
            axon0.fini()  # fini the proxy object

            core = s_cortex.openurl('ram://')
            core.setConfOpt('axon:url', busurl)

            self.false(axcluster.has('md5', craphash))
            self.false(axcluster.has('md5', asdfhash))

            node = core.formNodeByBytes(b'asdfasdf', name='asdf')
            self.eq(node[1].get('file:bytes'), asdfhash_iden)
            self.eq(node[1].get('file:bytes:md5'), asdfhash)

            self.true(axcluster.has('md5', asdfhash))
            self.true(axcluster.has('guid', asdfhash_iden))

            fd = io.BytesIO(b'visi')
            node = core.formNodeByFd(fd, name='visi.bin')
            self.eq(node[1].get('file:bytes:size'), 4)
            self.eq(node[1].get('file:bytes:name'), 'visi.bin')
            self.eq(node[1].get('file:bytes'), '442f602ecf8230b2a59a44b4f845be27')
            self.eq(node[1].get('file:bytes:md5'), '1b2e93225959e3722efed95e1731b764')

            self.true(axcluster.has('md5', '1b2e93225959e3722efed95e1731b764'))
            self.true(axcluster.has('guid', '442f602ecf8230b2a59a44b4f845be27'))

            host0.fini()
            host1.fini()
            host2.fini()

        svcprox.fini()
        dmon.fini()

class AxonFSTest(SynTest):
    # Axon File System Tests
    def test_axon_fs_create(self):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                axon.fs_create('/foofile', 33204)
                actual = axon.fs_getattr('/foofile')

                self.eq(actual['st_nlink'], 1)
                self.eq(actual['st_mode'], 33204)
                self.gt(actual['st_atime'], 1000000000)
                self.gt(actual['st_ctime'], 1000000000)
                self.gt(actual['st_mtime'], 1000000000)

                self.raises(NoSuchDir, axon.fs_create, '/foodir/foo2', 33204)
                # self.raises(NoSuchDir, axon.fs_create, 'foo2nope', 33204)  # No parent

    def test_axon_fs_getattr(self):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                actual = axon.fs_getattr('/')

                self.eq(actual['st_nlink'], 2)
                self.eq(actual['st_mode'], 16893)
                self.gt(actual['st_atime'], 1000000000)
                self.gt(actual['st_ctime'], 1000000000)
                self.gt(actual['st_mtime'], 1000000000)

    def test_axon_fs_getxattr(self):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                actual = axon.fs_getattr('/')
                self.eq(actual['st_nlink'], 2)
                self.eq(actual['st_mode'], 16893)
                self.gt(actual['st_atime'], 1000000000)
                self.gt(actual['st_ctime'], 1000000000)
                self.gt(actual['st_mtime'], 1000000000)

                nlink = axon.fs_getxattr('/', 'st_nlink')
                self.eq(nlink, actual['st_nlink'])
                self.raises(NoSuchData, axon.fs_getxattr, '/', 'zzz')
                self.raises(NoSuchData, axon.fs_getxattr, '/haha', 'st_nlink')

    def test_axon_fs_mkdir(self):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                actual = axon.fs_mkdir('/foo', 0x1FD)
                self.eq(actual, None)

                actual = axon.fs_getattr('/foo')
                self.eq(actual['st_nlink'], 2)
                self.eq(actual['st_mode'], 16893)
                self.gt(actual['st_atime'], 1000000000)
                self.gt(actual['st_ctime'], 1000000000)
                self.gt(actual['st_mtime'], 1000000000)

                self.raises(NoSuchDir, axon.fs_mkdir, '/foodir/foo2', 16893)
                # self.raises(NoSuchDir, axon.fs_mkdir, 'noparent', 16893)

                self.raises(FileExists, axon.fs_mkdir, '/foo', 0x1FD)

    def test_axon_fs_read(self, *args, **kwargs):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                data = b'haha\n'

                fd = tempfile.SpooledTemporaryFile()
                fd.write(data)

                axon.fs_create('/haha', 33204)

                _, blobprops = axon.eatfd(fd)
                blobsize = blobprops.get('axon:blob:size')
                blob = blobprops.get('axon:blob')

                axon._fs_update_blob('/haha', blobsize, blob)
                fd.close()

                sz = axon.fs_getattr('/haha').get('st_size')
                actual = axon.fs_read('/haha', sz, 0)
                self.eq(actual, data)

                self.raises(NoSuchEntity, axon.fs_read, '/hoho2', 100, 0)

                axon.fs_create('/haha2', 33204)
                actual = axon.fs_read('/haha2', 1000000000, 0)
                self.eq(actual, b'')

    def test_axon_fs_readdir(self, *args, **kwargs):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                axon.fs_create('/foofile', 33204)
                self.eq(sorted(axon.fs_readdir('/')), sorted(['.', '..', 'foofile']))

                self.raises(NoSuchEntity, axon.fs_readdir, '/haha')
                self.raises(NotSupported, axon.fs_readdir, '/foofile')

    def test_axon_fs_rmdir(self, *args, **kwargs):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                axon.fs_mkdir('/foo', 0x1FD)
                axon.fs_create('/foo/haha', 33204)

                self.raises(NotEmpty, axon.fs_rmdir, '/foo')

                axon.fs_unlink('/foo/haha')
                self.nn(axon.fs_getattr('/foo'))
                axon.fs_rmdir('/foo')
                self.raises(NoSuchEntity, axon.fs_getattr, '/foo')
                self.raises(NoSuchEntity, axon.fs_rmdir, '/foo')

    def test_axon_fs_rename(self, *args, **kwargs):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                axon.fs_mkdir('/dir', 0x1FD)
                axon.fs_create('/dir/a', 33204)
                axon.fs_rename('/dir/a', '/dir/b')

                actual = axon.fs_readdir('/dir')
                self.eq(sorted(actual), ['.', '..', 'b'])

                # source doesnt exist
                self.raises(NoSuchEntity, axon.fs_rename, 'fake', 'other')

                # source doesnt have parent
                tufo = axon.core.getTufoByProp('axon:path', '/dir')
                axon.core.delTufo(tufo)
                self.raises(NoSuchDir, axon.fs_rename, '/dir/b', '/dir/c')

                # dst doesnt have parent
                axon.fs_mkdir('/dira', 0x1FD)
                axon.fs_mkdir('/dirb', 0x1FD)
                axon.fs_create('/dira/a', 33204)
                tufo = axon.core.getTufoByProp('axon:path', '/dirb')
                axon.core.delTufo(tufo)
                self.raises(NoSuchDir, axon.fs_rename, '/dira/a', '/dirb/b')

                # dst not empty
                axon.fs_mkdir('/flda', 0x1FD)
                axon.fs_mkdir('/fldb', 0x1FD)
                axon.fs_create('/flda/a', 33204)
                axon.fs_create('/fldb/a', 33204)
                self.raises(NotEmpty, axon.fs_rename, '/flda', '/fldb')

                # overwrite a regular file with a directory
                axon.fs_mkdir('/adir', 0x1FD)
                axon.fs_create('/reg', 33204)
                axon.fs_rename('/adir', '/reg')
                self.none(axon.fs_getattr('/reg').get('st_size'))

                # update all the kids
                axon.fs_mkdir('/cool', 0x1FD)
                axon.fs_create('/cool/a', 33204)
                axon.fs_create('/cool/b', 33204)
                axon.fs_create('/cool/c', 33204)
                axon.fs_rename('/cool', '/cooler')

                self.raises(NoSuchEntity, axon.fs_getattr, '/cool')
                self.raises(NoSuchEntity, axon.fs_getattr, '/cool/a')
                self.raises(NoSuchEntity, axon.fs_getattr, '/cool/b')
                self.raises(NoSuchEntity, axon.fs_getattr, '/cool/c')

                self.nn(axon.fs_getattr('/cooler'))
                self.nn(axon.fs_getattr('/cooler/a'))
                self.nn(axon.fs_getattr('/cooler/b'))
                self.nn(axon.fs_getattr('/cooler/c'))

                # nested dirs
                axon.fs_mkdir('/nest1', 0x1FD)
                axon.fs_mkdir('/nest1/nest2', 0x1FD)
                axon.fs_mkdir('/nest1/nest2/nest3', 0x1FD)
                axon.fs_create('/nest1/nest2/nest3/reg', 33204)

                axon.fs_rename('/nest1', '/nest')

                self.nn(axon.fs_getattr('/nest'))
                self.nn(axon.fs_getattr('/nest/nest2'))
                self.nn(axon.fs_getattr('/nest/nest2/nest3'))
                self.nn(axon.fs_getattr('/nest/nest2/nest3/reg'))

    def test_axon_fs_truncate(self, *args, **kwargs):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                axon.fs_create('/foofile', 33204)
                tufo = axon.core.getTufoByProp('axon:path', '/foofile')
                axon.core.setTufoProps(tufo, st_size=100, blob=32 * 'a')
                self.eq(tufo[1].get('axon:path:st_size'), 100)
                self.eq(tufo[1].get('axon:path:blob'), 32 * 'a')

                axon.fs_truncate('/foofile')
                tufo = axon.core.getTufoByProp('axon:path', '/foofile')
                self.eq(tufo[1].get('axon:path:st_size'), 0)
                self.eq(tufo[1].get('axon:path:blob'), None)

                self.raises(NoSuchEntity, axon.fs_truncate, '/notthere')

    def test_axon_fs_unlink(self, *args, **kwargs):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                axon.fs_create('/foofile', 33204)

                actual = axon.fs_getattr('/foofile')
                self.eq(actual['st_nlink'], 1)
                self.eq(actual['st_mode'], 33204)
                self.gt(actual['st_atime'], 1000000000)
                self.gt(actual['st_ctime'], 1000000000)
                self.gt(actual['st_mtime'], 1000000000)

                axon.fs_unlink('/foofile')
                self.raises(NoSuchEntity, axon.fs_getattr, '/foofile')
                self.raises(NoSuchFile, axon.fs_unlink, '/foofile')

    def test_axon_fs_utimens(self, *args, **kwargs):
        with self.getTestDir() as dirname:
            with s_axon.Axon(dirname) as axon:
                axon.fs_create('/foofile', 33204)
                actual = axon.fs_getattr('/foofile')
                ctime = actual['st_ctime']

                self.gt(actual['st_atime'], 1000000000)
                self.gt(actual['st_ctime'], 1000000000)
                self.gt(actual['st_mtime'], 1000000000)

                axon.fs_utimens('/foofile', (0, 0))
                actual = axon.fs_getattr('/foofile')

                self.eq(actual['st_mtime'], 0)
                self.eq(actual['st_ctime'], ctime)
                self.eq(actual['st_atime'], 0)

                axon.fs_utimens('/foofile')
                self.eq(actual['st_mtime'], 0)
                self.eq(actual['st_ctime'], ctime)
                self.eq(actual['st_atime'], 0)

                self.raises(NoSuchEntity, axon.fs_utimens, '/derry/sewers', (0, 0))

    # test_axon_fs_write  - do not implement this function

    def test_axon__fs_isdir(self, *args, **kwargs):
        self.eq(s_axon.Axon._fs_isdir(None), False)
        self.eq(s_axon.Axon._fs_isdir(0), False)
        self.eq(s_axon.Axon._fs_isdir(33204), False)
        self.eq(s_axon.Axon._fs_isdir(16893), True)

    def test_axon__fs_isfile(self, *args, **kwargs):
        self.eq(s_axon.Axon._fs_isfile(None), False)
        self.eq(s_axon.Axon._fs_isfile(0), False)
        self.eq(s_axon.Axon._fs_isfile(33204), True)
        self.eq(s_axon.Axon._fs_isfile(16893), False)

    def test_axon_get_renameprops(self, *args, **kwargs):
        tufo = ('99ac9490ad2e1d4669de1c005a4ec666',
                {'tufo:form': 'axon:path', 'axon:path:st_ctime': 1491191818, 'axon:path:st_mode': 16893,
                 'axon:path:st_atime': 1491191818, 'axon:path': '/dir', 'axon:path:base': 'dir', 'axon:path:dir': '/',
                 'axon:path:st_nlink': 3, 'axon:path:st_mtime': 1491191818, 'axon:path:blob': 32 * 'a'})
        actual = s_axon.Axon._get_renameprops(tufo)

        self.eq(actual['st_nlink'], 3)
        self.eq(actual['st_mode'], 16893)
        self.eq(actual['blob'], 32 * 'a')
        self.gt(actual['st_atime'], 1000000000)
        self.gt(actual['st_ctime'], 1000000000)
        self.gt(actual['st_mtime'], 1000000000)
