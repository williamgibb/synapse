import synapse.exc as s_exc
import synapse.common as s_common

import synapse.tests.utils as s_t_utils

class BaseTest(s_t_utils.SynTest):

    async def test_model_base_node(self):

        async with self.getTestCore() as core:

            async with await core.snap() as snap:

                iden = s_common.guid()

                props = {
                    'type': 'hehe haha',
                    'data': ('some', 'data', 'here'),
                }

                node = await snap.addNode('graph:node', iden, props=props)
                self.eq(node.ndef, ('graph:node', iden))
                self.eq(node.get('type'), 'hehe haha')
                self.eq(node.get('data'), ('some', 'data', 'here'))

    async def test_model_base_link(self):

        async with self.getTestCore() as core:

            async with await core.snap() as snap:

                node1 = await snap.addNode('testint', 20)
                node2 = await snap.addNode('teststr', 'foo')

                link = await snap.addNode('graph:link', (node1.ndef, node2.ndef))

                self.eq(link.ndef[1], (('testint', 20), ('teststr', 'foo')))
                self.eq(link.get('n1'), ('testint', 20))
                self.eq(link.get('n1:form'), 'testint')

                self.eq(link.get('n2'), ('teststr', 'foo'))
                self.eq(link.get('n2:form'), 'teststr')

                timelink = await snap.addNode('graph:timelink', (node1.ndef, node2.ndef, '2015'))

                self.eq(timelink.ndef[1], (('testint', 20), ('teststr', 'foo'), 1420070400000))

                self.eq(timelink.get('time'), 1420070400000)

                self.eq(timelink.get('n1'), ('testint', 20))
                self.eq(timelink.get('n1:form'), 'testint')

                self.eq(timelink.get('n2'), ('teststr', 'foo'))
                self.eq(timelink.get('n2:form'), 'teststr')

    async def test_model_base_event(self):

        async with self.getTestCore() as core:

            async with await core.snap() as snap:

                iden = s_common.guid()

                props = {
                    'type': 'HeHe HaHa',
                    'time': '2015',
                    'data': ('some', 'data', 'here'),
                }

                node = await snap.addNode('event', iden, props=props)

                self.eq(node.ndef, ('event', iden))

                self.eq(node.get('type'), 'HeHe HaHa')
                self.eq(node.get('time'), 1420070400000)
                self.eq(node.get('data'), ('some', 'data', 'here'))

    async def test_model_base_edge(self):

        async with self.getTestCore() as core:

            pers = s_common.guid()
            plac = s_common.guid()

            n1def = ('ps:person', pers)
            n2def = ('geo:place', plac)

            async with await core.snap() as snap:

                node = await snap.addNode('has', (n1def, n2def))

                self.eq(node.get('n1'), n1def)
                self.eq(node.get('n1:form'), 'ps:person')

                self.eq(node.get('n2'), n2def)
                self.eq(node.get('n2:form'), 'geo:place')

                node = await snap.addNode('wentto', (n1def, n2def, '2016'))

                self.eq(node.get('time'), 1451606400000)

                self.eq(node.get('n1'), n1def)
                self.eq(node.get('n1:form'), 'ps:person')

                self.eq(node.get('n2'), n2def)
                self.eq(node.get('n2:form'), 'geo:place')

                # repr test
                pode = node.pack(dorepr=True)
                self.isin('"2016/', pode[1].get('repr', ''))

            opts = {'vars': {'pers': pers}}

            await self.agenlen(1, core.eval('ps:person=$pers -> has -> *', opts=opts))
            await self.agenlen(1, core.eval('ps:person=$pers -> has -> geo:place', opts=opts))
            await self.agenlen(0, core.eval('ps:person=$pers -> has -> inet:ipv4', opts=opts))

            await self.agenlen(1, core.eval('ps:person=$pers -> wentto -> *', opts=opts))
            await self.agenlen(1, core.eval('ps:person=$pers -> wentto +:time@=(2014,2017) -> geo:place', opts=opts))
            await self.agenlen(0, core.eval('ps:person=$pers -> wentto -> inet:ipv4', opts=opts))

            opts = {'vars': {'place': plac}}

            await self.agenlen(1, core.eval('geo:place=$place <- has <- *', opts=opts))
            await self.agenlen(1, core.eval('geo:place=$place <- has <- ps:person', opts=opts))
            await self.agenlen(0, core.eval('geo:place=$place <- has <- inet:ipv4', opts=opts))

            # Make a restricted edge and validate that you can only form certain relationships
            copts = {'n1:forms': ('ps:person',), 'n2:forms': ('geo:place',)}
            t = core.model.type('edge').clone(copts)
            norm, info = t.norm((n1def, n2def))
            self.eq(norm, (n1def, n2def))
            self.raises(s_exc.BadTypeValu, t.norm, (n1def, ('testint', 1)))
            self.raises(s_exc.BadTypeValu, t.norm, (('testint', 1), n2def))

            # Make sure we don't return None nodes if one node of an edge is deleted
            node = await core.getNodeByNdef(n2def)
            await node.delete()
            opts = {'vars': {'pers': pers}}
            await self.agenlen(0, core.eval('ps:person=$pers -> wentto -> *', opts=opts))

            # Make sure we don't return None nodes on a PropPivotOut
            opts = {'vars': {'pers': pers}}
            await self.agenlen(0, core.eval('ps:person=$pers -> wentto :n2 -> *', opts=opts))

            # repr test
            async with await core.snap() as snap:
                node = await snap.addNode('refs', (('inet:ipv4', 0x08080808),
                                                   ('teststr', 'beep')))
                pode = node.pack(dorepr=True)
                self.isin('8.8.8.8', pode[1].get('repr', ''))

    async def test_model_base_source(self):

        async with self.getTestCore() as core:

            async with await core.snap() as snap:

                props = {
                    'name': 'FOO BAR',
                    'type': 'osint',
                }

                sorc = await snap.addNode('source', '*', props=props)

                self.eq(sorc.get('type'), 'osint')
                self.eq(sorc.get('name'), 'foo bar')

                valu = (sorc.ndef[1], ('inet:fqdn', 'woot.com'))

                seen = await snap.addNode('seen', valu)

                self.eq(seen.get('source'), sorc.ndef[1])
                self.eq(seen.get('node'), ('inet:fqdn', 'woot.com'))

    async def test_model_base_cluster(self):

        async with self.getTestCore() as core:
            async with await core.snap() as snap:
                guid = s_common.guid()
                props = {'name': 'Test Cluster', 'desc': 'A cluster for testing', 'type': 'similarity'}
                cnode = await snap.addNode('cluster', guid, props)
                self.eq(cnode.get('type'), 'similarity')
                self.eq(cnode.get('name'), 'test cluster')
                self.eq(cnode.get('desc'), 'a cluster for testing')

                # Example reference nodes
                r1 = await snap.addNode('refs', (cnode.ndef, ('teststr', '1234')))
                r2 = await snap.addNode('refs', (cnode.ndef, ('testint', 1234)))

                # Gather up all the nodes in the cluster
                nodes = await snap.eval(f'cluster={guid} | noderefs -d 2 --join').list()
                self.len(5, nodes)
