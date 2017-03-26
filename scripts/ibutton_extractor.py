import collections
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s [%(filename)s:%(funcName)s]')
import synapse.cortex
import synapse.daemon
import synapse.telepath
import synapse.lib.service
import synapse.swarm.runtime
sys.path.append('./scripts')
import ibutton_ingest
# ???
ID = 0
PROPS = 1
# COre
core = synapse.cortex.openurl('sqlite:////Users/wgibb/Documents/projects/synapse/ibutton_data.db')
# a svcbus is an eventbus that multiple services can share and use to communicate with one another.
svcbus = synapse.lib.service.SvcBus()
# a daemon listens on an address and can respond to requests.
# this might spawn a background thread to handle requests? TODO.
dmon = synapse.daemon.Daemon()
# the link has details about the listening daemon.
# things like the ip, port, username, url, etc.
link = dmon.listen('tcp://127.0.0.1:0')
port = link[1].get('port')
# share the event bus that services can use over the daemon via the telepath protocol.
# clients that connect to the daemon can then attach services, and other clients can interact with those services.
# the telepath protocol is ... TODO
dmon.share('thebus', svcbus)
# connect to the remote svcbus via telepath.
# this lets us call methods registered on the remote svcbus as if they were local.
rsvcbus = synapse.telepath.openurl('tcp://127.0.0.1/thebus', port=port)
# attach a cortex to the service bus.
# now any client of the remote service bus can call methods on the cortex.
synapse.lib.service.runSynSvc('thecortex', core, rsvcbus)
# first argument: telepath'd service bus instance
#   this cannot be the core directly. nor a local SvcBus.
swarm = synapse.swarm.runtime.Runtime(rsvcbus)
# Get ibutton data
r = swarm.ask('mss_ibutton')
# Start group the ibutton data into locations
d = collections.defaultdict(list)
for tufo in r.get('data'):
    ibd = ibutton_ingest.undo_tufo(tufo)
    tup = ibd.get(ibutton_ingest.ROW), ibd.get(ibutton_ingest.BLOCK)
    d[tup].append(ibd)
# Stuff
for k, v in d.items():
    v.sort(key=lambda s: s.get('date'))
# Print
pprint(d.get((158, 1))[:2])
# Len
len(d.get((158,1)))
#
# D2 for buttons!
d2 = {}
for k, v in d.items():
  row, block = k
  ibutton = ibutton_ingest.ButtonData()
  for i in v:
   ibutton.append((i.get('date'), i.get('temp')))
  ibutton.metadata[ibutton_ingest.ROW] = row
  ibutton.metadata[ibutton_ingest.BLOCK] = block
  d2[k] = ibutton

# Compute the things
for k, v in d2.items():
    v.analyze_button()

# Write button data to disk
import os
os.makedirs('./datas')
for k, v in d2.items():
    v.write_computed_data_to_files('./datas')
