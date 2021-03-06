import synapse.glob as s_glob
import synapse.lib.cli as s_cli
import synapse.cmds.cron as s_cmds_cron
import synapse.cmds.hive as s_cmds_hive
import synapse.cmds.cortex as s_cmds_cortex
import synapse.cmds.trigger as s_cmds_trigger

cmdsbycell = {
    'cortex': (
        s_cmds_cron.At,
        s_cmds_cron.Cron,
        s_cmds_cortex.Log,
        s_cmds_hive.HiveCmd,
        s_cmds_cortex.PsCmd,
        s_cmds_cortex.KillCmd,
        s_cmds_cortex.StormCmd,
        s_cmds_trigger.Trigger,
    ),
}

async def getItemCmdr(cell, outp=None, **opts):
    '''
    Construct and return a cmdr for the given remote cell.

    Example:

        cmdr = await getItemCmdr(foo)

    '''
    cmdr = s_cli.Cli(cell, outp=outp)
    typename = await cell.getCellType()

    for ctor in cmdsbycell.get(typename, ()):
        cmdr.addCmdClass(ctor)

    return cmdr

def runItemCmdr(item, outp=None, **opts):
    '''
    Create a cmdr for the given item and run the cmd loop.

    Example:

        runItemCmdr(foo)

    '''
    cmdr = s_glob.sync(getItemCmdr(item, outp=outp, **opts))
    cmdr.runCmdLoop()
