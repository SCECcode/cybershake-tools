############################################################################
# PEGASUS USER PROPERTIES GENERATED FROM VDS PROPERTY FILE
# ( /home/scec-00/cybershk/pegasus-2.0.0cvs/etc/properties )
# GENERATED AT 2007-05-29T09:40:54-07:00
############################################################################
pegasus.catalog.replica = JDBCRC
pegasus.catalog.replica.db.driver = sqlite
pegasus.catalog.replica.db.url = jdbc:sqlite:/home/shock-ssd/scottcal/workflow/RC.sqlite
pegasus.catalog.site = YAML
pegasus.catalog.site.file = /home/shock/scottcal/runs/config/sites.yaml
pegasus.catalog.transformation = Text
pegasus.catalog.transformation.file = /home/shock/scottcal/runs/config/tc.txt
pegasus.clusterer.job.aggregator = MPIExec
pegasus.clusterer.job.aggregator.seqexec.log = false
pegasus.data.configuration = sharedfs
pegasus.dir.storage.mapper = Replica
pegasus.dir.storage.mapper.replica = Regex
pegasus.dir.storage.mapper.replica.file = /home/shock/scottcal/runs/config/rc.regex.go.output
pegasus.dir.submit.mapper = Flat
pegasus.file.cleanup.scope = fullahead
pegasus.integrity.checking = none
pegasus.monitord.events = true
pegasus.catalog.workflow.url = sqlite:///home/shock-ssd/scottcal/workflow/monitord
dagman.maxidle =  200
dagman.maxpre =  2
dagman.retry = 2
pegasus.dir.storage.deep = false
pegasus.dir.useTimestamp = true
pegasus.dir.submit.logs = /scratch
pegasus.stagein.clusters = 1
pegasus.stageout.clusters = 1
pegasus.exitcode.scope = all
pegasus.gridstart.label = true
pegasus.gridstart.arguments = -q
pegasus.gridstart.kickstart.stat = false
pegasus.schema.ptc = InvocationSchema
pegasus.transfer.force = true
pegasus.transfer.inter.thirdparty.sites = hpc,shock,abe,ranger,kraken,stampede
pegasus.transfer.stagein.thirdparty.sites = ranger,kraken,stampede
# pegasus.transfer.stageout.thirdparty.sites = ranger,kraken,stampede
pegasus.transfer.*.thirdparty.remote = ranger,kraken,stampede
pegasus.transfer.links = true
pegasus.transfer.refiner = Bundle
pegasus.transfer.stageout.thirdparty.sites = *
pegasus.transfer.throttle.processes = 1
pegasus.transfer.throttle.streams = 4
pegasus.transfer.threads = 8
