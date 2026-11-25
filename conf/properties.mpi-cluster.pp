############################################################################
# PEGASUS USER PROPERTIES GENERATED FROM VDS PROPERTY FILE
# ( /home/scec-00/cybershk/pegasus-2.0.0cvs/etc/properties )
# GENERATED AT 2007-05-29T09:40:54-07:00
############################################################################
pegasus.catalog.replica.output = JDBCRC
pegasus.catalog.replica.output.db.driver = sqlite
pegasus.catalog.replica.output.db.url = jdbc:sqlite:/home/shock-ssd/scottcal/workflow/RC.sqlite
pegasus.catalog.replica.db.driver=sqlite
pegasus.catalog.replica.db.url=jdbc\:sqlite\:/home/shock-ssd/scottcal/workflow/RC.sqlite
pegasus.catalog.replica=JDBCRC
pegasus.catalog.site = YAML
pegasus.catalog.site.file = /home/shock/scottcal/runs/config/sites.yaml
pegasus.catalog.transformation = Text
pegasus.catalog.transformation.file = /home/shock/scottcal/runs/config/tc.txt
pegasus.catalog.workflow.url = sqlite:///home/shock-ssd/scottcal/workflow/monitord_test.sqlite
pegasus.clusterer.job.aggregator = MPIExec
pegasus.clusterer.job.aggregator.seqexec.log = false
pegasus.data.configuration = sharedfs
pegasus.integrity.checking = none
pegasus.monitord.events = false
pegasus.dir.submit.mapper = Flat
pegasus.dir.storage.mapper = Replica
pegasus.dir.storage.mapper.replica = Regex
pegasus.dir.storage.mapper.replica.file = /home/shock/scottcal/runs/config/rc.regex.go.output
dagman.maxidle = 200
dagman.maxpre = 2
dagman.retry = 2
pegasus.dir.storage.deep = false
pegasus.dir.useTimestamp = true
pegasus.dir.submit.logs = /scratch
pegasus.exitcode.scope = none
pegasus.gridstart.label = true
pegasus.gridstart.arguments = -q
pegasus.gridstart.kickstart.stat = false
pegasus.transfer.force = true
pegasus.transfer.inter.thirdparty.sites = hpc,mercury,shock,sdsc,abe,ranger
pegasus.transfer.links = true
pegasus.transfer.refiner = Bundle
pegasus.transfer.stageout.priority = 1500
pegasus.transfer.throttle.processes = 1
pegasus.transfer.throttle.streams = 4
pegasus.transfer.threads = 8
pegasus.local.env=X509_USER_PROXY=/home/shock-ssd/scottcal/condor/x509up_u456264
pegasus.catalog.replica.cache.asrc=true
