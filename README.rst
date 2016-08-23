Torquemon
=========

Monitors a run of many torque jobs, limiting the number of jobs that will run simultaneously.

Torquemon (torque monitor) allows the user to execute a run of many jobs while only ever
actually submitting a defined number of those jobs to a torque queue simultaneously. If
the user has a run of 500 jobs but only ever wants to have 50 running at a time, this monitor
will limit the number of running jobs to 50 and submit more as those original 50 complete
until all 500 jobs have been completed.

The main input into Torquemon is a 'qsub file', which takes the following form:
    - Each line will be a literal qsub command that will be executed in its entirety as a
      shell call.
    - The above line can be optionally followed by a tab character and a path to a location
      where the torque output files should be placed. If this item is omitted, it will
      default to the current directory.

During a run, Torquemon will output and constantly update a 'restart file', which is in
the form of a qsub file. Should anything happen to interrupt a run, this restart file
can be provided directly to a new Torquemon run to pick up exactly where the last run
left off. Note that only jobs which completed fully will be left out of the restart
file; jobs which were in the middle of running will be re-run.
