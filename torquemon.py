#!/usr/bin/env python

"""
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

At any point during a run, the user can input from stdin operations into the program,
which will change some aspect about the run:
    - j<int> will change the number of available job slots to <int>
      ex: 'j45' will change the maximum number of jobs running to 45
    - i<int> will change the check interval to <int>
      ex: 'i45' will change the wait interval to every 45 seconds
    - s will show the qsub commands for the jobs currently running TODO
    - p<int> will show the <int> next jobs on the queue TODO
"""

import sys
import argparse
import subprocess
import time
import select
from datetime import datetime
from multiprocessing import Queue
from multiprocessing.pool import ThreadPool

__author__ = 'Dominic Fitzgerald'
__version__ = '1.1'
__email__ = 'dominicfitzgerald11@gmail.com'

QSUB_JOB_UNSUBMITTED = 0
QSUB_JOB_RUNNING = 1
QSUB_JOB_COMPLETE = 2
QSUB_JOB_ERROR = 3

QSTAT_ID_NOT_IN_QUEUE = 153

CHANGE_JOBS = 'j'
CHANGE_INTERVAL = 'i'


class QsubJob:
    """
    Representation of a job to submit to the torque queue.

    id::str Unique identifier for this job, given after submission to the torque queue
    cmd::str Command which will be executed exactly and entirely as a shell call
    logdir::str Location of torque output logs, defaults to current directory
    state::int Enumeration, corresponds to one of {UNSUBMITTED, RUNNING, COMPLETE, ERROR}
    """
    def __init__(self, id, cmd, logdir, state):
        self.id = id
        self.cmd = cmd
        self.logdir = logdir
        self.state = state


def to_log(msg):
    """
    Formats a date string and outputs a message to stdout.
    :param msg: str Message to log
    """
    time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sys.stdout.write('{} > {}\n'.format(time_now, msg))
    sys.stdout.flush()


def write_restart_file(qsub_jobs, restart_filename):
    """
    Writes out remaining un-completed jobs to the restart file.
    :param qsub_jobs: list<QsubJob> All qsub jobs in this run
    :param restart_filename: str Name of the restart file
    """
    remaining_jobs = [job for job in qsub_jobs
                      if job.state != QSUB_JOB_COMPLETE]
    restart_file = open(restart_filename, 'w')
    restart_file.write(
        '\n'.join(['\t'.join([job.cmd, job.logdir])
                   for job in remaining_jobs]) + '\n'
    )
    restart_file.flush()
    restart_file.close()


def num_jobs_in_state(condition, qsub_jobs):
    """
    Reports the number of jobs in a given state.
    :param condition: int Enumeration, state to query for
    :param qsub_jobs: list<QsubJob> All qsub jobs in this run
    :return: int Number of jobs presently in condition
    """
    return len([job for job in qsub_jobs
                if job.state == condition])


def job_is_running(qsub_job):
    """
    Reports whether a given qsub job is still in the torque queue.
    :param qsub_job: QsubJob The object representing the qusb job
    :return: bool True if job is still in the torque queue
    """
    try:
        subprocess.check_output('qstat {} &>/dev/null'.format(qsub_job.id), shell=True)
        return True
    except subprocess.CalledProcessError as e:
        if e.returncode == QSTAT_ID_NOT_IN_QUEUE:
            return False
        return True


def mark_completed_jobs(qsub_jobs):
    """
    Iterates through all jobs and marks complete those that have been submitted
    and are no longer present in the queue. This may mutate any number of
    QsubJobs' state field!
    :param qsub_jobs: list<QsubJob> All qsub jobs in this run
    """
    running_jobs = [job for job in qsub_jobs
                    if job.state == QSUB_JOB_RUNNING]
    for running_job in running_jobs:
        if not job_is_running(running_job):
            running_job.state = QSUB_JOB_COMPLETE


def poll_for_input():
    """
    This function was modified from this StackOverflow question:
    http://stackoverflow.com/questions/13207678/
    """
    dr, dw, de = select.select([sys.stdin], [], [], 0)
    if not dr == []:
        return sys.stdin.readline()
    return None


def listen_for_input(char_q, kill_q):
    """
    Runs in a separate thread to listen to user interrupts.
    :param char_q: Queue Size-1 queue containing the user interrupt
    :param kill_q: Queue Size-1 queue indicating thread should terminate
    """
    while True:
        if kill_q.full():
            return
        c = poll_for_input()
        if c is not None and char_q.empty():
            char_q.put(c)


def execute_interrupt(interrupt, user_args):
    """
    Given an interrupt, executes the operation. May mutate the user_args dictionary!
    :param interrupt: str User input gathered from stdin
    :param user_args: dict Command line arguments used to control main loop
    """
    op_code, op_val = interrupt[0], interrupt[1:].strip()

    if op_code == CHANGE_JOBS:
        try:
            user_args['jobs'] = int(op_val)
            sys.stderr.write('Changed number of jobs running to {}\n'.format(user_args['jobs']))
        except ValueError:
            sys.stderr.write('Invalid integer value\n')
    elif op_code == CHANGE_INTERVAL:
        try:
            user_args['interval'] = int(op_val)
            sys.stderr.write('Changed check interval to {}\n'.format(user_args['interval']))
        except ValueError:
            sys.stderr.write('Invalid integer value\n')
    else:
        sys.stderr.write('Invalid operation\n')


def main(user_args, char_q):
    # Set restart file filename
    restart_filename = user_args['run_name'] + '.restart.tqm'

    # Read in qsub file, inflate QsubJobs into a list
    qsub_jobs = []
    with open(user_args['qsub_file']) as qsub_file:
        to_log('Reading qsub file into memory')
        for qsub_job in qsub_file:
            try:
                cmd, log_output = qsub_job.strip().split('\t')
            except ValueError:
                cmd, log_output = qsub_job.strip(), '.'
            qsub_jobs.append(
                QsubJob(
                    id=None,
                    cmd=cmd,
                    logdir=log_output,
                    state=QSUB_JOB_UNSUBMITTED
                )
            )

    # Iterate through each QsubJob so each one will eventually be submitted to the queue
    for qsub_job in qsub_jobs:
        # Busy wait here as long as there are no more slots open to job
        # submissions in this run
        while num_jobs_in_state(QSUB_JOB_RUNNING, qsub_jobs) >= user_args['jobs']:
            # Report run stats to the user
            to_log('Running at maximum capacity ({}/{})'.format(
                str(num_jobs_in_state(QSUB_JOB_RUNNING, qsub_jobs)),
                str(user_args['jobs'])
            ))
            to_log('Completed: {}/{}'.format(
                str(num_jobs_in_state(QSUB_JOB_COMPLETE, qsub_jobs)),
                str(len(qsub_jobs))
            ))

            # Wait a specified amount of time
            wait_until = time.time() + user_args['interval']
            while time.time() < wait_until:
                # Check for interrupts
                if char_q.full():
                    execute_interrupt(char_q.get(), user_args)

            # After waiting, check for complete jobs and write out the restart file
            mark_completed_jobs(qsub_jobs)
            write_restart_file(qsub_jobs, restart_filename)

        # If the busy wait fails, that means a slot has opened for submission
        try:
            # Create log directory if it doesn't exist
            subprocess.call(['mkdir', '-p', qsub_job.logdir])

            # Submit job to the queue, capturing the job ID
            qsub_job.id = subprocess.check_output(
                qsub_job.cmd + ' -o {path} -e {path}'.format(
                    path=qsub_job.logdir
                ),
                shell=True
            ).strip()

            # Set this QsubJob to a running state
            qsub_job.state = QSUB_JOB_RUNNING

            # Log to stdout
            to_log('Submitted job {}'.format(qsub_job.id))
            to_log('Running {} out of {} total jobs'.format(
                str(num_jobs_in_state(QSUB_JOB_RUNNING, qsub_jobs)),
                str(len(qsub_jobs))
            ))
        except:
            # If there is any kind of error, log it and move on
            qsub_job.state = QSUB_JOB_ERROR
            to_log('Error submitting job {}'.format(qsub_job.id))
            to_log('Attempted submission command: {}'.format(qsub_job.cmd))

    # When the iteration of qsub_jobs is complete, all jobs have been submitted
    to_log('Winding down... No more jobs to submit')

    # Busy wait as long as jobs are still running
    while num_jobs_in_state(QSUB_JOB_RUNNING, qsub_jobs) > 0:
        # Report run stats to the user
        to_log('Still running {}/{}'.format(
            str(num_jobs_in_state(QSUB_JOB_RUNNING, qsub_jobs)),
            str(user_args['jobs'])
        ))

        # Wait a specified amount of time
        wait_until = time.time() + user_args['interval']
        while time.time() < wait_until:
            # Check for interrupts
            if char_q.full():
                execute_interrupt(char_q.get(), user_args)

        # After waiting, check for complete jobs and write out the restart file
        mark_completed_jobs(qsub_jobs)
        write_restart_file(qsub_jobs, restart_filename)

    # Run is complete, report errors to the user
    to_log('Finished all jobs in run {}'.format(user_args['run_name']))
    to_log('Errors: {}'.format(
        'None' if num_jobs_in_state(QSUB_JOB_ERROR, qsub_jobs) == 0
        else '\n' + '\n'.join(
            [job.cmd for job in qsub_jobs
             if job.state == QSUB_JOB_ERROR]
        )
    ))

if __name__ == '__main__':
    # Get arguments from the user
    parser = argparse.ArgumentParser(prog='Torquemon',
                                     description=('Submits and monitors jobs to the torque job queue ' +
                                                  'system. Provides for batch jobs to be restarted ' +
                                                  'if interrupted mid-run.'))
    parser.add_argument('-q', '--qsub-file', required=True,
                        help=('File containing qusb commands that will be executed in their entirety when ' +
                              'there are available job slots. Optionally, a qsub command can be followed by ' +
                              'a tab character and a folder path where torque log output will be deposited. ' +
                              'If a run is interrupted before all jobs are completed, the residual restart ' +
                              'file can be fed directly into torquemon as a qsub file.'))
    parser.add_argument('-j', '--jobs', type=int, default=10,
                        help='Number of jobs to allow to run simultaneously.')
    parser.add_argument('-i', '--interval', type=int, default=60,
                        help='Interval in seconds to check for completed jobs.')
    parser.add_argument('-n', '--run-name',
                        default=datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
                        help='Name of the run, template used for the restart qsub file.')
    parser.add_argument('--version', action='version', version='%(prog)s {}'.format(__version__))
    user_args = vars(parser.parse_args())

    # Set up thread for interrupts
    char_q, kill_q = Queue(1), Queue(1)
    thread_pool = ThreadPool(processes=1)
    thread_pool.apply_async(listen_for_input, args=(char_q, kill_q))

    # Execute main loop, listening for KeyboardInterrupt to
    # kill the interrupt listening thread
    try:
        main(user_args, char_q)
    except KeyboardInterrupt:
        kill_q.put(1)

    # Close and join interrupt thread
    kill_q.put(1)
    thread_pool.close()
    thread_pool.join()
