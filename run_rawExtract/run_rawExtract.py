
import sys
import os
import argparse
import subprocess
from math import ceil

PBS_COUNT = 0
RAW_EXTRACT_COMMAND = 'ThermoRawFileParser'

def makePBS(mem, ppn, walltime, wd, fileList):
    global PBS_COUNT
    pbsName = 'rawExtract_{}.pbs'.format(PBS_COUNT)
    _fileLists = getFileLists(ppn, fileList)

    if len(_fileLists) < ppn:
        ppn = len(_fileLists)

    with open(pbsName, 'w') as outF:
        outF.write("#!/bin/bash\n")
        outF.write('#PBS -l mem={}gb,nodes=1:ppn={},walltime={}\n\n'.format(mem, ppn, walltime))
        outF.write('cd {}\n'.format(wd))
        
        for i, l in enumerate(_fileLists):
            outF.write('; '.join(['{} -i {}'.format(RAW_EXTRACT_COMMAND, x) for x in l]))
            outF.write(' > stdout_{}_{}.txt &\n'.format(PBS_COUNT, i))
        outF.write('wait\n')

    PBS_COUNT += 1
    return pbsName


def getPlurality(num):
    if num > 1:
        return 's'
    else: return ''


def getFileLists(nProc, fileList):
    """
    Get input file names and split into a list for each subprocess.

    Args:
        nProc: number of processes per job.
        fileList: list of files.

    Returns:
        List of list containing file to run in each subprocess
    """

    #calculate number of files per thread
    nFiles = len(fileList)
    filesPerProcess = nFiles // nProc
    if nFiles % nProc != 0:
        filesPerProcess += 1

    #split up fileList
    ret = list()
    i = 0
    while(i < nFiles):
        # get beginning and end indecies
        begNum = i
        endNum = begNum + filesPerProcess
        if endNum > nFiles:
            endNum = nFiles

        ret.append(fileList[begNum:endNum])
        i += filesPerProcess

    fileSet = set()
    for i in ret:
        for j in i:
            fileSet.add(j)
    assert(len(fileSet) == len(fileList))

    return ret

def main():

    parser = argparse.ArgumentParser(prog = 'run_rawExtract',
                                     description = 'Run ThermoRawFileParser with pbs jobs.')

    parser.add_argument('-g', '--go', action = 'store_true', default = False,
                        help = 'Should jobs be submitted? If this flag is not supplied, program will be a dry run. '
                               'Required system resources will be printed but jobs will not be submitted.')

    parser.add_argument('-v', '--verbose', action = 'store_true', default = False,
                        help = 'Verbose output.')

    parser.add_argument('-n', '--nJob', type=int, default = 1,
                        help='Specify number of jobs to split into.')

    parser.add_argument('-p', '--ppn', default=4, type=int,
                        help='Number of processors to allocate per PBS job. Default is 4.')

    parser.add_argument('-m', '--mem', default=None, type = int,
                        help = 'Amount of memory to allocate per PBS job in gb. '
                               'Default is 4 times the number of processors per job.')

    parser.add_argument('-w', '--walltime', default='12:00:00',
                        help = 'Walltime per job in the format hh:mm:ss. Default is 12:00:00.')

    parser.add_argument('raw_files', nargs = '+',
                        help = '.raw files to parse.')

    args = parser.parse_args()

    nFiles = len(args.raw_files)
    fileLists = getFileLists(args.nJob, args.raw_files)

    wd = os.getcwd()
    ppn = args.ppn
    mem = args.mem
    if mem is None:
        mem = int(4 * ppn)

    #print summary of resources needed
    sys.stdout.write('\nRequested {} job{} with {} processor{} and {} gb memory each...\n'.format(args.nJob,
                                                                                                  getPlurality(args.nJob),
                                                                                                  args.ppn,
                                                                                                  getPlurality(args.ppn),
                                                                                                  mem))
    # check that requested memory is valid
    if mem > 180 or mem < 1:
        sys.stderr.write('{} is an invalid ammount of job memory!\nExiting...\n'.format(mem))
        exit()

    sys.stdout.write('\t{} raw file{}\n'.format(nFiles, getPlurality(nFiles)))
    if nFiles == 0:
        sys.stderr.write('No raw files specified!\nExiting...\n')
        exit()

    filesPerJob = max([len(x) for x in fileLists])
    sys.stdout.write('\t{} job{} needed\n'.format(len(fileLists), getPlurality(len(fileLists))))
    sys.stdout.write('\t{} file{} per job\n'.format(filesPerJob, getPlurality(filesPerJob)))

    if filesPerJob < ppn:
        ppn = filesPerJob
    sys.stdout.write('\t{} processor{} per job\n'.format(ppn, getPlurality(ppn)))
    filesPerProcess = int(ceil(float(filesPerJob) / float(ppn)))
    sys.stdout.write('\t{} file{} per process\n'.format(ceil(filesPerProcess), getPlurality(filesPerProcess)))

    for i, fileList in enumerate(fileLists):
        pbsName = makePBS(mem, args.ppn, args.walltime, wd, fileList)
        command = 'qsub {}'.format(pbsName)
        if args.verbose:
            sys.stdout.write('{}\n'.format(command))
        if args.go:
            proc = subprocess.Popen([command], cwd=wd, shell=True)
            proc.wait()

if __name__ == '__main__':
    main()

