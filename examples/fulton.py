#!/usr/bin/env python

# Mrs Fulton -- run Mrs programs on BYU's supercomputer, Marylou

#PYTHON="/fslapps/Python-2.5.2/bin/python2.5"
PYTHON="python2.5"
MASTER_PORT="4242"

def main():
    parser = create_parser()
    options, args = parser.parse_args()

    if len(args) < 1:
        parser.error('MRS_PROGRAM not specified.')

    mrs_program = args[0]
    mrs_args = args[1:]

    #pbsdsh my_command


USAGE = (""
"""%prog [OPTIONS] -- MRS_PROGRAM [PROGRAM_OPTIONS]

Mrs Fulton uses qsub to submit a Mrs program to the supercomputer.  The given
MRS_PROGRAM runs with the given PROGRAM_OPTIONS.  These options should not
include master or slave subcommands, since Mrs Fulton will take care of these
details.""")

def create_parser():
    from optparse import OptionParser
    parser = OptionParser(usage=USAGE)
    # We don't want options intended for the Mrs Program to go to Mrs Fulton.
    parser.disable_interspersed_args()
    parser.add_option('-N', '--name', dest='name', help='Name of job')
    parser.add_option('-d', '--dir', dest='dir',
            help='Working/output directory')
    #parser.add_option('-l', '--resources', dest='resources',
    #        help='Resource list')
    return parser

if __name__ == '__main__':
    main()

# vim: et sw=4 sts=4
