.. _Tut1_Installation:


***********
Tutorial #1
***********

.. _installation:

Installing Mrs
==============

Mrs is a lightweight but highly scalable implementation of the well known
MapReduce programming paradigm developed at Google. It is written entirely in
Python and was designed for ease of use in such areas as research and education
where the larger more complex implementations, such as Hadoop, are not needed.


Things to consider before beginning
-----------------------------------

    1. Mrs is written in Python, so you will need to have it installed before
       trying to install Mrs.

    2. Although, not necessary to have before installing Mrs, the following
       utilities can be very helpful when running Mrs in parallel:

            -pssh *(http://code.google.com/p/parallel-ssh/)*
            
            -screen *(most Linux distros already have this installed)*



Three minute, three step, installation instructions
---------------------------------------------------

If you are familiar with Python and the Linux environment, you should have no
difficulty installing Mrs and running it with the example wordcount program in
three minutes or less.

    1. Using Git, clone the repository from git://aml.cs.byu.edu/mrs.git,
       or if you prefer, download and unpack the tar file available from
       http://code.google.com/p/mrs-mapreduce/. ::

       > git clone git://aml.cs.byu.edu/mrs.git

    2. Navigate to the MRS_HOME folder and run setup.py as root(see notes below
       if you don't have root access, or you have problems running setup.py). ::

       > sudo python setup.py install

    3. Verify correct instalation by running the provided wordcount program
       from the MRS_HOME/examples directory, passing it an input text file and
       an output directory name. ::

       > python wordcount.py myTextFile.txt myOutputDir

If all went well you should have a file called source_something_something.mtxt
in your output directory containing a list of all the words in the text file
with their respective counts.



Now what?
---------

In the MRS_HOME/tutorials folder you will find three more tutorials to help
you get started running Mrs in parallel both manually and with a run-script,
and an introduction to using some of the more advanced features that Mrs offers.
Also, in the MRS_HOME/examples folder we have included several example files
referenced in the tutorials that we thought you might find useful.



What to do if you don't have root access
========================================

If you don't have root access, or for whatever reason don't want to run the
install, it's not a big problem, you just have to add the MRS_HOME directory
to your python path variable. For example, to add MRS_HOME to your python path
variable, you could use the 'export' command as follows: ::

    > export PYTHONPATH=$PYTHONPATH:~/path_from_home_dir_to_MRS_HOME/

You can do this each time you log in and run Mrs, or you can modify your
terminal configuration file to automatically do it for you. So, if you're
using bash, for example, you would just add the example line to your .bashrc
file.



If you have problems running setup.py:
======================================

You probably don't have setuptools installed. Run the following commands: ::

    > wget 'http://peak.telecommunity.com/dist/ez_setup.py'
    > sudo python ez_setup.py

And then return to step 2. Or, follow the instructions for what to do if you
don't have root access and skip step 2.




