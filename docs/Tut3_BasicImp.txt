.. _Tut3_BasicImp:

***********
Tutorial #3
***********

.. _writing-your-own:

Writing Your Own Mrs Implimentation:
====================================

The wordcount implementation that we have been working with throught the
previous three tutorials shows a Mrs program in it's most simple form. In this
tutorial, we will try to explain the Basic format for a Mrs MapReduce program
and some of the options for a more complex implimentation.

The Basic format for a Mrs MapReduce program looks something like this ::

    class Mrs-Program(mrs.MapReduce):
    
        def map(key, value):
            yield newkey, newvalue
      
        def reduce(key, values):
            yield newvalue

    if __name__ == '__main__':
        import mrs
        mrs.main(Mrs-Program)

Here we create a class that extends the mapreduce.py file in MRS_HOME/mrs, and
overrides only the required map and reduce functions. The last section of code
(the "if __name__ == '__main__':" section) just passes the program to Mrs
to run.

For example, in the map function in the wordcount program, we take in a chunk of
text as the starting value, break it up into words and yield each word as a new
key with a value of one (found one instance of that word). Then the reduce
function sums all values associated with a particular key (word), and yields
the result.

But what if, instead to reading in just one textfile and counting the words as
we've been doing, you had thousands of files that you wanted to count? This is a
little more complicated, so you would probably want to override the input_data()
function from mapreduce.py, by adding something like the following to your the
wordcount program. (See wordcount2.py in the examples folder.) ::

    def input_data(self, job):
        if len(self.args) < 2:
            print >>sys.stderr, "Requires input(s) and an output."
            return None
        inputs = []
        f = open(self.args[0])
        for line in f:
            inputs.append(line[:-1])
        return job.file_data(inputs)

Now, you can just pass in a single file containing the path to each text file on
a line of the input file, and it will read them all in. (See input_files in the
example folder.)

In the examples folder we have included a file called mapreduce-parse.py as an
example of a much more complex implimentation where there was a need to extend
several of the functions.



Defining More Complex Map Functions:
------------------------------------

Let's look at how to create map functions in more detail. The simple WordCount
mapper is: ::

    def mapper(key, value):
        for word in value.split():
            yield (word, str(1))

This is great for simple examples, but what do you do if you need to do
something special, like initializing the mapper with options before
processing any key-value pairs?  The following example shows how thismay be
done with a factory function that creates mappers: ::

    def MapperFactory(options):
        ignore = options['ignore'].split(', ')
        def f(key, value):
            for word in value.split():
                if word not in ignore:
                    yield (word, str(1))
        return f

The MapReduce system will initialize the mapper with the options specified by
the user. It may do something like the following: ::

    mapper = Mapper({'ignore': 'a, an, the'})

This mapper works the same as the first mapper, except that it ignores the
articles in the list.

An alternative way to approach the problem is to define a class that can be
initialized with the parameters.  The following is equivalent to the
previous example: ::

    class MapperClass:
        def __init__(self, options):
            self.ignore = options['ignore'].split(', ')
        def __call__(self, key, value):
            for word in value.split():
                if word not in self.ignore:
                    yield (word, str(1))

This Mapper class can be instantiated with options: ::

    mapper = Mapper({'ignore': 'a, an, the'})

Of the three different ways just shown to define a map function, the simple
generator is the best way when no initialization or saved state is needed.
The factory function is the best way when initialization is required but no
saved state is needed. And the class is the best way when the generator needs
to modify state while processing input.

