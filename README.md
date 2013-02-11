Furious - An Asynchronous Workflow Library.
===========================================

Introduction
------------

Furious is a lightweight library that wraps Google App Engine taskqueues to
make building dynamic workflows easy.


Installation
------------

Include `furious` within your App Engine project's library directory, often
`/lib`.  If that does not make sense to you, simply include the `furious`
subdirectory within your application's root directory (where your `app.yaml`
is located).

Usage
-----

In the simplest form, usage looks like:

    from furious import Async

    # Create an Async object.
    async = Async(
        target="your.module.func",
        args=("positional", "args"),
        kwargs={"kwargs": "too"})

    # Tell the async to insert itself to be run.
    async.start()

This inserts a task that will make the following call:

    your.module.func("pos", "args", kwarg="too")


### Grouping async jobs

You can group jobs together,

    from furious import context

    # Instantiate a new Context.
    with context.new() as batch:

        for number in xrange(10):

            # Create a new Async object that will be added to the Context.
            batch.add(target="square_a_number", args=(number,))


### Setting defaults.

It is possible to set options, like the target queue,

    from furious import context, defaults

    @defaults(queue='square')
    def square_a_number(number):
        return number * number

    @defaults(queue='delayed_square', task_args={'countdown': 20})
    def delayed_square(number):
        return number * number

    # Instantiate a new Context.
    with context.new() as batch:

        for number in xrange(10):

            if number % 2:
                # At insert time, the task is added to the 'square' queue.
                batch.add(target="square_a_number", args=(number,))
            else:
                # At insert time, the task is added to the 'delayed_square'
                # queue with a 20 second countdown.
                batch.add(target="delayed_square", args=(number,))


Tasks targeted at the same queue will be batch inserted.


### Workflows via Contexts

*NOTE*: The `Context.on_complete` method is not yet fully implemented.

Contexts allow you to build workflows easily,

    from furious import Async
    from furious.context import get_current_context, new

    def square_a_number(number):
        return number ** number

    def sum_results():
        # Get the current context.
        context = get_current_context()

        # Get an iterator that will iterate over the results.
        results = context.get_results()

        # Do something with the results.
        return sum(result.payload for result in results)

    # Instantiate a new Context.
    with context.new() as batch:

        # Set a function to be called when all async jobs are complete.
        batch.on_complete(sum_results)

        for number in xrange(10):

            # Create a new Async object that will be added to the Context.
            batch.add(target="square_a_number", args=(number,))


### Workflows via nesting

Asyncs and Contexts maybe nested to build more complex workflows.

    from furious import Async
    from furious.context import new

    def do_some_work(number):
        if number > 1000:
            # We're done! Square once more, then return the number.
            return number * number

        # The number is not large enough yet!  Recurse!
        return Async(target="do_some_work", args=(number * number,))

    def all_done():
        from furious.context import get_current_async

        # Get the executing async.
        async = get_current_async()

        # Log the result.  This will actually be the result of the
        # number returned last.
        logging.info("Result is: %d", async.result)

    # Create an Async object.
    async = Async(
        target="do_some_work",
        args=(2,),
        callbacks={'success': all_done})

    # Tell the async to insert itself to be run.
    async.start()

This inserts a task that will keep recursing until the value is large enough,
then it will log the final value.  Nesting may be combined with Contexts to
build powerful fan-out / fan-in flows.



Working Examples
-----

For working examples see `examples/__init__.py`.  To use the examples, start
dev_appserver.py then step through the code and make a request to the
corresponding URLs.

