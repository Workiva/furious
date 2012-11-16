Furious - An Asynchronous Workflow Library.
===========================================

Introduction
------------

Furious is a lightweight library that wraps Google App Engine taskqueues to
make building dynamic workflows easy.


Installation
------------
Include `furious` within your App Engine project's libary directory, often
`/lib`.  If that does not make sense to you, simply include the `furious`
subdirectory within your application's root directory (where your `app.yaml`
is located).

Usage
-----

In the simplest form, usage looks like:

    from furious import async

    async(job=("your.module.func", ("pos", "args"), {"kwarg": "too"})).start()

This inserts a task that will make the following call:

    your.module.func("pos", "args", kwarg="too")


### Grouping async jobs

You can group jobs together,

    from furious import async, group

    groups = [async(job=("square_a_number", i)) for i in range(10)]
    group(jobs=groups).start()

### Workflows via Grouping

Grouping allows you to build workflows easily,

    from furious import async, group
    from furious.context import get_results

    def square_a_number(number):
        return number ** number

    def sum_results():
        results = get_results()
        return sum(result.payload for result in results)

    # Build up your jobs.
    groups = [async(job=("square_a_number", i)) for i in range(10)]

    # Group the jobs, capture their return values.
    async_group = group(jobs=groups, capture=True)

    # Once all the jobs have run, call sum_results.
    async_group.on_complete(sum_results)
    async_group.start()

