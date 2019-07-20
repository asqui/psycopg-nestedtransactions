psycopg-nestedtransactions
==========================

[![Build Status](https://travis-ci.org/asqui/psycopg-nestedtransactions.svg?branch=master)](https://travis-ci.org/asqui/psycopg-nestedtransactions)

Database transaction manager for psycopg2 database connections with seamless support for nested transactions.

Use it like this:
 
    with Transaction(cxn):
       # do stuff
 
    # Transaction is automatically committed if the block succeeds,
    # and rolled back if an exception is raised out of the block.

Transaction nesting is also supported:
 
    with Transaction(cxn):
        with Transaction(cxn):
            # do stuff
 
This is useful for code composability, for example, if the inner transaction
is actually contained within library code.
 
    updateWidget(cxn, ...):
        """Make atomic changes to a widget across three different tables"""
        with Transaction(cxn):
            cur = cxn.cursor()
            cur.execute(...)  # Update data in widget table
            cur.execute(...)  # Update data in widget_id table
            cur.execute(...)  # Update data in widget_market_data table
        # If any statement fails, all changes to this widget are rolled back
 
    with Transaction(cxn):
        updateWidget(cxn, ...)  # Update widget A
        updateWidget(cxn, ...)  # Update widget B
        # If any widget fails to update, changes to all widgets are rolled back
 
Each transaction acts upon the changes made within that context and dictates
whether those changes are committed or rolled back, with the outermost transaction
being the ultimate arbiter of whether the net changes are committed or rolled back.
 
    with Transaction(cxn):
        for widget in widgets:
            try:
                updateWidget(cxn, widget)
            except:
                # Handle the failure and continue processing other widgets
        # If something else raises here, all changes are rolled back; alternately,
        # if this block exits successfully, all changes are committed at this point (and not before).


Commit and Rollback
-------------------

You may choose to rollback a transaction unconditionally, for example if
you are running in dry-run mode:
 
    dry_run_mode = True
    with Transaction(cxn, force_discard=dry_run_mode) as txn:
        # Do stuff
    # Transaction is rolled back
    
You cannot explicitly commit the transaction without exiting the block.
 
    with Transaction(cxn) as txn:
        txn.commit()  # This does not work

Just exit the block without raising to commit. For example:
 
    with Transaction(cxn):
        try:
            # try something that may fail
        except:
            # Handle the failure
    # Transaction is committed

In addition to the `force_discard` mode, it is also possible to
conditionally rollback inside the block without having to raise:

    with Transaction(cxn) as txn:
        updates = updateWidgets()
        if tooManyUpdates(updates):
            txn.rollback()
            log.warn('Too many updates. Changes rolled-back!')

Note that calling `rollback()` ends your transaction scope immediately.
Any further updates executed after the call to `rollback()` will be
executed outside the scope of this transaction (even if they are still
within the context manager):

    with Transaction(cxn) as txn:
        txn.rollback()
        # Updates made here are equivalent to...
    # ...updates made here.


Composability with classic transaction management
-------------------------------------------------

When introducing the `Transaction` context manager to an existing code
base which uses classic transaction management techniques, the
`Transaction` must be introduced in the innermost levels of code first.

For example, this works as desired:

    updateWidget(cxn, ...):
        """Make atomic changes to a widget across three different tables"""
        with Transaction(cxn):
            cur = cxn.cursor()
            cur.execute(...)  # Update data in widget table
            cur.execute(...)  # Update data in widget_id table
            cur.execute(...)  # Update data in widget_market_data table

    cxn = connect()
    cxn.autocommit = False
    try:
        updateWidget(cxn, ...)  # Update widget A
        updateWidget(cxn, ...)  # Update widget B
    except:
        cxn.rollback()
        raise
    else:
        cxn.commit()
    finally:
        cxn.close()


Note that it is **not** possible to introduce the `Transaction` context
manager at the outermost levels, surrounding code that uses classic
transaction management techniques.

For example, this will not work:

    updateWidget(cxn, ...):
        """This method uses classic transaction management techniques."""
        cxn.autocommit = False
        cur = cxn.cursor()
        try:
            cur.execute(...)  # Update data in widget table
            cur.execute(...)  # Update data in widget_id table
            cur.execute(...)  # Update data in widget_market_data table
        except:
            cxn.rollback()
            raise
        else:
            cxn.commit()

    cxn = connect()
    with Transaction(cxn):
        updateWidget(cxn, ...)  # Update widget A
        updateWidget(cxn, ...)  # Update widget B

Note that in this example, the first call to `updateWidget()` will
result in an explicit call to `commit()` or `rollback()` on the
underlying connection. This will not interact correctly with the
containing `Transaction` context.

Where possible, the `commit()` and `rollback()` methods are patched to
raise an exception for the duration of the `Transaction` context, to
help trap errors such as this.


Reusability and Reentrancy
--------------------------
The `Transaction` context manager is reusable. For example, you can do
this:

    txn = Transaction(cxn)
    with txn:
        # do stuff
    with txn:
        # do more stuff

This seems pointless in such a simple example, but there are other cases
where reusability may be helpful.

The `Transaction` context manager is *not* reentrant. This is not
supported and will not work:

    txn = Transaction(cxn)
    with txn:
        # do stuff
        with txn:  # Don't do this; it will not work!
            pass

(If you have a use case for reentrancy, raise an issue and we can
implement support for it!)


Development
-----------

1.  Install Postgres (See www.postgresql.org)
1.  Create a Virtual Environment:

        $ python3 -m venv psycopg-nestedtransactions
        $ cd psycopg-nestedtransactions

1.  Make `initdb` available in your PATH.
    (The postgres `testing.postgresql` library needs `initdb` to be available in your PATH,
    otherwise you will get `RuntimeError: command not found: initdb` errors).

    The appropriate path will likely be:
    *   On MacOS: `/Library/PostgreSQL/10/bin`
    *   On Linux: `/usr/lib/postgresql/10/bin`
    *   On Windows: `C:/Program Files/PostgreSQL/10/bin`

    Your options include:
    *   Add it to your system PATH (as appropriate for your operating system)
    *   Add it to your your venv `activate` script, with something along these lines:

            # Add postgres to PATH
            PATH="/Library/PostgreSQL/10/bin:${PATH}"
            export PATH

    * Add a symlink to `initd` in `/usr/bin`:

          sudo ln -s /usr/lib/postgresql/9.6/bin/initdb /usr/bin/initdb

1.  Activate the virtual env and install dependencies

        $ source bin/activate
        $ cd /path/to/source/root
        $ bin/pip install -U -e .[test,build]

1.  Run the tests. They should all pass.

       $ pytest


Contributors
------------

* Daniel Fortunov
* Nigel Gott
* Harry Percival
* Nicole Vavrova
