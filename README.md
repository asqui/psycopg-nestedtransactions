psycopg-nestedtransactions
==========================

Database transaction manager for psycopg2 database connections with seamless support for nested transactions.
 
Most commonly used as a context manager, but can also be used directly in special circumstances.
 
Use like this:
 
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
        # Make atomic changes to a widget across three different tables
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
 
You may also choose to rollback a transaction unconditionally, for example
if you are running in dry-run mode:
 
    dry_run_mode = True
    with Transaction(cxn, discard_changes=dry_run_mode) as txn:
        # Do stuff
    # Transaction is rolled back
    
NB: You cannot explicitly commit or rollback a transaction without exiting the block.
 
    with Transaction(cxn) as txn:
        txn.commit()  # This does not work
        txn.rollback()  # This does not work either
 
Just exit the block without raising to commit. For example:
 
    with Transaction(cxn):
        try:
            # try something that may fail
        except:
            # Handle the failure
    # Transaction is committed
 
In special cases where you cannot use a context manager (e.g. in test code where you want to begin
the transaction in `setUp()` and roll it back in `tearDown()`) you can use the `Transaction`
directly, like this:
 
    def setUp():
        self.txn = Transaction(cxn)  # Transaction begins here
 
    self tearDown():
        self.txn.rollback()  # All test changes are rolled back


Development
-----------

1. Install Postgres: See www.postgresql.org
On Windows, you will need to add the Postgres bin/ directory to your PATH. (e.g. C:/Program Files/PostgreSQL/10/bin)
1. Create a Virtual Environment
1. Install dependencies

    $ bin/pip install -U -e .[test,build]

1. Run the tests. They should all pass.

    $ pytest


Contributors
------------

* Nicole Vavrova
* Nigel Gott
* Daniel Fortunov