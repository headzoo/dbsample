DBSampler
=========
DBSampler creates small samples of large databases while preserving foreign key constraints.

Dealing with large development/staging databases is unacceptable in an age of cloud services and containers, where personal dev platforms can be spun up in _seconds_ and then discarded. Containers have made it simple for each member of the dev team to work on their own copy of an application, but often the team is sharing a single dev/staging database at a remote location. Those databases often contain old data, and altering them is impossible without effecting everyone else on the team.

Random data generators are a common solution to the problem of creating small testable databases, but they generate data that is usually a poor representation of the real application data, and the generator itself is difficult to create and becomes another piece of software to be maintained. DBSampler solves the problem by creating a snapshot of your real database with a small _sample_ of the _real_ data.

Currently supports MySQL 5+. Other drivers and versions may be supported in the future.

## Usage
```
usage: dbsampler [<flags>] <database>

Flags:
      --help                  Show context-sensitive help (also try --help-long and --help-man).
      --version               Show application version.
  -h, --host="127.0.0.1"      The database host.
  -P, --port="3306"           The database port.
      --protocol="tcp"        The protocol to use for the connection (tcp, socket, pip, memory).
  -u, --user=USER             The database user.
  -p, --pass=PASS             The database password.
      --prompt                Prompt for the database password.
      --routines              Dump procedures and functions.
      --triggers              Dump triggers.
  -l, --limit=100             Max number of rows from each table to dump.
      --skip-create-database  Disable adding CREATE DATABASE statement.
      --skip-lock-tables      Disable locking tables on read.
      --skip-add-drop-table   Disable adding DROP TABLE statements.
      --extended-insert       Use multiple-row INSERT syntax that include several VALUES lists.
      --rename-database=DUMP-NAME  
                              Use this database name in the dump.

Args:
  <database>  Name of the database to dump.

```