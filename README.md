dbsampler
=========
DB Sampler creates small samples of large databases while preserving foreign key constraints.

Dealing with large development/staging databases is unacceptable in an age of cloud services and containers, where personal dev platforms can be spun up in _seconds_ and then discarded. Containers have made it simple for each member of the dev team to work on their own copy of an application, but often the team is sharing a single dev/staging database at a remote location. Those databases often contain old data, and altering them is impossible without effecting everyone else on the team.

Random data generators are a common solution to the problem of creating small testable databases, but they generate data that is usually a poor representation of the real application data, and the generator itself is difficult to create and becomes another piece of software to be maintained. dbsampler solves the problem by creating a snapshot of your real database with a small _sample_ of the _real_ data.