Scenario: we are running a small image hosting service, and have a
collection of Apache access log files.

1. Finance needs reporting so they can bill our clients.  For each
client they need to know how many hits were recorded, and the total
number of bytes transferred.  We only charge our clients for successful
image fetches.

2. PM has a new feature request -- they would like to be able to tell
each client the number of unique visitors per day.

3. For capacity planning, Ops would like to know how when the busiest
5-minute interval was (e.g. 12:00-12:05, 12:05-12:10), again by number
of hits and number of bytes transferred.
