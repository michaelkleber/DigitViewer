Scanning for All Strings of Digits
========
by Michael Kleber

Code in this directory implements a way to scan through a large file of digits until _every_ sequence of $d$ digits has appeared.

Are you wondering "Does my 10-digit phone number appear in the digits of pi?"
Yes it does, somewhere in the first 241,641,121,048 digits.
What about your 16-digit credit card number?
I don't know — we haven't calculated enough digits of pi to see every 16-digit number.
(Yet.)

## Background

Pi, and many other numbers you can compute with y-cruncher, are believed to be [normal numbers](https://en.wikipedia.org/wiki/Normal_number).
This would mean that every sequence of $d$ decimal digits should appear in it, in approximately $1/(10^d)$ of the possible locations.
(That's what you would expect if the digits were random... and we have every reason to believe that pi's digits behave like random ones _from this particular point of view_.)

That leads to asking the very natural question:
"Out of the $10^d$ sequences of $d$ digits, which one takes the longest to appear, and how many digits does it take?"

* For n=1, the digit 0 is the last one to show up in pi, all the way out at the 32nd place after the decimal point: 3.1415926535897932384626433832795**0**2...
* For n=2 you need to go out to 606 places before you finally see the two-digit sequence 68.
* For n=3,4,5,...,11, you need to go out to 8555, 99849, 1369564, 14118312, 166100506, 1816743912, 22445207406, 241641121048, 2512258603207 digits of pi before you finally see the digit sequence 483, 6716, 33394, 569540, 1075656, 36432643, 172484538, 5918289042, 56377726040 respectively.
    * These are recorded in the [On-line Encyclopedia of Integer Sequences](https://oeis.org/) as entries [A036903](https://oeis.org/A036903) and [A032510](https://oeis.org/A032510).

With 314 trillion random digits, there is around a
[79% chance](https://www.wolframalpha.com/input?i=N%5Bexp%28-n+exp%28-w%2Fn%29%29%5D+where+n+%3D+10%5E13+and+w+%3D+314+trillion)
of seeing all strings of length 13.

## Algorithm

### Basic idea
To search for every string of $d$ digits:
* Make a bitvector of $10^d$ zeros
* Look at strings of $d$ digits one at a time, considered as a $d$-digit number $n$.
    * If the $n$'th bit in the bitstring is a $0$, then you've found a new string!
        * Go you!  Add one to the variable "how many strings I've found so far."
        * If that variable equals $10^d$, you've seen them all!  Have a party.
    *  If the $n$'th bit in the bitstring is already a $1$, nothing to see here, move along.

If you have a lot of digits, a lot of memory, and a lot of time, this will do the job.

If you don't have $10^d$ bits of memory, then you could scan the digits more than once —
"Okay _this_ time I'm going to only pay attention to $d$-digit strings that start with a 7."
This multi-scan idea is not implemented here.  Call a friend with more RAM.

### Parallelization and efficiency
To run this search faster, we use many threads.  We can't have all those threads writing to the same memory at once
(their changes might clobber each other), so we implement a little mapreduce-like arrangement: The mappers each own a
chunk of digits and convert them into d-digit values; the reducers each own a chunk of memory and flip bits from 0 to 1
when the value is seen.  The shuffling between mappers and reducers is implemented by storing the values in an NxN array
of vectors of values, where vector (i,j) holds values produced by mapper i and consumed by reducer j.

We stop that approach when the bitvector is getting close to all 1's, and switch to a new phase where we track the arrival
of the last few thousand strings in a (mutex-guarded) hash map that remembers at what position those strings finally appear.
This lets us keep using many threads and still find out which string took the longest to first show up.

The bitvector phase of the search is sped up by issuing memory prefetch hints, since the CPU spending all its time
asking for randomly-placed individual bits in a very large span of memory is a latency-pessimal access pattern.
The hash map phase uses a quick little Bloom filter to do less hashing.

The cutover point between the two search phases and the memory prefetch hint details are definitely sensitive to what
exact hardware you're running on.  If you plan to use this for large $d$ (say 10 or up), you may profit from tuning
these details to your setup.
