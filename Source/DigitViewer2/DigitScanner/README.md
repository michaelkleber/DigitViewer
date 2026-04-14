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

* For d=1, the digit 0 is the last one to show up in pi, all the way out at the 32nd place after the decimal point: 3.1415926535897932384626433832795**0**2...
* For d=2 you need to go out to 606 places before you finally see the two-digit sequence 68.
* When Fabrice Bellard calculated 2.7 trillion digits of pi, he scanned for all sequences up to d=11, reported [here](https://bellard.org/pi/pi2700e9/pidigits.html#:~:text=scan%20decimal%20expansion%20of%20pi) in 2010.
* The scan for d=12 used the code in this directory, running on the [100 trillion digits computed by Google](https://pi.delivery/).
* The scan for d=13 used the code in this directory, running on the [314 trillion digits computed by StorageReview](https://www.storagereview.com/review/storagereview-sets-new-pi-record-314-trillion-digits-on-a-dell-poweredge-r7725).

| d | digits needed        | last d-digit seq |
|:-:|---------------------:|:----------------:|
|  1|                  32  | `0`              |
|  2|                 606  | `68`             |
|  3|               8,555  | `483`            |
|  4|              99,849  | `6716`           |
|  5|           1,369,564  | `33394`          |
|  6|          14,118,312  | `569540`         |
|  7|         166,100,506  | `1075656`        |
|  8|       1,816,743,912  | `36432643`       |
|  9|      22,445,207,406  | `172484538`      |
| 10|     241,641,121,048  | `5918289042`     |
| 11|   2,512,258,603,207  | `56377726040`    |
| 12|  27,261,146,164,637  | `717542605965`   |
| 13| 294,420,436,740,325  | `8683109988379`  |

* These are recorded in the [On-line Encyclopedia of Integer Sequences](https://oeis.org/) as entries [A036903](https://oeis.org/A036903) and [A032510](https://oeis.org/A032510).

For a 50-50 chance of seeing all sequences of 14 digits, you would need
[around 3.26 _quadrillion_](https://www.wolframalpha.com/input?i=N%5Bexp%28-n+exp%28-w%2Fn%29%29%5D+where+n+%3D+10%5E14+and+w+%3D+3.26+quadrillion)
random digits, so don't hold your breath.


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
(their changes might clobber each other), so we implement a little mapreduce-like arrangement: The mapper threads each own a
chunk of digits and convert them into d-digit values; the reducer threads each own a chunk of memory and flip bits from 0 to 1
when the value is seen.  The shuffling between mappers and reducers is implemented by storing the values in an NxN array
of vectors of values, where vector (i,j) holds values produced by mapper i and consumed by reducer j.

We stop that approach when the bitvector is getting close to all 1's, and switch to a new phase where we track the arrival
of the last few thousand strings in a (mutex-guarded) hash map that remembers at what position those strings finally appear.
This lets us keep using many threads and still find out which string took the longest to first show up.

The bitvector phase of the search is sped up by issuing memory prefetch hints, since the CPU spending all its time
asking for randomly-placed individual bits in a very large span of memory is a latency-pessimal access pattern.
The hash map phase uses a quick little Bloom filter to do less hashing.

The cutover point between the two search phases, the memory prefetch hint details, and the number of threads to use
are definitely sensitive to what exact hardware you're running on.  If you plan to run this code for large $d$
(say 10 or up), you may profit from tuning these to your setup.
