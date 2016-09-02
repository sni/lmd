LAMPD - Livestatus Acceleration Multi Proxy Daemon
==================================================

**EXPERIMENT - this is just an experiment so far, which means the repository is subject to rebases, force pushes, etc...**

[![Build Status](https://travis-ci.org/sni/lampd.svg?branch=master)](https://travis-ci.org/sni/lampd)

Fetches livestatus data from multiple sources and provides itself a livestatus
api for those sources which makes livestatus querys a lot faster than requesting
them directly from the remote sources.

Map / reduce included to combine response from multiple sources.

<img src="docs/Architecture.png" alt="Architecture" style="width: 600px;"/>


Installation
============

```
    %> go get github.com/sni/lampd
```

Copy lampd.ini.example to lampd.ini and change to your needs. Then run lampd.
You can specify the path to your config file with `--config`.

```
    lampd --config=/etc/lampd/lampd.ini
```

Additional Livestatus Header
----------------------------

### Output Format ###

The default OutputFormat is `wrapped_json` but `json` is also supported.

The `wrapped_json` format will put the normal `json` result in a hash with
some more keys:

    - data: the original result
    - total: the number of matches in the result set _before_ the limit and offset applied
    - failed: a hash of backends which could not be retrieved

### Response Header ###

The only ResponseHeader supported right now is `fixed16`.

### Backends Header ###

There is a new Backends header which may set a space separated list of
backends. If none specific, all are returned.

ex.:

    Backends: id1 id2


### Offset Header ###

The offset header can be used to only retrieve a subset of the complete result
set. Best used together with the sort header.

    Offset: 100
    Limit: 10

This will return entrys 100-109 from the overal result set.


### Sort Header ###

The sort header can be used to sort the results by one or more columns.
Multiple sort header can be used.

    Sort: <column name> <asc/desc>

ex.:

    GET hosts
    Sort: state asc
    Sort: name desc


TODO
----

Some things are still not complete

    - Remove old sockets on exit
    - Stats Querys
    - Implement "GET backends" with status and some statistics
    - send all columns if no column is specified
    - commands handling


Ideas
-----

Some ideas may or may not be implemented in the future

    - Support clustered backends by specifing multiple sources for a connection
    - Cluster the daemon itself to spread out the load, only one last reduce
      would be required to combine the result from all cluster partners
    - Add transparent and half-transparent mode which just handles the map/reduce without cache
