LMD - Livestatus Multitool Daemon
=================================

[![Build Status](https://travis-ci.org/sni/lmd.svg?branch=master)](https://travis-ci.org/sni/lmd)

What is this
============

LMD fetches Livestatus data from one or multiple sources and provides:

- A Livestatus api for those sources
- A cache which makes livestatus querys a lot faster than requesting them directly from the remote sources.
- A enhanced livestatus api with more useful output formats and sorting
- Aggregated livestatus results for multiple backends
- A Prometheus exporter for livestatus based metrics for Nagios, Icinga, Shinken and Naemon

So basically this is a "Livestatus In / Livestatus Out" daemon. Its main purpose is to
provide the backend handling of the [Thruk Monitoring Gui](http://www.thruk.org) to a native
compiled fast daemon, but it works for everything which requires livestatus.

<img src="docs/Architecture.png" alt="Architecture" style="width: 600px;"/>

Log table requests and commands are just passed through to the actual backends.


Installation
============

```
    %> go install github.com/sni/lmd/lmd
```

Copy lmd.ini.example to lmd.ini and change to your needs. Then run lmd.
You can specify the path to your config file with `--config`.

```
    lmd --config=/etc/lmd/lmd.ini
```

Configuration
=============

The configuration is explained in the `lmd.ini.example` in detail.
There are several different connection types.

### TCP Livestatus  ###

Remote livestatus connections via tcp can be defined as:

```
    [[Connections]]
    name   = "Monitoring Site A"
    id     = "id1"
    source = ["192.168.33.10:6557"]
```

If the source is a cluster, you can specify multiple addresses like
```
    source = ["192.168.33.10:6557", "192.168.33.20:6557"]
```

### Unix Socket Livestatus  ###

Local unix sockets livestatus connections can be defined as:

```
    [[Connections]]
    name   = "Monitoring Site A"
    id     = "id1"
    source = ["/var/tmp/nagios/live.sock"]
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

- Check backend capabilities on first request
  - Add shinken specific columns
- write huge logfile requests directly into a file


Ideas
-----

Some ideas may or may not be implemented in the future

- Cluster the daemon itself to spread out the load, only one last reduce
  would be required to combine the result from all cluster partners
- Add transparent and half-transparent mode which just handles the map/reduce without cache
- Cache last 24h of logfiles to speed up most logfile requests
- Add REST interface
