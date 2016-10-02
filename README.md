LMD - Livestatus Multitool Daemon
=================================

[![Build Status](https://travis-ci.org/sni/lmd.svg?branch=master)](https://travis-ci.org/sni/lmd)
[![Go Report Card](https://goreportcard.com/badge/github.com/sni/lmd)](https://goreportcard.com/report/github.com/sni/lmd)
[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](http://www.gnu.org/licenses/gpl-3.0)

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


How does it work
================

After starting LMD, it fetches all tables via Livestatus API from all
configured remote backends. It then polls periodically all dynamic parts of the
objects, like host status or plugin or downtime status. When there are no
incoming connections to LMD, it switches into the idle mode with a slower poll
interval. As soon as the first client requests some data, LMD will do a spin
up and run a synchronous update and change back to the normal poll interval.

Usage
=====

If you want to use LMD with Thruk within OMD, see the [omd/lmd
page](https://labs.consol.de/omd/packages/lmd/) for a quick start. The
[OMD-Labs Edition](https://labs.consol.de/omd/) is already prepared to use LMD.


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
============================

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


Resource Usage
==============
The improved performance comes at a price of course. The following numbers should give you a rough idea on what to expect:
An example installation with 200.000 services at a 3 second update interval uses around 1.5gB of memory and 200kByte/s of bandwith.
This makes an average of 7kB memory and 1Byte/s of bandwitdh usage per service.

However your milage may vary, these number heavily depend on the size of the plugin output and the check interval of your services.
Use the Prometheus exporter to create nice graphs to see how your environment differs.

Btw, changing the update interval to 30 seconds does not reduce the used bandwith, you just have to update many services every 30 seconds than small packages every 3 seconds.


Ideas
=====

Some ideas may or may not be implemented in the future

- Cluster the daemon itself to spread out the load, only one last reduce
  would be required to combine the result from all cluster partners
- Add transparent and half-transparent mode which just handles the map/reduce without cache
- Cache last 24h of logfiles to speed up most logfile requests
- Add REST interface
