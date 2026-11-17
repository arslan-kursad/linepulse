# Kepware, and where it sits in this lab

## A statement first

**The author of this project has not used Kepware.** There has been no hands-on work with it,
no configuration of a Kepware channel, device or tag, and no test of the collector against a
Kepware server. What follows is a description of what such a connectivity layer is for and of
what would change in this code to point it at one. Everything about Kepware itself comes from
published product documentation, not from experience, and should be read that way.

In this lab, the OPC-UA simulator (`sim/opcua_line_server.py`) occupies the position a
connectivity server would occupy. It is not Kepware and does not behave like it in detail.

## What the layer is for

A production line is not one controller. It is controllers from several vendors, each with
its own protocol (Siemens S7, Rockwell EtherNet/IP, Modbus, and others), and nothing about
them is uniform. Software that wants their data has two options: speak every protocol, or put
one layer in between that does, and consume that layer's single, uniform interface.

A connectivity server such as Kepware is that layer. It talks the native protocols to the
controllers and republishes the tags over OPC-UA, so everything above it sees one protocol
and one kind of address space regardless of what is on the floor.

That is the property this project relies on, and it is why the collector is written against
OPC-UA and not against any one controller: the collector cannot tell whether the server it
reads from is a simulator, a connectivity server, or a controller with OPC-UA built in. Only
the address space and the security settings differ.

## Where it would sit here

```
 PLCs ---native protocols---> [ connectivity server ] ---OPC-UA---> collector ---> ...
                              (the simulator, in this lab)
```

## What would have to change to point the collector at a real server

Three things, in increasing order of effort. None has been tried.

1. **The endpoint.** `Collector:OpcUa:EndpointUrl` in `src/LinePulse.Collector/appsettings.json`.
   One value.

2. **Security.** The lab's simulator accepts anonymous, unencrypted connections, and says so
   in its log ("No signing policy available"). A plant server would require a signed and
   encrypted channel, a trusted certificate on each side, and credentials. The collector
   already creates an application certificate in `PkiRoot` and accepts the server's
   certificate for the lab; a deployment would replace "accept any server certificate" with an
   explicit trust list and add a user identity. This is configuration plus a small code change
   in `OpcUaSource`, and it is the part that matters most, because it is the difference between
   a laboratory and something that may be connected to a production network.

3. **The address-space mapping.** This is the real work. The collector finds lines by
   browsing `Objects/<RootObjectName>/<site>/<line>/<tag>` and expects the tag names in
   `MachineTags.cs` (`State`, `GoodCount`, `ScrapCount`, and so on), because that is the
   layout the simulator publishes. A connectivity server organises its tags in its own
   hierarchy, commonly channel, then device, then tag. Mapping that onto site, line and tag
   needs a configurable translation in `OpcUaLineBrowser`, which does not exist today. The
   rest of the pipeline is unaffected, because it only sees the `MachineSample` that the
   collector assembles.

## What it would not change

Everything downstream of the collector: the spool, NATS, deduplication, event-time
bucketing, the KPI calculations. They consume a `MachineSample` and have no idea where it
came from. That separation is deliberate, and is the reason the simulator is an honest
stand-in for the purposes of everything this project demonstrates about reliability. It is
not an honest stand-in for the difficulty of configuring a real controller estate, which this
project does not touch.
