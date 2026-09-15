"""fprime_yamcs.comm: bidirectional bridge between an F Prime endpoint and YAMCS UDP links"""

# Bridge defaults: a TCP server on the F Prime GDS port fed through the TM frame aggregator
DEFAULT_COMMUNICATION = "tcp-fast-server"
DEFAULT_FRAMING = "tm-frame-aggregator"
