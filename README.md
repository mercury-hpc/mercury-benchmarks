== Build instructions:
- build BMI shared libraries
- build CCI shared libraries
  (downloads at http://cci-forum.com/?page_id=46)
- build mercury shared libraries
- set PKG_CONFIG_PATH to point at mercury's pkgconfig path
- set LD_LIBRARY_PATH to point at BMI's, CCI's library paths
- run make with appropriate CFLAGS (e.g. -O3)

== benchmark descriptions

hg-ctest1
- looking at client-side concurrent issuance of data, rpc's
- use implicit bulk context created by HG_Init, issue bulk
  push followed by RPC call to (optionally different) servers N times

hg-ctest2
- concurrent rpc/bulk ops in different threads
- uses different HG contexts backed by a single NA context
- benchmark runs a fixed amount of time, each thread issues back to back calls

hg-ctest3
- single-threaded version of hg-ctest2 (same HG context, same thread executing
  rpc / bulk calls in whatever order HG_Trigger hands callbacks out)

hg-ctest4
- N client processes, 1 server process, each single-threaded. Clients can be
  configured in RPC or bulk xfer mode. 

== Running

general
- when running the server, it's useful to fix the port the server attaches to.
  For that purpose, use CCI_CONFIG=cci-serv.conf (modify to your purposes)
  when running ./hg-ctestX server.
  - alternatively, the server spits out it's address to a file, so you can also
    script against that.
- run programs without arguments to see usage instructions

== Scratch notes
- CCI ignores mercury URIs passed in, but Mercury must be able to correctly
  parse the URI input. So just use a dummy like cci+tcp://foobar as the URI for
  the server (the client of course needs to provide the resulting URI)
