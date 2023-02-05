# mongonet - mongo wire protocol tools

**a fork of mongodb mongonet for some different trying about go driver**  

The purpose of this library is to have a number of low level tools for building a variety of things.
This is not a driver, though could be turned into one if someone wanted.

There are two examples included:

### straight_proxy
This is a trivial proxy for mongod, no real use today, maybe SSL termination??

## sni_tester
This is a proxy that adds a single command "sni".
This is to help test client sni support.

To Start
   cd cmd/sni_tester
   go run sni_tester.go <path to crt file> <path to key file>

To use
    > db.adminCommand("sni")
    { "sniName" : "local.10gen.cc", "ok" : 1 }

## msg_tester
a low level trying on mongo client send/recv