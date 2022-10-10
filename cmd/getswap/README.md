# Example: getswap

Example demonstrating the use of go-selfish-bitswap-client.

First, we start an IPFS instance.
```sh
# init and start an IPFS node
> ipfs init
> ipfs daemon
```

We get a Multiaddress of the running IPFS instance.
```sh
# get ipfs instance multiaddress
> ipfs id
# select one entry from the "Address" field
# e.g /ip6/::1/udp/4001/quic/p2p/12D3KooWJm3q58KR3GuXWpnxUbYNbazHKMb9a2vtYrEhZzm19f66
```

Then, we create a new file, and add it to IPFS.
```sh
# create a file and pin it to ipfs
> echo Hello world! > hello.txt
> ipfs add hello.txt
added QmXgBq2xJKMqVo8jZdziyudNmnbiwjbpAycy5RbfDBoJRM hello.txt
 13 B / 13 B [================================================] 100.00%
```

Run the selfish bitswap client to retrieve the file you just added to IPFS.
```sh
# from within cmd/getswap
> go run main.go -p <multaddress> <cid>
```


