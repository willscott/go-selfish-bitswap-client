Selfish Bitswap Client
=======================

> A minimal retrieve-only bitswap client

This client implements a minimal bitswap session that allows retrieval of CIDs from a single known peer.

## Documentation

### Usage

```
import (
	bitswap "github.com/willscott/go-selfish-bitswap-client"
)


session := bitswap.New(libp2p.Host, peer.ID)
defer session.Close()
bytes, err := session.Get(cid.Cid)
```

## Lead Maintainer

[willscott](https://github.com/willscott)

## Contributing

Contributions are welcome! This repository is governed by the ipfs [contributing guidelines](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md).

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)
