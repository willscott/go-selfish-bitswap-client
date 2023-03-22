package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	tls "github.com/libp2p/go-libp2p/p2p/security/tls"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	"github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	bitswap "github.com/willscott/go-selfish-bitswap-client"
)

func main() {
	app := &cli.App{
		Name:  "getswap",
		Usage: "Bitswap demonstration retrieval client",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "peer",
				Aliases:  []string{"p"},
				Required: true,
			},
		},
		Action: Get,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

const yamuxID = "/yamux/1.0.0"
const mplexID = "/mplex/6.7.0"

func Get(c *cli.Context) error {
	// handle /parse cid
	if c.Args().Len() == 0 {
		return fmt.Errorf("no cid specified")
	}
	cidStr := c.Args().First()
	cidParsed, err := cid.Parse(cidStr)
	if err != nil {
		return err
	}

	// handle / parse peer
	mStr := c.String("peer")
	ma, err := multiaddr.NewMultiaddr(mStr)
	if err != nil {
		return err
	}
	ai, err := peer.AddrInfoFromP2pAddr(ma)
	if err != nil {
		return err
	}

	// make host
	opts := make([]libp2p.Option, 0)
	opts = append([]libp2p.Option{libp2p.Identity(nil)}, opts...)
	opts = append([]libp2p.Option{libp2p.Transport(tcp.NewTCPTransport, tcp.WithMetrics()), libp2p.Transport(quic.NewTransport), libp2p.Transport(websocket.New)}, opts...)
	// add security
	opts = append([]libp2p.Option{libp2p.Security(tls.ID, tls.New), libp2p.Security(noise.ID, noise.New)}, opts...)

	// add muxers
	opts = append([]libp2p.Option{libp2p.Muxer(yamuxID, yamuxTransport()), libp2p.Muxer(mplexID, mplex.DefaultTransport)}, opts...)

	host, err := libp2p.New(opts...)
	if err != nil {
		return err
	}
	host.Peerstore().AddAddr(ai.ID, ai.Addrs[0], time.Hour)

	s := bitswap.New(host, ai.ID, bitswap.Options{})

	// traverse the dag into a car.
	ls := cidlink.DefaultLinkSystem()
	ls.StorageReadOpener = func(ctx linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		c := l.(cidlink.Link).Cid
		blk, err := s.Get(ctx.Ctx, c)
		if err != nil {
			return nil, err
		}
		r := bytes.NewBuffer(blk)
		return r, nil
	}
	_, err = car.TraverseV1(context.Background(), &ls, cidParsed, selectorparse.CommonSelector_ExploreAllRecursively, os.Stdout)
	return err
}

func yamuxTransport() network.Multiplexer {
	tpt := *yamux.DefaultTransport
	tpt.AcceptBacklog = 512
	return &tpt
}
