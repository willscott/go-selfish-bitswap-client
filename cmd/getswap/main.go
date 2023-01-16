package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
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
	host, err := libp2p.New()
	if err != nil {
		return err
	}
	host.Peerstore().AddAddr(ai.ID, ai.Addrs[0], time.Hour)

	s := bitswap.New(host, ai.ID)
	fmt.Printf("starting get...\n")
	bytes, err := s.Get(cidParsed)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(1)
	}
	_, err = os.Stdout.Write(bytes)
	return err
}
