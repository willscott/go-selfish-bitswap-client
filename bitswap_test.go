package bitswap_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	bitswap "github.com/willscott/go-selfish-bitswap-client"
	bitswapserver "github.com/willscott/go-selfish-bitswap-client/server"
	"github.com/willscott/go-selfish-bitswap-client/server/util"
)

func TestRoundtrip(t *testing.T) {
	serverHost, _ := libp2p.New()
	clientHost, _ := libp2p.New()
	clientHost.Peerstore().AddAddrs(serverHost.ID(), serverHost.Addrs(), time.Hour)

	store := util.NewMemStore(make(map[cid.Cid][]byte))
	c := util.Add(store, []byte("hello world"))
	bitswapserver.AttachBitswapServer(serverHost, store)

	session := bitswap.New(clientHost, serverHost.ID())
	fmt.Printf("making client ask to get.\n")
	blk, err := session.Get(c)
	if err != nil {
		t.Fatalf("should get block, got %v", err)
	}
	if string(blk) != "hello world" {
		t.Fatalf("get didn't succeed")
	}
}
