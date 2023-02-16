package bitswap_test

import (
	"context"
	"errors"
	"sync"
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

	session := bitswap.New(clientHost, serverHost.ID(), bitswap.Options{})
	blk, err := session.Get(context.Background(), c)
	if err != nil {
		t.Fatalf("should get block, got %v", err)
	}
	if string(blk) != "hello world" {
		t.Fatalf("get didn't succeed")
	}
}
func TestAskRepeated(t *testing.T) {
	serverHost, _ := libp2p.New()
	clientHost, _ := libp2p.New()
	clientHost.Peerstore().AddAddrs(serverHost.ID(), serverHost.Addrs(), time.Hour)

	store := util.NewMemStore(make(map[cid.Cid][]byte))
	c1 := util.Add(store, []byte("hello world"))
	c2 := util.Add(store, []byte("hello world 2"))
	bitswapserver.AttachBitswapServer(serverHost, store)

	session := bitswap.New(clientHost, serverHost.ID(), bitswap.Options{})
	blk1, err := session.Get(context.Background(), c1)
	if err != nil {
		t.Fatalf("should get block, got %v", err)
	}
	if string(blk1) != "hello world" {
		t.Fatalf("get didn't succeed")
	}
	blk2, err := session.Get(context.Background(), c2)
	if err != nil {
		t.Fatalf("should get block, got %v", err)
	}
	if string(blk2) != "hello world 2" {
		t.Fatalf("repeated get didn't succeed")
	}
}

func TestAskMultiple(t *testing.T) {
	serverHost, _ := libp2p.New()
	clientHost, _ := libp2p.New()
	clientHost.Peerstore().AddAddrs(serverHost.ID(), serverHost.Addrs(), time.Hour)

	store := util.NewMemStore(make(map[cid.Cid][]byte))
	c1 := util.Add(store, []byte("hello world"))
	c2 := util.Add(store, []byte("hello world 2"))
	bitswapserver.AttachBitswapServer(serverHost, store)

	session := bitswap.New(clientHost, serverHost.ID(), bitswap.Options{})
	wg := sync.WaitGroup{}
	wg.Add(2)
	var err1 error
	var err2 error
	go func(t *testing.T) {
		blk1, err := session.Get(context.Background(), c1)
		if err != nil {
			err1 = err
		} else if string(blk1) != "hello world" {
			err1 = errors.New("get didn't succeed")
		}
		wg.Done()
	}(t)
	go func(t *testing.T) {
		blk2, err := session.Get(context.Background(), c2)
		if err != nil {
			err2 = err
		}
		if string(blk2) != "hello world 2" {
			err2 = errors.New("2nd didn't get succeed")
		}
		wg.Done()
	}(t)
	wg.Wait()
	if err1 != nil {
		t.Fatal(err1)
	}
	if err2 != nil {
		t.Fatal(err2)
	}
}

func TestBadAsksClose(t *testing.T) {
	serverHost, _ := libp2p.New()
	clientHost, _ := libp2p.New()
	clientHost.Peerstore().AddAddrs(serverHost.ID(), serverHost.Addrs(), time.Hour)

	store := util.NewMemStore(make(map[cid.Cid][]byte))
	_ = util.Add(store, []byte("hello world"))
	otherStore := util.NewMemStore(make(map[cid.Cid][]byte))
	badC := util.Add(otherStore, []byte("not a number"))
	bitswapserver.AttachBitswapServer(serverHost, store)

	session := bitswap.New(clientHost, serverHost.ID(), bitswap.Options{})
	_, err := session.Get(context.Background(), badC)
	if err == nil {
		t.Fatalf("should fail to get a cid not on server")
	}

	conns := clientHost.Network().ConnsToPeer(serverHost.ID())
	for _, c := range conns {
		strs := c.GetStreams()
		for _, s := range strs {
			if s.Protocol() == bitswap.ProtocolBitswap {
				t.Fatal("after failure there shouldn't be an open bitswap conn")
			}
		}
	}
}
