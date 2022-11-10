package bitswap

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"

	"github.com/ipfs/go-bitswap/message"
	pb "github.com/ipfs/go-bitswap/message/pb"
)

// Session holds state for a related set of CID requests from a single remote peer
type Session struct {
	host.Host
	peer     peer.ID
	initated sync.Once

	close   context.CancelFunc
	conn    network.Stream
	connErr error

	// 1 message sent on the ready chan once the connection is established
	ready chan struct{}

	wants chan cid.Cid
	lbuf  []byte

	interestMtx sync.Mutex
	interests   map[cid.Cid]func([]byte, error)
}

// New initiates a bitswap retrieval session
func New(h host.Host, peer peer.ID) *Session {
	return &Session{
		Host:      h,
		peer:      peer,
		ready:     make(chan struct{}),
		wants:     make(chan cid.Cid, 5),
		lbuf:      make([]byte, binary.MaxVarintLen64),
		interests: make(map[cid.Cid]func([]byte, error)),
	}
}

var (
	// ProtocolBitswapNoVers is a legacy bitswap protocol id
	ProtocolBitswapNoVers protocol.ID = "/ipfs/bitswap"
	// ProtocolBitswapOneZero is the prefix for the legacy bitswap protocol
	ProtocolBitswapOneZero protocol.ID = "/ipfs/bitswap/1.0.0"
	// ProtocolBitswapOneOne is the the prefix for version 1.1.0
	ProtocolBitswapOneOne protocol.ID = "/ipfs/bitswap/1.1.0"
	// ProtocolBitswap is the current version of the bitswap protocol: 1.2.0
	ProtocolBitswap protocol.ID = "/ipfs/bitswap/1.2.0"
)

func (s *Session) connect() {
	sessionCtx, cncl := context.WithCancel(context.Background())
	s.close = cncl
	go s.writeLoop(sessionCtx)

	// todo: configuratble timeout
	ctx, cncl := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cncl()
	stream, err := s.Host.NewStream(ctx, s.peer, ProtocolBitswap, ProtocolBitswapOneZero, ProtocolBitswapOneOne, ProtocolBitswapNoVers)
	s.connErr = err
	s.conn = stream
	if s.connErr != nil {
		return
	}
	s.Host.SetStreamHandler(stream.Protocol(), s.onStream)

	go s.onStream(s.conn)
	s.ready <- struct{}{}
}

func (s *Session) onStream(stream network.Stream) {
	reader := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)

	for {
		received, err := message.FromMsgReader(reader)
		if err != nil {
			if os.IsTimeout(err) {
				continue
			}
			if err == io.EOF {
				return
			}
			s.connErr = err
			s.Close()
			return
		}

		for _, b := range received.Blocks() {
			s.resolve(b.Cid(), b.RawData(), err)
		}
	}
}

// writeLoop is the event loop handling outbound messages
func (s *Session) writeLoop(ctx context.Context) {
	cids := make([]cid.Cid, 0)
	timeout := time.NewTicker(time.Millisecond * 50)
	ready := false
	defer close(s.ready)
	defer timeout.Stop()
	for {
		select {
		case c := <-s.wants:
			cids = append(cids, c)
		case <-s.ready:
			ready = true
			if len(cids) > 0 {
				s.send(cids)
				cids = make([]cid.Cid, 0)
			}
		case <-timeout.C:
			if !ready {
				continue
			}
			if len(cids) > 0 {
				s.send(cids)
				cids = make([]cid.Cid, 0)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Session) send(cids []cid.Cid) error {

	msg := message.New(false)
	for _, c := range cids {
		msg.AddEntry(c, 0, pb.Message_Wantlist_Block, false)
	}

	m := msg.ToProtoV1()
	size := m.Size()

	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	written, err := m.MarshalTo(buf[n:])
	if err != nil {
		return err
	}
	n += written

	_, err = s.conn.Write(buf[:n])
	return err
}

// Close stops the session.
func (s *Session) Close() error {
	if s.close != nil {
		s.close()
	}
	if s.connErr != nil {
		s.interestMtx.Lock()
		defer s.interestMtx.Unlock()
		for _, i := range s.interests {
			i(nil, s.connErr)
		}
		// delete all interests
		s.interests = make(map[cid.Cid]func([]byte, error))
	}
	return nil
}

func (s *Session) on(c cid.Cid, cb func([]byte, error)) {
	s.interestMtx.Lock()
	defer s.interestMtx.Unlock()
	// todo: support multiple
	s.interests[c] = cb
}

func (s *Session) resolve(c cid.Cid, data []byte, err error) {
	s.interestMtx.Lock()
	defer s.interestMtx.Unlock()

	cb, ok := s.interests[c]
	if ok {
		delete(s.interests, c)
		cb(data, err)
	}
}

// Get a specific block of data in this session.
func (s *Session) Get(c cid.Cid) ([]byte, error) {
	// confirm connected.
	s.initated.Do(s.connect)
	if s.connErr != nil {
		return nil, s.connErr
	}
	s.wants <- c

	// wait for want to be handled.
	wg := sync.WaitGroup{}
	wg.Add(1)
	var data []byte
	var err error

	s.on(c, func(rb []byte, re error) {
		data = rb
		err = re
		wg.Done()
	})
	wg.Wait()

	return data, err
}
