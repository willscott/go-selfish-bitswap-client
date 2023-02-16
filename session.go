package bitswap

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multihash"

	bitswap_message_pb "github.com/willscott/go-selfish-bitswap-client/message"
)

type Bitswap interface {
	Get(ctx context.Context, c cid.Cid) ([]byte, error)
	Close() error
}

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
	interests   map[string]func([]byte, error)
	stimeout    time.Duration
	ttimeout    time.Duration
}

type Options struct {
	SessionTimeout          time.Duration
	WriteAggregationQuantum time.Duration
}

const (
	defaultWriteAggregationQuantum = 50 * time.Millisecond
)

// New initiates a bitswap retrieval session
func New(h host.Host, peer peer.ID, opts Options) *Session {
	if opts.WriteAggregationQuantum == 0 {
		opts.WriteAggregationQuantum = defaultWriteAggregationQuantum
	}
	return &Session{
		Host:      h,
		peer:      peer,
		ready:     make(chan struct{}),
		wants:     make(chan cid.Cid, 5),
		lbuf:      make([]byte, binary.MaxVarintLen64),
		interests: make(map[string]func([]byte, error)),
		stimeout:  opts.SessionTimeout,
		ttimeout:  opts.WriteAggregationQuantum,
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

	logger = log.Logger("bitswap-client")
)

const (
	// maximum block we'll read is 4mb
	MaxBlockSize = 1024 * 1024 * 4
)

func (s *Session) connect() {
	sessionCtx, cncl := context.WithCancel(context.Background())
	s.close = cncl
	go s.writeLoop(sessionCtx)
	ctx := context.Background()
	if s.stimeout != 0 {
		ctx, cncl = context.WithDeadline(context.Background(), time.Now().Add(s.stimeout))
		defer cncl()
	}
	stream, err := s.Host.NewStream(ctx, s.peer, ProtocolBitswap, ProtocolBitswapOneZero, ProtocolBitswapOneOne, ProtocolBitswapNoVers)
	s.connErr = err
	s.conn = stream
	if s.connErr != nil {
		logger.Warnw("could not connect", "peer", s.peer, "err", s.connErr)
		return
	}
	s.Host.SetStreamHandler(stream.Protocol(), s.onStream)

	go s.onStream(s.conn)
	s.ready <- struct{}{}
}

func (s *Session) onStream(stream network.Stream) {
	defer stream.Close()
	buf := make([]byte, 4*1024*1024)
	pos := uint64(0)
	prefixLen := 0
	msgLen := uint64(0)
	for {
		readLen, err := stream.Read(buf[pos:])

		if err != nil {
			if os.IsTimeout(err) {
				continue
			}
			s.connErr = err
			s.Close()
			return
		}
		if msgLen == 0 {
			nextLen, intLen := binary.Uvarint(buf)
			if intLen <= 0 {
				s.connErr = errors.New("invalid message")
				s.Close()
				return
			}
			if nextLen > MaxBlockSize {
				s.connErr = errors.New("too large message")
				s.Close()
				return
			}
			if nextLen > uint64(len(buf)) {
				nb := make([]byte, uint64(intLen)+nextLen)
				copy(nb, buf[:])
				buf = nb
			}
			msgLen = nextLen + uint64(intLen)
			pos = uint64(readLen)
			prefixLen = intLen
		} else {
			pos += uint64(readLen)
		}

		if pos == msgLen {
			if err := s.handle(buf[prefixLen:msgLen]); err != nil {
				s.connErr = fmt.Errorf("invalid block read: %w", err)
				s.Close()
				return
			}
			pos = 0
			prefixLen = 0
			msgLen = 0
		}
	}
}

// writeLoop is the event loop handling outbound messages
func (s *Session) writeLoop(ctx context.Context) {
	cids := make([]cid.Cid, 0)
	timeout := time.NewTicker(s.ttimeout)
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
	m := bitswap_message_pb.Message{}
	m.Wantlist = bitswap_message_pb.Message_Wantlist{}
	for _, c := range cids {
		bc := bitswap_message_pb.Cid{Cid: c}
		m.Wantlist.Entries = append(m.Wantlist.Entries, bitswap_message_pb.Message_Wantlist_Entry{Block: bc})
	}

	bytes, err := m.Marshal()
	if err != nil {
		return err
	}

	ln := binary.PutUvarint(s.lbuf, uint64(len(bytes)))
	if _, err := s.conn.Write(s.lbuf[0:ln]); err != nil {
		return err
	}
	if _, err := s.conn.Write(bytes); err != nil {
		return err
	}
	return nil
}

// Handle an inbound message.
func (s *Session) handle(buf []byte) error {
	m := bitswap_message_pb.Message{}
	if err := m.Unmarshal(buf); err != nil {
		logger.Warnw("failed to parse message as bitswap", "err", err)
		return err
	}
	foundBlocks := 0
	// bitswap 1.1
	for _, bp := range m.Payload {
		prefix, err := cid.PrefixFromBytes(bp.Prefix)
		if err != nil {
			logger.Warnw("failed to parse payload cid", "err", err)
			continue
		}
		c, err := prefix.Sum(bp.GetData())
		if err != nil {
			logger.Warnw("failed to hash payload", "err", err)
			continue
		}
		if err := s.resolve(c, bp.GetData(), nil); err != nil {
			continue
		}
		foundBlocks++
	}
	// bitswap 1.0
	for _, b := range m.Blocks {
		// CIDv0, sha256, protobuf only
		mh, err := multihash.Sum(b, multihash.SHA2_256, -1)
		if err != nil {
			logger.Warnw("failed to hash block", "err", err)
			continue
		}
		c := cid.NewCidV0(mh)
		if err := s.resolve(c, b, nil); err != nil {
			continue
		}
		foundBlocks++
	}
	if foundBlocks == 0 {
		return errors.New("no requested block read")
	}
	return nil
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
	}
	return nil
}

func (s *Session) on(c cid.Cid, cb func([]byte, error)) {
	s.interestMtx.Lock()
	defer s.interestMtx.Unlock()
	// todo: support multiple
	mh := c.Hash().HexString()
	s.interests[mh] = cb
}

func (s *Session) resolve(c cid.Cid, data []byte, err error) error {
	s.interestMtx.Lock()

	mh := c.Hash().HexString()
	cb, ok := s.interests[mh]
	if ok {
		delete(s.interests, mh)
	}

	s.interestMtx.Unlock()

	if ok {
		cb(data, err)
		return nil
	} else {
		return fmt.Errorf("could not resolve block: no callback registered for %s", c)
	}
}

// Get a specific block of data in this session.
// ctx is used to wrap client in timeout logic across a session.
func (s *Session) Get(ctx context.Context, c cid.Cid) ([]byte, error) {
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

	select {
	case <-ctx.Done():
		return data, ctx.Err()
	default:
		return data, err
	}
}
