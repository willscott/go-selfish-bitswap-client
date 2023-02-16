package bitswapserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	bitswap "github.com/willscott/go-selfish-bitswap-client"
	bitswap_message_pb "github.com/willscott/go-selfish-bitswap-client/message"
)

// accept bitswap streams. return requested blocks. simple

const (
	MaxRequestTimeout = 30 * time.Second
	MaxSendMsgSize    = 3 * 1024 * 1024
)

var (
	ErrNotHave  = errors.New("no requested blocks available")
	ErrOverflow = errors.New("send queue overflow")
)

var logger = log.Logger("bitswap-server")

type Blockstore interface {
	Has(c cid.Cid) (bool, error)
	Get(c cid.Cid) ([]byte, error)
}

func AttachBitswapServer(h host.Host, bs Blockstore) error {
	bsh := handler{bs}
	h.SetStreamHandler(bitswap.ProtocolBitswap, bsh.onStream)
	return nil
}

type handler struct {
	bs Blockstore
}

func (h *handler) onStream(s network.Stream) {
	if err := s.SetReadDeadline(time.Now().Add(MaxRequestTimeout)); err != nil {
		_ = s.Close()
		return
	}
	go h.readLoop(s)
}

func (h *handler) readLoop(stream network.Stream) {
	responder := &streamSender{stream, make(chan []byte, 5)}
	go responder.writeLoop()
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
			if errors.Is(err, io.EOF) {
				return
			}
			//otherwise assume real error / conn closed.
			//s.connErr = err
			stream.Close()
			return
		}
		if msgLen == 0 {
			nextLen, intLen := binary.Uvarint(buf)
			if intLen <= 0 {
				//s.connErr = errors.New("invalid message")
				stream.Close()
				return
			}
			if nextLen > bitswap.MaxBlockSize {
				//s.connErr = errors.New("too large message")
				stream.Close()
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
			if err := h.onMessage(responder, buf[prefixLen:msgLen]); err != nil {
				//s.connErr = fmt.Errorf("invalid block read: %w", err)
				stream.Close()
				return
			}
			pos = 0
			prefixLen = 0
			msgLen = 0
		}
	}
}

func (h *handler) onMessage(ss *streamSender, buf []byte) error {
	m := bitswap_message_pb.Message{}
	if err := m.Unmarshal(buf); err != nil {
		logger.Warnw("failed to parse message as bitswap", "err", err)
		return fmt.Errorf("failed to parse message (len %d) as bitswap: %w", len(buf), err)
	}

	resp := bitswap_message_pb.Message{}
	resp.Wantlist = bitswap_message_pb.Message_Wantlist{}
	filled := 0
	for _, e := range m.Wantlist.Entries {
		if has, err := h.bs.Has(e.Block.Cid); err == nil && has {
			if filled < MaxSendMsgSize {
				data, err := h.bs.Get(e.Block.Cid)
				if err != nil {
					return err
				}
				resp.Blocks = append(resp.Blocks, data)
				filled += len(data)
			} else {
				resp.BlockPresences = append(resp.BlockPresences, bitswap_message_pb.Message_BlockPresence{
					Cid:  e.Block,
					Type: bitswap_message_pb.Message_Have,
				})
			}
		}
	}

	if filled > 0 {
		rBytes, err := resp.Marshal()
		if err != nil {
			return fmt.Errorf("marshal of response failed: %w", err)
		}
		return ss.enqueue(rBytes)
	} else {
		return ErrNotHave
	}
}

type streamSender struct {
	network.Stream
	queue chan []byte
}

func (ss *streamSender) enqueue(msg []byte) error {
	select {
	case ss.queue <- msg:
		return nil
	default:
		return ErrOverflow
	}
}

func (ss *streamSender) writeLoop() {
	next := []byte{}
	for {
		if len(next) > 0 {
			n, err := ss.Stream.Write(next)
			if err != nil {
				return
			}
			next = next[n:]
			continue
		}

		msg, ok := <-ss.queue
		buf := make([]byte, binary.MaxVarintLen64)
		ln := binary.PutUvarint(buf, uint64(len(msg)))
		if _, err := ss.Stream.Write(buf[0:ln]); err != nil {
			return
		}
		next = msg
		if !ok {
			return
		}
	}
}
