package dht

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/go-ipns"
	internalConfig "github.com/libp2p/go-libp2p-kad-dht/internal/config"
	record_pb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	pb "github.com/ipfs/go-ipns/pb"
	//logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multihash"
)

var (
	PublishLogger    *log.Logger
	ErrPublishLogger *log.Logger
	ResolveLogger    *log.Logger
	ErrResolveLogger *log.Logger
	cPeers           cachedPeers
)

func init() {
	pubFile, err := os.OpenFile("publish.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	resFile, err := os.OpenFile("resolve.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}

	PublishLogger = log.New(pubFile, "INFO: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	ErrPublishLogger = log.New(pubFile, "ERROR: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	ResolveLogger = log.New(resFile, "INFO: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)
	ErrResolveLogger = log.New(resFile, "ERROR: ", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	cPeers = cachedPeers{
		peers:    nil,
		validity: time.Now(),
		ctx:      nil,
		cancel:   nil,
	}
}

type cachedPeers struct {
	peers    []peer.ID          // the top K peers at the end of the query in a publish
	validity time.Time          // the time at which the peers are no longer valid
	ctx      context.Context    // the context of the query
	cancel   context.CancelFunc // the cancel function of the query
}

// This file implements the Routing interface for the IpfsDHT struct.

// Basic Put/Get

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *IpfsDHT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	logger.Debugw("putting value", "key", internal.LoggableRecordKeyString(key))

	// don't even allow local users to put bad values.
	if err := dht.Validator.Validate(key, value); err != nil {
		return err
	}

	old, err := dht.getLocal(ctx, key)
	if err != nil {
		// Means something is wrong with the datastore.
		return err
	}

	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.Validator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dht.putLocal(ctx, key, rec)
	if err != nil {
		return err
	}

	if ctx.Value("ipns") != nil {

		foundPeers := false
		peers := cPeers.peers

		if peers == nil || time.Now().After(cPeers.validity) {
			//valeLogs
			t1 := time.Now()
			PublishLogger.Println("ID:", ctx.Value("id"), "Getting closest peers to recordKey", internal.LoggableRecordKeyString(key))
			//change the context value, so I can print only things that are relevant to the putValue process
			ctx = context.WithValue(ctx, "process", "putValue")
			ctx = context.WithValue(ctx, "time", t1)

			//finding new peers
			peers, err = dht.GetClosestPeers(ctx, key)

			if err != nil {
				ErrPublishLogger.Println("ID:", ctx.Value("id"), "Failed getting closest peers to recordKey", internal.LoggableRecordKeyString(key))
				return err
			}

			foundPeers = true
			//valeLogs
			PublishLogger.Println("ID:", ctx.Value("id"), "Found closest peers to key", internal.LoggableRecordKeyString(key), peers)

		} else {
			//valeLogs
			PublishLogger.Println("ID:", ctx.Value("id"), "Using cached closest peers to key", internal.LoggableRecordKeyString(key), peers)
			cPeers.cancel()
		}

		// Publish to all 20 peers
		intersect := 20

		//We will wait until 5 successful putValue operations to return
		sucAnswers := make(chan bool)

		t1 := time.Now()
		for i := 0; i < intersect; i++ {
			p := peers[i]
			go func(p peer.ID, i int) {
				nCtx, cancel := context.WithCancel(context.Background())
				defer cancel()

				routing.PublishQueryEvent(nCtx, &routing.QueryEvent{
					Type: routing.Value,
					ID:   p,
				})

				err := dht.protoMessenger.PutValue(nCtx, p, rec)
				if err != nil {
					logger.Debugf("failed putting value to peer: %s", err)

					//valeLogs
					ErrPublishLogger.Println("ID:", ctx.Value("id"), "Failed putting value to peer", p, err)
					sucAnswers <- false

				} else {
					PublishLogger.Println("ID:", ctx.Value("id"), "PutValue to peer", p, "took", time.Since(t1))
					sucAnswers <- true
				}
			}(p, i)
		}
		responsesNeeded := 0
		totalAnswers := 0
		done := false

		for !done {
			if <-sucAnswers {
				responsesNeeded++
				totalAnswers++
			} else {
				totalAnswers++
			}

			if responsesNeeded >= 5 || totalAnswers >= intersect {
				done = true
				go func(sucAnswers chan bool, totalAnswers int, intersect int) {
					for totalAnswers < intersect {
						<-sucAnswers
						totalAnswers++
					}
					close(sucAnswers)
				}(sucAnswers, totalAnswers, intersect)
			}
		}

		go func() {
			if cPeers.peers != nil {
				cPeers.cancel()
			}
			cPeers.ctx, cPeers.cancel = context.WithCancel(context.Background())
			cPeers.ctx = context.WithValue(cPeers.ctx, "id", ctx.Value("id"))
			cPeers.ctx = context.WithValue(cPeers.ctx, "ipns", true)

			var newPeers []peer.ID

			e := new(pb.IpnsEntry)
			if err := proto.Unmarshal(rec.Value, e); err != nil {
				ErrPublishLogger.Println("ID:", cPeers.ctx.Value("id"), "Failed to unmarshal record:", err)
				return
			}

			validity, err := u.ParseRFC3339(string(e.GetValidity()))
			if err != nil {
				ErrPublishLogger.Println("Failed to parse validity:", err)
				return
			}

			if !foundPeers {

				t1 := time.Now()
				PublishLogger.Println("ID:", cPeers.ctx.Value("id"), "Concurrently getting closest peers to recordKey", internal.LoggableRecordKeyString(key))
				//change the context value, so I can print only things that are relevant to the putValue process
				cPeers.ctx = context.WithValue(cPeers.ctx, "process", "putValue")
				cPeers.ctx = context.WithValue(cPeers.ctx, "time", t1)

				newPeers, err = dht.GetClosestPeers(cPeers.ctx, key)

				if err != nil {
					ErrPublishLogger.Println("ID:", cPeers.ctx.Value("id"), "Failed getting closest peers to recordKey", internal.LoggableRecordKeyString(key), err)
					return
				}

				PublishLogger.Println("ID:", cPeers.ctx.Value("id"), "Found closest peers to key", internal.LoggableRecordKeyString(key), peers)

				//ctx and not cPeers.ctx bcs we need the privKey
				trashRecord, err := copyTrashRecord(e, key, ctx)
				if err != nil {
					ErrPublishLogger.Println("Failed to create the TRASH record:", err)
					return
				}

				//cPeers -> previously cached closest peers
				//newPeers -> new closest peers

				//publishing updated ones
				for _, p := range newPeers {
					if !contains(p, cPeers.peers) {
						go dht.updateRecord(p, cPeers.ctx, rec, false)
					}
				}

				//publishing trash ones
				for _, p := range cPeers.peers {
					if !contains(p, newPeers) {
						go dht.updateRecord(p, cPeers.ctx, trashRecord, true)
					}
				}

			} else {

				newPeers = peers
			}

			cPeers.validity = validity
			cPeers.peers = append(cPeers.peers, newPeers...)

		}()

	} else {

		peers, err := dht.GetClosestPeers(ctx, key)
		if err != nil {
			return err
		}

		wg := sync.WaitGroup{}
		for _, p := range peers {
			wg.Add(1)
			go func(p peer.ID) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				defer wg.Done()
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type: routing.Value,
					ID:   p,
				})

				err := dht.protoMessenger.PutValue(ctx, p, rec)
				if err != nil {
					logger.Debugf("failed putting value to peer: %s", err)

				}
			}(p)
		}
		wg.Wait()
	}
	/**
		peers, err := dht.GetClosestPeers(ctx, key)
		if err != nil {
			if ctx.Value("ipns") != nil {
				ErrPublishLogger.Println("ID:", ctx.Value("id"), "Failed getting closest peers to recordKey", internal.LoggableRecordKeyString(key))
			}
			return err
		}

		//valeLogs
		if ctx.Value("ipns") != nil {
			PublishLogger.Println("ID:", ctx.Value("id"), "Found closest peers to key", internal.LoggableRecordKeyString(key), peers)
		}

		wg := sync.WaitGroup{}
		for _, p := range peers {
			wg.Add(1)
			go func(p peer.ID) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				defer wg.Done()
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type: routing.Value,
					ID:   p,
				})

				err := dht.protoMessenger.PutValue(ctx, p, rec)
				if err != nil {
					logger.Debugf("failed putting value to peer: %s", err)

					//valeLogs
					if ctx.Value("ipns") != nil {
						ErrPublishLogger.Println("ID:", ctx.Value("id"), "Failed putting value to peer", p, err)
					}
				}
			}(p)
		}
		wg.Wait()
	**/
	return nil
}

func contains(id peer.ID, peers []peer.ID) bool {
	for _, p := range peers {
		if p == id {
			return true
		}
	}
	return false
}

func (dht *IpfsDHT) updateRecord(p peer.ID, ctX context.Context, record *record_pb.Record, trash bool) {

	t := time.Now()
	ctx, cancel := context.WithCancel(ctX)
	defer cancel()

	routing.PublishQueryEvent(ctx, &routing.QueryEvent{
		Type: routing.Value,
		ID:   p,
	})

	err := dht.protoMessenger.PutValue(ctx, p, record)
	if err != nil {
		logger.Debugf("failed putting value to peer: %s", err)

		//valeLogs
		ErrPublishLogger.Println("ID:", ctx.Value("id"), "Failed putting value to peer", p, err)
	} else {
		if trash {
			PublishLogger.Println("ID:", ctx.Value("id"), "PutValue TRASH to peer", p, "took", time.Since(t))
		} else {
			PublishLogger.Println("ID:", ctx.Value("id"), "PutValue UPDATE to peer", p, "took", time.Since(t))
		}
	}

	return
}

func copyTrashRecord(rec *pb.IpnsEntry, key string, ctx context.Context) (*record_pb.Record, error) {
	pKey := ctx.Value("pKey").(crypto.PrivKey)

	validity, err := u.ParseRFC3339(string(rec.GetValidity()))
	if err != nil {
		ErrPublishLogger.Println("Failed to parse validity:", err)
		return nil, err
	}

	// Create record with 1s TTL
	entry, err := ipns.Create(pKey, rec.GetValue(), rec.GetSequence(), validity, time.Second)
	if err != nil {
		ErrPublishLogger.Println("Failed to create record:", err)
		return nil, err
	}

	copyRec, err := proto.Marshal(entry)
	if err != nil {
		ErrPublishLogger.Println("Failed to marshal record:", err)
		return nil, err
	}

	pubRec := record.MakePutRecord(key, copyRec)

	return pubRec, err
}

// recvdVal stores a value and the peer from which we got the value.
type recvdVal struct {
	Val  []byte
	From peer.ID
}

// GetValue searches for the value corresponding to given Key.
func (dht *IpfsDHT) GetValue(ctx context.Context, key string, opts ...routing.Option) (_ []byte, err error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	// apply defaultQuorum if relevant
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, Quorum(internalConfig.GetQuorum(&cfg)))

	responses, err := dht.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	var best []byte

	for r := range responses {
		best = r
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)

	return best, nil
}

// SearchValue searches for the value corresponding to given Key and streams the results.
func (dht *IpfsDHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = internalConfig.GetQuorum(&cfg)

	}

	//valeLogs
	ctx = context.WithValue(ctx, "process", "searchValue")
	ctx = context.WithValue(ctx, "time", time.Now())

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key, stopCh)

	out := make(chan []byte)
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
		if best == nil || aborted {
			return
		}

		updatePeers := make([]peer.ID, 0, dht.bucketSize)
		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}

			for _, p := range l.peers {
				if _, ok := peersWithBest[p]; !ok {
					updatePeers = append(updatePeers, p)
				}
			}
		case <-ctx.Done():
			return
		}

		dht.updatePeerValues(dht.Context(), key, best, updatePeers)
	}()

	return out, nil
}

func (dht *IpfsDHT) searchValueQuorum(ctx context.Context, key string, valCh <-chan recvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v recvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v.Val:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && numResponses > nvals {
				close(stopCh)
				return true
			}
			return false
		})
}

func (dht *IpfsDHT) processValues(ctx context.Context, key string, vals <-chan recvdVal,
	newVal func(ctx context.Context, v recvdVal, better bool) bool) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
loop:
	for {
		if aborted {
			return
		}

		select {
		case v, ok := <-vals:
			if !ok {
				break loop
			}

			// Select best value
			if best != nil {
				if bytes.Equal(best, v.Val) {
					peersWithBest[v.From] = struct{}{}
					aborted = newVal(ctx, v, false)
					continue
				}
				sel, err := dht.Validator.Select(key, [][]byte{best, v.Val})
				if err != nil {
					logger.Warnw("failed to select best value", "key", internal.LoggableRecordKeyString(key), "error", err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, v, false)
					continue
				}
			}
			peersWithBest = make(map[peer.ID]struct{})
			peersWithBest[v.From] = struct{}{}
			best = v.Val
			aborted = newVal(ctx, v, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

func (dht *IpfsDHT) updatePeerValues(ctx context.Context, key string, val []byte, peers []peer.ID) {
	fixupRec := record.MakePutRecord(key, val)
	for _, p := range peers {
		go func(p peer.ID) {
			// TODO: Is this possible?
			if p == dht.self {
				err := dht.putLocal(ctx, key, fixupRec)
				if err != nil {
					logger.Error("Error correcting local dht entry:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()
			t1 := time.Now()
			err := dht.protoMessenger.PutValue(ctx, p, fixupRec)
			t2 := time.Since(t1)
			if err != nil {
				logger.Debug("Error correcting DHT entry: ", err)
				if ctx.Value("process") == "searchValue" && ctx.Value("ipns") != nil {
					ErrResolveLogger.Println("Failed to update peer", p, "with more recent record, took", t2, "error:", err)
				}
			}

			if ctx.Value("process") == "searchValue" && ctx.Value("ipns") != nil {
				ResolveLogger.Println("Updated peer", p, "with more recent record, took:", t2)
			}
		}(p)
	}
}

func (dht *IpfsDHT) getValues(ctx context.Context, key string, stopQuery chan struct{}) (<-chan recvdVal, <-chan *lookupWithFollowupResult) {

	valCh := make(chan recvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	logger.Debugw("finding value", "key", internal.LoggableRecordKeyString(key))

	if rec, err := dht.getLocal(ctx, key); rec != nil && err == nil {
		select {
		case valCh <- recvdVal{
			Val:  rec.GetValue(),
			From: dht.self,
		}:
		case <-ctx.Done():
		}
	}

	times := uint64(0)

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		lookupRes, err := dht.runLookupWithFollowup(ctx, key,
			func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
				// For DHT query command
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type: routing.SendingQuery,
					ID:   p,
				})

				//valeLogs
				t1 := time.Now()

				rec, peers, err := dht.protoMessenger.GetValue(ctx, p, key)
				if err != nil {
					return nil, err
				}

				t2 := time.Since(t1)

				// For DHT query command
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type:      routing.PeerResponse,
					ID:        p,
					Responses: peers,
				})

				if rec == nil {
					return peers, nil
				}

				val := rec.GetValue()
				if val == nil {
					logger.Debug("received a nil record value")
					if ctx.Value("process") == "searchValue" && ctx.Value("ipns") != nil {
						ErrResolveLogger.Println("ID:", ctx.Value("id"), "Received a nil record value and took", t2, "to process")
					}
					return peers, nil
				}

				//after this I know that the record is valid
				if err := dht.Validator.Validate(key, val); err != nil {
					// make sure record is valid
					logger.Debugw("received invalid record (discarded)", "error", err)
					if ctx.Value("process") == "searchValue" && ctx.Value("ipns") != nil {
						ErrResolveLogger.Println("ID:", ctx.Value("id"), "Received invalid record (discarded)", err)
					}
					return peers, nil
				}

				if ctx.Value("process") == "searchValue" && ctx.Value("ipns") != nil {
					//aqui vou ter que processar o record
					e := new(pb.IpnsEntry)
					if err := proto.Unmarshal(val, e); err != nil {
						ErrResolveLogger.Println("ID:", ctx.Value("id"), "Failed to unmarshal record:", err)
						return peers, nil
					}

					validity, err := u.ParseRFC3339(string(e.GetValidity()))
					if err != nil {
						ErrResolveLogger.Println("Failed to parse validity:", err)
						return peers, nil
					}
					ResolveLogger.Println("ID:", ctx.Value("id"), "Received", times, "recordKey", internal.LoggableRecordKeyString(key), "version", e.GetSequence(), "validity", validity.Format("2006/01/02 15:04:05"), "from", p.String(), "took", t2, "found after", time.Since(ctx.Value("time").(time.Time)))
					atomic.AddUint64(&times, 1)

				}

				// the record is present and valid, send it out for processing
				select {
				case valCh <- recvdVal{
					Val:  val,
					From: p,
				}:
				case <-ctx.Done():
					return nil, ctx.Err()
				}

				return peers, nil
			},
			func() bool {
				select {
				case <-stopQuery:
					return true
				default:
					return false
				}
			},
		)

		if err != nil {
			return
		}
		lookupResCh <- lookupRes

		if ctx.Err() == nil {
			dht.refreshRTIfNoShortcut(kb.ConvertKey(key), lookupRes)
		}
	}()

	return valCh, lookupResCh
}

func (dht *IpfsDHT) refreshRTIfNoShortcut(key kb.ID, lookupRes *lookupWithFollowupResult) {
	if lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(key, time.Now())
	}
}

// Provider abstraction for indirect stores.
// Some DHTs store values directly, while an indirect store stores pointers to
// locations of the value, similarly to Coral and Mainline DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) Provide(ctx context.Context, key cid.Cid, brdcst bool) (err error) {
	if !dht.enableProviders {
		return routing.ErrNotSupported
	} else if !key.Defined() {
		return fmt.Errorf("invalid cid: undefined")
	}
	keyMH := key.Hash()
	logger.Debugw("providing", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	// add self locally
	dht.providerStore.AddProvider(ctx, keyMH, peer.AddrInfo{ID: dht.self})
	if !brdcst {
		return nil
	}

	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// timed out
			return context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// Reserve 10% for the final put.
			deadline = deadline.Add(-timeout / 10)
		} else {
			// Otherwise, reserve a second (we'll already be
			// connected so this should be fast).
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	peers, err := dht.GetClosestPeers(closerCtx, string(keyMH))
	switch err {
	case context.DeadlineExceeded:
		// If the _inner_ deadline has been exceeded but the _outer_
		// context is still fine, provide the value to the closest peers
		// we managed to find, even if they're not the _actual_ closest peers.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return err
	}

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			logger.Debugf("putProvider(%s, %s)", internal.LoggableProviderRecordBytes(keyMH), p)
			err := dht.protoMessenger.PutProvider(ctx, p, keyMH, dht.host)
			if err != nil {
				logger.Debug(err)
			}
		}(p)
	}
	wg.Wait()
	if exceededDeadline {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}

// FindProviders searches until the context expires.
func (dht *IpfsDHT) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported
	} else if !c.Defined() {
		return nil, fmt.Errorf("invalid cid: undefined")
	}

	var providers []peer.AddrInfo
	for p := range dht.FindProvidersAsync(ctx, c, dht.bucketSize) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes. If count is zero then the query will run until it
// completes. Note: not reading from the returned channel may block the query
// from progressing.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	if !dht.enableProviders || !key.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	chSize := count
	if count == 0 {
		chSize = 1
	}
	peerOut := make(chan peer.AddrInfo, chSize)

	keyMH := key.Hash()

	logger.Debugw("finding providers", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))
	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, peerOut)
	return peerOut
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, peerOut chan peer.AddrInfo) {
	defer close(peerOut)

	findAll := count == 0

	ps := make(map[peer.ID]struct{})
	psLock := &sync.Mutex{}
	psTryAdd := func(p peer.ID) bool {
		psLock.Lock()
		defer psLock.Unlock()
		_, ok := ps[p]
		if !ok && (len(ps) < count || findAll) {
			ps[p] = struct{}{}
			return true
		}
		return false
	}
	psSize := func() int {
		psLock.Lock()
		defer psLock.Unlock()
		return len(ps)
	}

	provs, err := dht.providerStore.GetProviders(ctx, key)
	if err != nil {
		return
	}
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if psTryAdd(p.ID) {
			select {
			case peerOut <- p:
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, don't bother with remote RPC
		// TODO: is this a DOS vector?
		if !findAll && len(ps) >= count {
			return
		}
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(key),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			provs, closest, err := dht.protoMessenger.GetProviders(ctx, p, key)
			if err != nil {
				return nil, err
			}

			logger.Debugf("%d provider entries", len(provs))

			// Add unique providers from request, up to 'count'
			for _, prov := range provs {
				dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
				logger.Debugf("got provider: %s", prov)
				if psTryAdd(prov.ID) {
					logger.Debugf("using provider: %s", prov)
					select {
					case peerOut <- *prov:
					case <-ctx.Done():
						logger.Debug("context timed out sending more providers")
						return nil, ctx.Err()
					}
				}
				if !findAll && psSize() >= count {
					logger.Debugf("got enough providers (%d/%d)", psSize(), count)
					return nil, nil
				}
			}

			// Give closer peers back to the query to be queried
			logger.Debugf("got closer peers: %d %s", len(closest), closest)

			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: closest,
			})

			return closest, nil
		},
		func() bool {
			return !findAll && psSize() >= count
		},
	)

	if err == nil && ctx.Err() == nil {
		dht.refreshRTIfNoShortcut(kb.ConvertKey(string(key)), lookupRes)
	}
}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (_ peer.AddrInfo, err error) {
	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}

	logger.Debugw("finding peer", "peer", id)

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(id),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, id)
			if err != nil {
				logger.Debugf("error getting closer peers: %s", err)
				return nil, err
			}

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func() bool {
			return dht.host.Network().Connectedness(id) == network.Connected
		},
	)

	if err != nil {
		return peer.AddrInfo{}, err
	}

	dialedPeerDuringQuery := false
	for i, p := range lookupRes.peers {
		if p == id {
			// Note: we consider PeerUnreachable to be a valid state because the peer may not support the DHT protocol
			// and therefore the peer would fail the query. The fact that a peer that is returned can be a non-DHT
			// server peer and is not identified as such is a bug.
			dialedPeerDuringQuery = (lookupRes.state[i] == qpeerset.PeerQueried || lookupRes.state[i] == qpeerset.PeerUnreachable || lookupRes.state[i] == qpeerset.PeerWaiting)
			break
		}
	}

	// Return peer information if we tried to dial the peer during the query or we are (or recently were) connected
	// to the peer.
	connectedness := dht.host.Network().Connectedness(id)
	if dialedPeerDuringQuery || connectedness == network.Connected || connectedness == network.CanConnect {
		return dht.peerstore.PeerInfo(id), nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}
