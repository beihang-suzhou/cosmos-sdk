package baseapp

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"

	"errors"

	"github.com/gogo/protobuf/proto"

	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/crypto/tmhash"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/log"

	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/store"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/version"
)

// Key to store the consensus params in the main store.
var mainConsensusParamsKey = []byte("consensus_params")

// Enum mode for app.runTx
type runTxMode uint8

const (
	// Check a transaction
	runTxModeCheck runTxMode = iota
	// Simulate a transaction
	runTxModeSimulate runTxMode = iota
	// Deliver a transaction
	runTxModeDeliver runTxMode = iota

	// MainStoreKey is the string representation of the main store
	MainStoreKey = "main"
)

// BaseApp reflects the ABCI application implementation.
type BaseApp struct {
	// initialized on creation
	logger      log.Logger
	name        string                         // application name from abci.Info
	db          dbm.DB                         // common DB backend
	cms         map[int32]sdk.CommitMultiStore // Main (uncached) state
	router      Router                         // handle any kind of message
	queryRouter QueryRouter                    // router for redirecting query calls
	txDecoder   sdk.TxDecoder                  // unmarshal []byte into sdk.Tx

	// set upon LoadVersion or LoadLatestVersion.
	baseKey map[int32]*sdk.KVStoreKey // Main KVStore in cms

	anteHandler    sdk.AnteHandler  // ante handler for fee and auth
	initChainer    sdk.InitChainer  // initialize state with validators and state blob
	beginBlocker   sdk.BeginBlocker // logic to run before any txs
	endBlocker     sdk.EndBlocker   // logic to run after all txs, and to determine valset changes
	addrPeerFilter sdk.PeerFilter   // filter peers by address and port
	idPeerFilter   sdk.PeerFilter   // filter peers by node ID
	fauxMerkleMode bool             // if true, IAVL MountStores uses MountStoresDB for simulation speed.

	// --------------------
	// Volatile state
	// checkState is set on initialization and reset on Commit.
	// deliverState is set in InitChain and BeginBlock and cleared on Commit.
	// See methods setCheckState and setDeliverState.
	checkState   map[int32]*state // for CheckTx
	deliverState map[int32]*state // for DeliverTx
	voteInfos    []abci.VoteInfo  // absent validators from begin block

	// consensus params
	// TODO: Move this in the future to baseapp param store on main store.
	consensusParams *abci.ConsensusParams

	// The minimum gas prices a validator is willing to accept for processing a
	// transaction. This is mainly used for DoS and spam prevention.
	minGasPrices sdk.DecCoins

	// flag for sealing options and parameters to a BaseApp
	sealed bool
}

var _ abci.Application = (*BaseApp)(nil)

// NewBaseApp returns a reference to an initialized BaseApp. It accepts a
// variadic number of option functions, which act on the BaseApp to set
// configuration choices.
//
// NOTE: The db is used to store the version number for now.
func NewBaseApp(
	name string, logger log.Logger, db dbm.DB, group int32, txDecoder sdk.TxDecoder, options ...func(*BaseApp),
) *BaseApp {

	stores := make(map[int32]sdk.CommitMultiStore)

	var i int32
	for i = 0; i < 33; i++ {
		if i != 0 && (group>>uint32(i-1))&1 == 0 {
			continue
		}
		stores[i] = store.NewCommitMultiStore(db)
	}

	app := &BaseApp{
		logger:         logger,
		name:           name,
		db:             db,
		cms:            stores,
		router:         NewRouter(),
		queryRouter:    NewQueryRouter(),
		txDecoder:      txDecoder,
		fauxMerkleMode: false,
	}
	for _, option := range options {
		option(app)
	}

	return app
}

// Name returns the name of the BaseApp.
func (app *BaseApp) Name() string {
	return app.name
}

// Logger returns the logger of the BaseApp.
func (app *BaseApp) Logger() log.Logger {
	return app.logger
}

// SetCommitMultiStoreTracer sets the store tracer on the BaseApp's underlying
// CommitMultiStore.
func (app *BaseApp) SetCommitMultiStoreTracer(w io.Writer) {
	for _, item := range app.cms {
		item.SetTracer(w)
	}
}

// MountStores mounts all IAVL or DB stores to the provided keys in the BaseApp
// multistore.
func (app *BaseApp) MountStores(group int32, keys ...sdk.StoreKey) {
	for _, key := range keys {
		switch key.(type) {
		case *sdk.KVStoreKey:
			if !app.fauxMerkleMode {
				app.MountStore(group, key, sdk.StoreTypeIAVL)
			} else {
				// StoreTypeDB doesn't do anything upon commit, and it doesn't
				// retain history, but it's useful for faster simulation.
				app.MountStore(group, key, sdk.StoreTypeDB)
			}
		case *sdk.TransientStoreKey:
			app.MountStore(group, key, sdk.StoreTypeTransient)
		default:
			panic("Unrecognized store key type " + reflect.TypeOf(key).Name())
		}
	}
}

// MountStoreWithDB mounts a store to the provided key in the BaseApp
// multistore, using a specified DB.
func (app *BaseApp) MountStoreWithDB(group int32, key sdk.StoreKey, typ sdk.StoreType, db dbm.DB) {
	if _, ok := app.cms[group]; !ok {
		return
	}
	app.cms[group].MountStoreWithDB(key, typ, db)
}

// MountStore mounts a store to the provided key in the BaseApp multistore,
// using the default DB.
func (app *BaseApp) MountStore(group int32, key sdk.StoreKey, typ sdk.StoreType) {
	if _, ok := app.cms[group]; !ok {
		return
	}
	app.cms[group].MountStoreWithDB(key, typ, nil)
}

// LoadLatestVersion loads the latest application version. It will panic if
// called more than once on a running BaseApp.
func (app *BaseApp) LoadLatestVersion(group int32, baseKey *sdk.KVStoreKey) error {
	if _, ok := app.cms[group]; !ok {
		return errors.New("Invalid group.")
	}

	err := app.cms[group].LoadLatestVersion(group)
	if err != nil {
		return err
	}
	return app.initFromMainStore(group, baseKey)
}

// LoadVersion loads the BaseApp application version. It will panic if called
// more than once on a running baseapp.
func (app *BaseApp) LoadVersion(group int32, version int64, baseKey *sdk.KVStoreKey) error {
	if _, ok := app.cms[group]; !ok {
		return errors.New("Invalid group.")
	}

	err := app.cms[group].LoadVersion(group, version)
	if err != nil {
		return err
	}
	return app.initFromMainStore(group, baseKey)
}

// LastCommitID returns the last CommitID of the multistore.
func (app *BaseApp) LastCommitID() map[int32]sdk.CommitID {
	commits := make(map[int32]sdk.CommitID)
	for key, item := range app.cms {
		commits[key] = item.LastCommitID()
	}
	return commits
}

// LastBlockHeight returns the last committed block height.
func (app *BaseApp) LastBlockHeight(group int32) int64 {
	if _, ok := app.cms[group]; !ok {
		return -1
	}
	return app.cms[group].LastCommitID().Version
}

// initializes the remaining logic from app.cms
func (app *BaseApp) initFromMainStore(group int32, baseKey *sdk.KVStoreKey) error {
	mainStore := app.cms[group].GetKVStore(baseKey)
	if mainStore == nil {
		return errors.New("baseapp expects MultiStore with 'main' KVStore")
	}

	// memoize baseKey
	if app.baseKey == nil {
		app.baseKey = make(map[int32]*sdk.KVStoreKey)
	}
	if _, ok := app.baseKey[group]; ok {
		panic("app.baseKey expected to be nil; duplicate init?")
	}
	app.baseKey[group] = baseKey

	// Load the consensus params from the main store. If the consensus params are
	// nil, it will be saved later during InitChain.
	//
	// TODO: assert that InitChain hasn't yet been called.
	consensusParamsBz := mainStore.Get(mainConsensusParamsKey)
	if consensusParamsBz != nil {
		var consensusParams = &abci.ConsensusParams{}

		err := proto.Unmarshal(consensusParamsBz, consensusParams)
		if err != nil {
			panic(err)
		}

		app.setConsensusParams(consensusParams)
	}

	// needed for `gaiad export`, which inits from store but never calls initchain
	// can not use header.group.
	app.setCheckState(abci.Header{})
	app.Seal()

	return nil
}

func (app *BaseApp) setMinGasPrices(gasPrices sdk.DecCoins) {
	app.minGasPrices = gasPrices
}

// Router returns the router of the BaseApp.
func (app *BaseApp) Router() Router {
	if app.sealed {
		// We cannot return a router when the app is sealed because we can't have
		// any routes modified which would cause unexpected routing behavior.
		panic("Router() on sealed BaseApp")
	}
	return app.router
}

// QueryRouter returns the QueryRouter of a BaseApp.
func (app *BaseApp) QueryRouter() QueryRouter { return app.queryRouter }

// Seal seals a BaseApp. It prohibits any further modifications to a BaseApp.
func (app *BaseApp) Seal() { app.sealed = true }

// IsSealed returns true if the BaseApp is sealed and false otherwise.
func (app *BaseApp) IsSealed() bool { return app.sealed }

// setCheckState sets checkState with the cached multistore and
// the context wrapping it.
// It is called by InitChain() and Commit()
func (app *BaseApp) setCheckState(header abci.Header) {
	if app.checkState == nil {
		app.checkState = make(map[int32]*state)
	}

	for key, _ := range app.cms {
		header.Group = key
		app.checkState[key] = &state{
			ms:  app.cms[key].CacheMultiStore(),
			ctx: sdk.NewContext(app.cms[key], key, header, true, app.logger),
		}
	}
}

// setCheckState sets checkState with the cached multistore and
// the context wrapping it.
// It is called by InitChain() and BeginBlock(),
// and deliverState is set nil on Commit().
func (app *BaseApp) setDeliverState(header abci.Header) {
	if app.deliverState == nil {
		app.deliverState = make(map[int32]*state)
	}

	for key, _ := range app.cms {
		header.Group = key
		app.deliverState[key] = &state{
			ms:  app.cms[key].CacheMultiStore(),
			ctx: sdk.NewContext(app.cms[key], key, header, false, app.logger),
		}
	}
}

// setConsensusParams memoizes the consensus params.
func (app *BaseApp) setConsensusParams(consensusParams *abci.ConsensusParams) {
	app.consensusParams = consensusParams
}

// setConsensusParams stores the consensus params to the main store.
func (app *BaseApp) storeConsensusParams(consensusParams *abci.ConsensusParams) {
	consensusParamsBz, err := proto.Marshal(consensusParams)
	if err != nil {
		panic(err)
	}
	mainStore := app.cms[0].GetKVStore(app.baseKey[0])
	mainStore.Set(mainConsensusParamsKey, consensusParamsBz)
}

// getMaximumBlockGas gets the maximum gas from the consensus params.
func (app *BaseApp) getMaximumBlockGas() (maxGas uint64) {
	if app.consensusParams == nil || app.consensusParams.BlockSize == nil {
		return 0
	}
	return uint64(app.consensusParams.BlockSize.MaxGas)
}

// ----------------------------------------------------------------------------
// ABCI

// Info implements the ABCI interface. Just return the main group state
func (app *BaseApp) Info(req abci.RequestInfo) abci.ResponseInfo {
	fmt.Println("test inforr___")
	fmt.Println(fmt.Sprintf("%X", app.cms[0].LastCommitID().Hash))
	fmt.Println(fmt.Sprintf("%X", app.cms[2].LastCommitID().Hash))
	return abci.ResponseInfo{
		Data:             app.name,
		LastBlockHeight:  app.cms[0].LastCommitID().Version,
		LastBlockAppHash: app.cms[0].LastCommitID().Hash,
	}
}

// SetOption implements the ABCI interface.
func (app *BaseApp) SetOption(req abci.RequestSetOption) (res abci.ResponseSetOption) {
	// TODO: Implement!
	return
}

// InitChain implements the ABCI interface. It runs the initialization logic
// directly on the CommitMultiStore.
func (app *BaseApp) InitChain(req abci.RequestInitChain) (res abci.ResponseInitChain) {

	// stash the consensus params in the cms main store and memoize
	if req.ConsensusParams != nil {
		app.setConsensusParams(req.ConsensusParams)
		app.storeConsensusParams(req.ConsensusParams)
	}

	// initialize the deliver state and check state with ChainID and run initChain
	app.setDeliverState(abci.Header{ChainID: req.ChainId, Group: 0})
	app.setCheckState(abci.Header{ChainID: req.ChainId, Group: 0})
	for key, _ := range app.cms {
		app.deliverState[key].ctx = app.deliverState[key].ctx.
			WithBlockGasMeter(sdk.NewInfiniteGasMeter())
	}

	if app.initChainer == nil {
		return
	}

	// add block gas meter for any genesis transactions (allow infinite gas)

	ctxs := make(map[int32]sdk.Context)
	for key, item := range app.deliverState {
		ctxs[key] = item.ctx
	}
	res = app.initChainer(ctxs, req)

	// NOTE: We don't commit, but BeginBlock for block 1 starts from this
	// deliverState.
	return
}

// FilterPeerByAddrPort filters peers by address/port.
func (app *BaseApp) FilterPeerByAddrPort(info string) abci.ResponseQuery {
	if app.addrPeerFilter != nil {
		return app.addrPeerFilter(info)
	}
	return abci.ResponseQuery{}
}

// FilterPeerByIDfilters peers by node ID.
func (app *BaseApp) FilterPeerByID(info string) abci.ResponseQuery {
	if app.idPeerFilter != nil {
		return app.idPeerFilter(info)
	}
	return abci.ResponseQuery{}
}

// Splits a string path using the delimiter '/'.
// e.g. "this/is/funny" becomes []string{"this", "is", "funny"}
func splitPath(requestPath string) (path []string) {
	path = strings.Split(requestPath, "/")
	// first element is empty string
	if len(path) > 0 && path[0] == "" {
		path = path[1:]
	}
	return path
}

// Query implements the ABCI interface. It delegates to CommitMultiStore if it
// implements Queryable.
func (app *BaseApp) Query(req abci.RequestQuery) (res abci.ResponseQuery) {
	path := splitPath(req.Path)
	if len(path) == 0 {
		msg := "no query path provided"
		return sdk.ErrUnknownRequest(msg).QueryResult()
	}

	switch path[0] {
	// "/app" prefix for special application queries
	case "app":
		return handleQueryApp(app, path, req)

	case "store":
		return handleQueryStore(app, path, req)

	case "p2p":
		return handleQueryP2P(app, path, req)

	case "custom":
		return handleQueryCustom(app, path, req)
	}

	msg := "unknown query path"
	return sdk.ErrUnknownRequest(msg).QueryResult()
}

func handleQueryApp(app *BaseApp, path []string, req abci.RequestQuery) (res abci.ResponseQuery) {
	if len(path) >= 2 {
		var result sdk.Result

		switch path[1] {
		case "simulate":
			txBytes := req.Data
			tx, err := app.txDecoder(txBytes)
			if err != nil {
				result = err.Result()
			} else {
				result = app.Simulate(txBytes, tx)
			}

		case "version":
			return abci.ResponseQuery{
				Code:      uint32(sdk.CodeOK),
				Codespace: string(sdk.CodespaceRoot),
				Value:     []byte(version.Version),
			}

		default:
			result = sdk.ErrUnknownRequest(fmt.Sprintf("Unknown query: %s", path)).Result()
		}

		value := codec.Cdc.MustMarshalBinaryLengthPrefixed(result)
		return abci.ResponseQuery{
			Code:      uint32(sdk.CodeOK),
			Codespace: string(sdk.CodespaceRoot),
			Value:     value,
		}
	}

	msg := "Expected second parameter to be either simulate or version, neither was present"
	return sdk.ErrUnknownRequest(msg).QueryResult()
}

func handleQueryStore(app *BaseApp, path []string, req abci.RequestQuery) (res abci.ResponseQuery) {
	if _, ok := app.cms[req.Group]; !ok {
		return sdk.ErrUnknownRequest("Invalid group").QueryResult()
	}
	// "/store" prefix for store queries
	queryable, ok := app.cms[req.Group].(sdk.Queryable)
	if !ok {
		msg := "multistore doesn't support queries"
		return sdk.ErrUnknownRequest(msg).QueryResult()
	}

	req.Path = "/" + strings.Join(path[1:], "/")
	return queryable.Query(req)
}

func handleQueryP2P(app *BaseApp, path []string, _ abci.RequestQuery) (res abci.ResponseQuery) {
	// "/p2p" prefix for p2p queries
	if len(path) >= 4 {
		cmd, typ, arg := path[1], path[2], path[3]
		switch cmd {
		case "filter":
			switch typ {
			case "addr":
				return app.FilterPeerByAddrPort(arg)
			case "id":
				return app.FilterPeerByID(arg)
			}
		default:
			msg := "Expected second parameter to be filter"
			return sdk.ErrUnknownRequest(msg).QueryResult()
		}
	}

	msg := "Expected path is p2p filter <addr|id> <parameter>"
	return sdk.ErrUnknownRequest(msg).QueryResult()
}

func handleQueryCustom(app *BaseApp, path []string, req abci.RequestQuery) (res abci.ResponseQuery) {
	if _, ok := app.cms[req.Group]; !ok {
		return sdk.ErrUnknownRequest("Invalid group").QueryResult()
	}
	// path[0] should be "custom" because "/custom" prefix is required for keeper
	// queries.
	//
	// The queryRouter routes using path[1]. For example, in the path
	// "custom/gov/proposal", queryRouter routes using "gov".
	if len(path) < 2 || path[1] == "" {
		return sdk.ErrUnknownRequest("No route for custom query specified").QueryResult()
	}

	querier := app.queryRouter.Route(path[1])
	if querier == nil {
		return sdk.ErrUnknownRequest(fmt.Sprintf("no custom querier found for route %s", path[1])).QueryResult()
	}

	// cache wrap the commit-multistore for safety
	ctx := sdk.NewContext(app.cms[req.Group].CacheMultiStore(), req.Group, app.checkState[req.Group].ctx.BlockHeader(), true, app.logger).WithMinGasPrices(app.minGasPrices)

	// Passes the rest of the path as an argument to the querier.
	//
	// For example, in the path "custom/gov/proposal/test", the gov querier gets
	// []string{"proposal", "test"} as the path.
	resBytes, err := querier(ctx, path[2:], req)
	if err != nil {
		return abci.ResponseQuery{
			Code:      uint32(err.Code()),
			Codespace: string(err.Codespace()),
			Log:       err.ABCILog(),
		}
	}

	return abci.ResponseQuery{
		Code:  uint32(sdk.CodeOK),
		Value: resBytes,
	}
}

// BeginBlock implements the ABCI application interface.
func (app *BaseApp) BeginBlock(req abci.RequestBeginBlock) (res abci.ResponseBeginBlock) {
	group := req.Header.Group
	isGroup := true
	if _, ok := app.cms[group]; !ok {
		isGroup = false
	}

	if app.cms[0].TracingEnabled() {
		app.cms[0].SetTracingContext(sdk.TraceContext(
			map[string]interface{}{"blockHeight": req.Header.Height, "group": 0},
		))
	}

	if group != 0 && isGroup {
		if app.cms[group].TracingEnabled() {
			app.cms[group].SetTracingContext(sdk.TraceContext(
				map[string]interface{}{"blockHeight": req.Header.Height, "group": group},
			))
		}
	}

	// Initialize the DeliverTx state. If this is the first block, it should
	// already be initialized in InitChain. Otherwise app.deliverState will be
	// nil, since it is reset on Commit.
	if app.deliverState == nil {
		app.setDeliverState(req.Header)
	} else {
		// In the first block, app.deliverState.ctx will already be initialized
		// by InitChain. Context is now updated with Header information.
		for key := range app.cms {
			app.deliverState[key].ctx = app.deliverState[key].ctx.
				WithBlockHeader(req.Header).
				WithBlockHeight(req.Header.Height)
		}
	}

	// add block gas meter
	var gasMeter sdk.GasMeter
	if maxGas := app.getMaximumBlockGas(); maxGas > 0 {
		gasMeter = sdk.NewGasMeter(maxGas)
	} else {
		gasMeter = sdk.NewInfiniteGasMeter()
	}

	app.deliverState[0].ctx = app.deliverState[0].ctx.WithBlockGasMeter(gasMeter)

	if app.beginBlocker != nil {
		res = app.beginBlocker(app.deliverState[0].ctx, req)
		// if group != 0 && isGroup {
		// 	res2 := app.beginBlocker(app.deliverState[group].ctx, req)
		// 	res.Tags = append(res.Tags, res2.Tags...)
		// }
	}

	// set the signed validators for addition to context in deliverTx
	app.voteInfos = req.LastCommitInfo.GetVotes()
	return
}

// CheckTx implements the ABCI interface. It runs the "basic checks" to see
// whether or not a transaction can possibly be executed, first decoding, then
// the ante handler (which checks signatures/fees/ValidateBasic), then finally
// the route match to see whether a handler exists.
//
// NOTE:CheckTx does not run the actual Msg handler function(s).
func (app *BaseApp) CheckTx(txBytes []byte) (res abci.ResponseCheckTx) {
	var result sdk.Result

	tx, err := app.txDecoder(txBytes)
	if err != nil {
		result = err.Result()
	} else {
		result = app.runTx(runTxModeCheck, txBytes, tx)
	}

	return abci.ResponseCheckTx{
		Code:      uint32(result.Code),
		Data:      result.Data,
		Log:       result.Log,
		GasWanted: int64(result.GasWanted), // TODO: Should type accept unsigned ints?
		GasUsed:   int64(result.GasUsed),   // TODO: Should type accept unsigned ints?
		Tags:      result.Tags,
	}
}

// DeliverTx implements the ABCI interface.
func (app *BaseApp) DeliverTx(txBytes []byte) (res abci.ResponseDeliverTx) {
	var result sdk.Result

	tx, err := app.txDecoder(txBytes)
	if err != nil {
		result = err.Result()
	} else {
		result = app.runTx(runTxModeDeliver, txBytes, tx)
	}

	return abci.ResponseDeliverTx{
		Code:      uint32(result.Code),
		Codespace: string(result.Codespace),
		Data:      result.Data,
		Log:       result.Log,
		GasWanted: int64(result.GasWanted), // TODO: Should type accept unsigned ints?
		GasUsed:   int64(result.GasUsed),   // TODO: Should type accept unsigned ints?
		Tags:      result.Tags,
	}
}

// validateBasicTxMsgs executes basic validator calls for messages.
func validateBasicTxMsgs(msgs []sdk.Msg) sdk.Error {
	if msgs == nil || len(msgs) == 0 {
		return sdk.ErrUnknownRequest("Tx.GetMsgs() must return at least one message in list")
	}

	for _, msg := range msgs {
		// Validate the Msg.
		err := msg.ValidateBasic()
		if err != nil {
			return err
		}
	}

	return nil
}

// retrieve the context for the tx w/ txBytes and other memoized values.
func (app *BaseApp) getContextForTx(mode runTxMode, txBytes []byte, group int32) (ctx sdk.Context) {
	ctx = app.getState(mode, group).ctx.
		WithTxBytes(txBytes).
		WithVoteInfos(app.voteInfos).
		WithConsensusParams(app.consensusParams)

	if mode == runTxModeSimulate {
		ctx, _ = ctx.CacheContext()
	}

	return
}

type indexedABCILog struct {
	MsgIndex int    `json:"msg_index"`
	Success  bool   `json:"success"`
	Log      string `json:"log"`
}

// runMsgs iterates through all the messages and executes them.
func (app *BaseApp) runMsgs(ctx sdk.Context, msgs []sdk.Msg, mode runTxMode) (result sdk.Result) {
	idxlogs := make([]indexedABCILog, 0, len(msgs)) // a list of JSON-encoded logs with msg index

	var data []byte   // NOTE: we just append them all (?!)
	var tags sdk.Tags // also just append them all
	var code sdk.CodeType
	var codespace sdk.CodespaceType

	for msgIdx, msg := range msgs {
		// match message route
		msgRoute := msg.Route()
		handler := app.router.Route(msgRoute)
		if handler == nil {
			return sdk.ErrUnknownRequest("Unrecognized Msg type: " + msgRoute).Result()
		}

		var msgResult sdk.Result

		// skip actual execution for CheckTx mode
		if mode != runTxModeCheck {
			msgResult = handler(ctx, msg)
		}

		// NOTE: GasWanted is determined by ante handler and GasUsed by the GasMeter.

		// Result.Data must be length prefixed in order to separate each result
		data = append(data, msgResult.Data...)
		tags = append(tags, sdk.MakeTag(sdk.TagAction, msg.Type()))
		tags = append(tags, msgResult.Tags...)

		idxLog := indexedABCILog{MsgIndex: msgIdx, Log: msgResult.Log}

		// stop execution and return on first failed message
		if !msgResult.IsOK() {
			idxLog.Success = false
			idxlogs = append(idxlogs, idxLog)

			code = msgResult.Code
			codespace = msgResult.Codespace
			break
		}

		idxLog.Success = true
		idxlogs = append(idxlogs, idxLog)
	}

	logJSON := codec.Cdc.MustMarshalJSON(idxlogs)
	result = sdk.Result{
		Code:      code,
		Codespace: codespace,
		Data:      data,
		Log:       strings.TrimSpace(string(logJSON)),
		GasUsed:   ctx.GasMeter().GasConsumed(),
		Tags:      tags,
	}

	return result
}

// Returns the applicantion's deliverState if app is in runTxModeDeliver,
// otherwise it returns the application's checkstate.
func (app *BaseApp) getState(mode runTxMode, group int32) *state {
	if mode == runTxModeCheck || mode == runTxModeSimulate {
		return app.checkState[group]
	}

	return app.deliverState[group]
}

// cacheTxContext returns a new context based off of the provided context with
// a cache wrapped multi-store.
func (app *BaseApp) cacheTxContext(ctx sdk.Context, txBytes []byte) (
	sdk.Context, sdk.CacheMultiStore) {

	ms := ctx.MultiStore()
	// TODO: https://github.com/cosmos/cosmos-sdk/issues/2824
	msCache := ms.CacheMultiStore()
	if msCache.TracingEnabled() {
		msCache = msCache.SetTracingContext(
			sdk.TraceContext(
				map[string]interface{}{
					"txHash": fmt.Sprintf("%X", tmhash.Sum(txBytes)),
				},
			),
		).(sdk.CacheMultiStore)
	}

	return ctx.WithMultiStore(ctx.BlockHeader().Group, msCache), msCache
}

// runTx processes a transaction. The transactions is proccessed via an
// anteHandler. The provided txBytes may be nil in some cases, eg. in tests. For
// further details on transaction execution, reference the BaseApp SDK
// documentation.
func (app *BaseApp) runTx(mode runTxMode, txBytes []byte, tx sdk.Tx) (result sdk.Result) {
	// NOTE: GasWanted should be returned by the AnteHandler. GasUsed is
	// determined by the GasMeter. We need access to the context to get the gas
	// meter so we initialize upfront.
	var gasWanted uint64
	group := tx.GetMsgs()[0].Group()
	isGroup := true
	if _, ok := app.cms[group]; !ok {
		isGroup = false
	}

	var ctx0 sdk.Context
	var ms0 sdk.MultiStore

	ctx0 = app.getContextForTx(mode, txBytes, 0)
	ms0 = ctx0.MultiStore()

	// only run the tx if there is block gas remaining
	if mode == runTxModeDeliver && ctx0.BlockGasMeter().IsOutOfGas() {
		result = sdk.ErrOutOfGas("no block gas left to run tx").Result()
		return
	}

	var startingGas uint64
	if mode == runTxModeDeliver {
		startingGas = ctx0.BlockGasMeter().GasConsumed()
	}

	defer func() {
		if r := recover(); r != nil {
			switch rType := r.(type) {
			case sdk.ErrorOutOfGas:
				log := fmt.Sprintf(
					"out of gas in location: %v; gasWanted: %d, gasUsed: %d",
					rType.Descriptor, gasWanted, ctx0.GasMeter().GasConsumed(),
				)
				result = sdk.ErrOutOfGas(log).Result()
			default:
				log := fmt.Sprintf("recovered: %v\nstack:\n%v", r, string(debug.Stack()))
				result = sdk.ErrInternal(log).Result()
			}
		}

		result.GasWanted = gasWanted
		result.GasUsed = ctx0.GasMeter().GasConsumed()
	}()

	// If BlockGasMeter() panics it will be caught by the above recover and will
	// return an error - in any case BlockGasMeter will consume gas past the limit.
	//
	// NOTE: This must exist in a separate defer function for the above recovery
	// to recover from this one.
	defer func() {
		if mode == runTxModeDeliver {
			ctx0.BlockGasMeter().ConsumeGas(
				ctx0.GasMeter().GasConsumedToLimit(),
				"block gas meter",
			)

			if ctx0.BlockGasMeter().GasConsumed() < startingGas {
				panic(sdk.ErrorGasOverflow{Descriptor: "tx gas summation"})
			}
		}
	}()

	var msgs = tx.GetMsgs()
	if err := validateBasicTxMsgs(msgs); err != nil {
		return err.Result()
	}

	if app.anteHandler != nil {
		var anteCtx0 sdk.Context
		var msCache0 sdk.CacheMultiStore

		// Cache wrap context before anteHandler call in case it aborts.
		// This is required for both CheckTx and DeliverTx.
		// Ref: https://github.com/cosmos/cosmos-sdk/issues/2772
		//
		// NOTE: Alternatively, we could require that anteHandler ensures that
		// writes do not happen if aborted/failed.  This may have some
		// performance benefits, but it'll be more difficult to get right.
		anteCtx0, msCache0 = app.cacheTxContext(ctx0, txBytes)

		newCtx, result, abort := app.anteHandler(anteCtx0, tx, (mode == runTxModeSimulate))
		if !newCtx.IsZero() {
			// At this point, newCtx.MultiStore() is cache-wrapped, or something else
			// replaced by the ante handler. We want the original multistore, not one
			// which was cache-wrapped for the ante handler.
			//
			// Also, in the case of the tx aborting, we need to track gas consumed via
			// the instantiated gas meter in the ante handler, so we update the context
			// prior to returning.
			ctx0 = newCtx.WithMultiStore(0, ms0)
		}

		gasWanted = result.GasWanted

		if abort {
			return result
		}

		msCache0.Write()
	}

	if mode == runTxModeCheck {
		return
	}

	// Create a new context based off of the existing context with a cache wrapped
	// multi-store in case message processing fails.
	if isGroup {
		var ctx sdk.Context

		if group != 0 {
			ctx = app.getContextForTx(mode, txBytes, group)
		} else {
			ctx = ctx0
		}
		runMsgCtx, msCache := app.cacheTxContext(ctx, txBytes)
		result = app.runMsgs(runMsgCtx, msgs, mode)
		result.GasWanted = gasWanted

		if mode == runTxModeSimulate {
			return
		}

		// only update state if all messages pass
		if result.IsOK() {
			msCache.Write()
		}
	}

	return
}

// EndBlock implements the ABCI interface.
func (app *BaseApp) EndBlock(req abci.RequestEndBlock) (res abci.ResponseEndBlock) {
	if app.deliverState[0].ms.TracingEnabled() {
		app.deliverState[0].ms = app.deliverState[0].ms.SetTracingContext(nil).(sdk.CacheMultiStore)
	}
	isGroup := true
	if _, ok := app.cms[req.Group]; !ok {
		isGroup = false
	}
	if req.Group != 0 && isGroup && app.deliverState[req.Group].ms.TracingEnabled() {
		app.deliverState[req.Group].ms = app.deliverState[req.Group].ms.SetTracingContext(nil).(sdk.CacheMultiStore)
	}

	if app.endBlocker != nil {
		res = app.endBlocker(app.deliverState[0].ctx, req)
		// if req.Group != 0 && isGroup {
		// 	res2 := app.endBlocker(app.deliverState[req.Group].ctx, req)
		// 	res.Tags = append(res.Tags, res2.Tags...)
		// }
	}

	return
}

// Commit implements the ABCI interface.
func (app *BaseApp) Commit() (res abci.ResponseCommit) {
	var buffer bytes.Buffer
	var header abci.Header
	var hash []byte
	var keys []int
	for key := range app.deliverState {
		keys = append(keys, (int)(key))
	}
	sort.Ints(keys)
	for _, key := range keys {
		k := (int32)(key)
		header = app.deliverState[k].ctx.BlockHeader()

		// write the Deliver state and commit the MultiStore
		app.deliverState[k].ms.Write()

		commitID := app.cms[k].Commit(k)
		if key == 0 {
			hash = commitID.Hash
		}

		app.logger.Debug("Commit synced", "commit", fmt.Sprintf("%X,%d", commitID, key))
		buffer.Write(commitID.Hash)

	}
	// Reset the Check state to the latest committed.
	//
	// NOTE: safe because Tendermint holds a lock on the mempool for Commit.
	// Use the header from this latest block.
	app.setCheckState(header)

	// empty/reset the deliver state
	app.deliverState = nil

	return abci.ResponseCommit{
		Data: hash,
	}
}

// ----------------------------------------------------------------------------
// State

type state struct {
	ms  sdk.CacheMultiStore
	ctx sdk.Context
}

func (st *state) CacheMultiStore() sdk.CacheMultiStore {
	return st.ms.CacheMultiStore()
}

func (st *state) Context() sdk.Context {
	return st.ctx
}
