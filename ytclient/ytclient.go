package ytclient

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/wire"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
)

const rpcProtocolVersion int32 = 1

// discoverProxy picks a random RPC proxy using the same HTTP client
// and cluster URL that the official ytrpc.NewClient would use internally.
func discoverProxy(conf *yt.Config) (string, error) {
	if conf.RPCProxy != "" {
		return conf.RPCProxy, nil
	}

	clusterURL, err := conf.GetClusterURL()
	if err != nil {
		return "", fmt.Errorf("get cluster url: %w", err)
	}

	// Use the same HTTP client the official client builds (handles TLS, timeouts).
	httpClient, err := ytrpc.BuildHTTPClient(conf)
	if err != nil {
		return "", fmt.Errorf("build http client: %w", err)
	}

	url := fmt.Sprintf("%s://%s/api/v4/discover_proxies?type=rpc", clusterURL.Scheme, clusterURL.Address)
	rsp, err := httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("discover_proxies: %w", err)
	}
	defer rsp.Body.Close()

	var result struct {
		Proxies []string `json:"proxies"`
	}
	if err := json.NewDecoder(rsp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode proxies: %w", err)
	}
	if len(result.Proxies) == 0 {
		return "", fmt.Errorf("no RPC proxies for %s", clusterURL.Address)
	}
	return result.Proxies[rand.Intn(len(result.Proxies))], nil
}

type Client struct {
	conn        *bus.ClientConn
	token       string
	clusterHost string
}

func NewClient(ctx context.Context, clusterHost string) *Client {
	token := strings.TrimSpace(os.Getenv("YT_TOKEN"))
	if token == "" {
		home, _ := os.UserHomeDir()
		raw, _ := os.ReadFile(filepath.Join(home, ".yt", "token"))
		token = strings.TrimSpace(string(raw))
	}

	conf := &yt.Config{
		Proxy: clusterHost,
		Token: token,
	}
	proxy, err := discoverProxy(conf)
	if err != nil {
		panic(fmt.Errorf("discoverProxy: %w", err))
	}

	conn := bus.NewClient(ctx, proxy,
		bus.WithDefaultProtocolVersionMajor(rpcProtocolVersion),
	)
	return &Client{conn: conn, token: token, clusterHost: clusterHost}
}

// NewHTTPClient creates an official YT HTTP client using the same cluster and token.
// Use for operations like ReadTable that require HTTP streaming.
func (c *Client) NewHTTPClient() (yt.Client, error) {
	return ythttp.NewClient(&yt.Config{
		Proxy: c.clusterHost,
		Token: c.token,
	})
}

func (c *Client) Close() {
	c.conn.Close()
}

// GenerateTimestamp returns a fresh YT timestamp without starting a transaction.
// Use this to initialize the storage timestamp on startup.
func (c *Client) GenerateTimestamp(ctx context.Context) (uint64, error) {
	req := &rpc_proxy.TReqGenerateTimestamps{}
	var rsp rpc_proxy.TRspGenerateTimestamps
	if err := c.conn.Send(ctx, "ApiService", "GenerateTimestamps", req, &rsp,
		bus.WithToken(c.token)); err != nil {
		return 0, err
	}
	return rsp.GetTimestamp(), nil
}

// StartTabletTx starts a sticky tablet transaction pinned to one proxy.
func (c *Client) StartTabletTx(ctx context.Context) (guid.GUID, uint64, error) {
	txType := rpc_proxy.ETransactionType_TT_TABLET
	sticky := true
	req := &rpc_proxy.TReqStartTransaction{
		Type:   &txType,
		Sticky: &sticky,
	}
	var rsp rpc_proxy.TRspStartTransaction
	if err := c.conn.Send(ctx, "ApiService", "StartTransaction", req, &rsp,
		bus.WithToken(c.token)); err != nil {
		return guid.GUID{}, 0, err
	}
	return misc.NewGUIDFromProto(rsp.GetId()), rsp.GetStartTimestamp(), nil
}

// CommitTabletTx commits the transaction and returns PrimaryCommitTimestamp.
func (c *Client) CommitTabletTx(ctx context.Context, txID guid.GUID) (uint64, error) {
	req := &rpc_proxy.TReqCommitTransaction{
		TransactionId: misc.NewProtoFromGUID(txID),
	}
	var rsp rpc_proxy.TRspCommitTransaction
	if err := c.conn.Send(ctx, "ApiService", "CommitTransaction", req, &rsp,
		bus.WithToken(c.token)); err != nil {
		return 0, err
	}
	return rsp.GetPrimaryCommitTimestamp(), nil
}

// AbortTabletTx aborts the transaction.
func (c *Client) AbortTabletTx(ctx context.Context, txID guid.GUID) error {
	req := &rpc_proxy.TReqAbortTransaction{
		TransactionId: misc.NewProtoFromGUID(txID),
	}
	var rsp rpc_proxy.TRspAbortTransaction
	return c.conn.Send(ctx, "ApiService", "AbortTransaction", req, &rsp,
		bus.WithToken(c.token))
}

// InsertRows writes rows in the given transaction. rows — structs with yson tags.
func (c *Client) InsertRows(ctx context.Context, txID guid.GUID, path string, rows []any) error {
	nameTable, wireRows, err := wire.Encode(rows)
	if err != nil {
		return err
	}
	data, err := wire.MarshalRowset(wireRows)
	if err != nil {
		return err
	}

	modTypes := make([]rpc_proxy.ERowModificationType, len(rows))
	for i := range modTypes {
		modTypes[i] = rpc_proxy.ERowModificationType_RMT_WRITE
	}

	req := &rpc_proxy.TReqModifyRows{
		TransactionId:        misc.NewProtoFromGUID(txID),
		Path:                 []byte(path),
		RowModificationTypes: modTypes,
		RowsetDescriptor:     buildDescriptor(nameTable),
	}
	var rsp rpc_proxy.TRspModifyRows
	return c.conn.Send(ctx, "ApiService", "ModifyRows", req, &rsp,
		bus.WithToken(c.token),
		bus.WithAttachments(data))
}

func (c *Client) DeleteRows(ctx context.Context, txID guid.GUID, path string, rows []any) error {
	nameTable, wireRows, err := wire.Encode(rows)
	if err != nil {
		return err
	}
	data, err := wire.MarshalRowset(wireRows)
	if err != nil {
		return err
	}

	modTypes := make([]rpc_proxy.ERowModificationType, len(rows))
	for i := range modTypes {
		modTypes[i] = rpc_proxy.ERowModificationType_RMT_DELETE
	}

	req := &rpc_proxy.TReqModifyRows{
		TransactionId:        misc.NewProtoFromGUID(txID),
		Path:                 []byte(path),
		RowModificationTypes: modTypes,
		RowsetDescriptor:     buildDescriptor(nameTable),
	}
	var rsp rpc_proxy.TRspModifyRows
	return c.conn.Send(ctx, "ApiService", "ModifyRows", req, &rsp,
		bus.WithToken(c.token),
		bus.WithAttachments(data))
}

// LookupRows reads rows by keys at the given timestamp.
// Pass timestamp=0 to read the latest committed data.
// Returns raw wire rows and the response name table for decoding.
func (c *Client) LookupRows(ctx context.Context, path string, keys []any, timestamp uint64) ([]wire.Row, wire.NameTable, error) {
	nameTable, wireKeys, err := wire.Encode(keys)
	if err != nil {
		return nil, nil, err
	}
	data, err := wire.MarshalRowset(wireKeys)
	if err != nil {
		return nil, nil, err
	}

	req := &rpc_proxy.TReqLookupRows{
		Path:             []byte(path),
		RowsetDescriptor: buildDescriptor(nameTable),
	}
	if timestamp != 0 {
		req.Timestamp = &timestamp
	}

	var rsp rpc_proxy.TRspLookupRows
	var rspAttachments [][]byte
	if err := c.conn.Send(ctx, "ApiService", "LookupRows", req, &rsp,
		bus.WithToken(c.token),
		bus.WithAttachments(data),
		bus.WithResponseAttachments(&rspAttachments)); err != nil {
		return nil, nil, err
	}

	var merged []byte
	for _, a := range rspAttachments {
		merged = append(merged, a...)
	}
	rows, err := wire.UnmarshalRowset(merged)
	if err != nil {
		return nil, nil, err
	}

	var rspNameTable wire.NameTable
	for _, e := range rsp.GetRowsetDescriptor().GetNameTableEntries() {
		rspNameTable = append(rspNameTable, wire.NameTableEntry{Name: e.GetName()})
	}

	return rows, rspNameTable, nil
}

func buildDescriptor(nameTable wire.NameTable) *rpc_proxy.TRowsetDescriptor {
	kind := rpc_proxy.ERowsetKind_RK_UNVERSIONED
	entries := make([]*rpc_proxy.TRowsetDescriptor_TNameTableEntry, len(nameTable))
	for i, e := range nameTable {
		name := e.Name
		entries[i] = &rpc_proxy.TRowsetDescriptor_TNameTableEntry{Name: &name}
	}
	return &rpc_proxy.TRowsetDescriptor{
		WireFormatVersion: ptr.Int32(1),
		RowsetKind:        &kind,
		NameTableEntries:  entries,
	}
}
