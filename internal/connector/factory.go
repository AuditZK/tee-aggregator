package connector

import (
	"fmt"
	"strings"

	"github.com/trackrecord/enclave/internal/proxy"
)

// ErrUnsupportedExchange is returned when exchange is not supported
var ErrUnsupportedExchange = fmt.Errorf("unsupported exchange")

// Factory creates exchange connectors
type Factory struct {
	proxyCfg *proxy.Config
}

// NewFactory creates a new connector factory with no proxy.
func NewFactory() *Factory {
	return &Factory{}
}

// NewFactoryWithProxy creates a factory that routes geo-restricted exchanges
// through the given HTTP proxy (e.g. Binance from EU datacenters).
func NewFactoryWithProxy(cfg *proxy.Config) *Factory {
	return &Factory{proxyCfg: cfg}
}

// Create returns a connector for the given credentials
func (f *Factory) Create(creds *Credentials) (Connector, error) {
	exchange := strings.ToLower(strings.TrimSpace(creds.Exchange))

	switch exchange {
	// Major crypto exchanges — native connectors (5MB vs CCXT's 150MB per LoadMarkets)
	// Native connectors use direct HTTP with HMAC signing, no market loading.
	case "binance", "binance_futures", "binanceusdm":
		// Route through proxy if configured (Binance geo-blocks EU regions).
		// proxyCfg.ShouldProxy is nil-safe and returns false when no proxy is set.
		if f.proxyCfg.ShouldProxy("binance") {
			client := f.proxyCfg.NewClient("binance")
			return NewBinanceWithClient(creds, client), nil
		}
		return NewBinance(creds), nil
	case "bybit":
		return NewBybit(creds), nil
	case "okx":
		return NewOKX(creds), nil
	case "kraken":
		return NewKraken(creds), nil
	case "deribit":
		return NewDeribit(creds), nil
	case "mexc":
		return NewMEXC(creds), nil
	case "bitget":
		return NewBitget(creds), nil
	case "kucoin":
		return NewKuCoin(creds), nil
	case "coinbase":
		return NewCoinbase(creds), nil
	case "gate":
		return NewGate(creds), nil
	case "bingx":
		return NewBingX(creds), nil
	case "huobi":
		return NewHuobi(creds), nil

	// Traditional brokers
	case "ibkr":
		return NewIBKR(creds), nil
	case "alpaca":
		return NewAlpaca(creds), nil
	case "tradestation":
		return NewTradeStation(creds), nil

	// DEX (read-only, wallet address)
	case "hyperliquid":
		return NewHyperliquid(creds), nil
	case "lighter":
		return NewLighter(creds), nil

	// CFD/Forex brokers
	case "ctrader":
		return NewCTrader(creds), nil
	case "mt4", "mt5", "exness":
		return NewMetaTrader(creds), nil

	// Testing
	case "mock":
		return NewMock(), nil

	default:
		return nil, fmt.Errorf(
			"%w: %s. Supported: %s",
			ErrUnsupportedExchange,
			exchange,
			strings.Join(f.SupportedExchanges(), ", "),
		)
	}
}

// SupportedExchanges returns list of supported exchanges
func (f *Factory) SupportedExchanges() []string {
	return []string{
		"binance",
		"binance_futures",
		"binanceusdm",
		"bybit",
		"okx",
		"kraken",
		"deribit",
		"bitget",
		"mexc",
		"kucoin",
		"coinbase",
		"gate",
		"bingx",
		"huobi",
		"ibkr",
		"alpaca",
		"tradestation",
		"hyperliquid",
		"lighter",
		"ctrader",
		"mt4",
		"mt5",
		"exness",
		"mock",
	}
}
