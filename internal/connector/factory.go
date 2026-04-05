package connector

import (
	"fmt"
	"strings"
)

// ErrUnsupportedExchange is returned when exchange is not supported
var ErrUnsupportedExchange = fmt.Errorf("unsupported exchange")

// Factory creates exchange connectors
type Factory struct{}

// NewFactory creates a new connector factory
func NewFactory() *Factory {
	return &Factory{}
}

// Create returns a connector for the given credentials
func (f *Factory) Create(creds *Credentials) (Connector, error) {
	exchange := strings.ToLower(strings.TrimSpace(creds.Exchange))

	switch exchange {
	// Major crypto exchanges — native connectors (5MB vs CCXT's 150MB per LoadMarkets)
	// Native connectors use direct HTTP with HMAC signing, no market loading.
	case "binance", "binance_futures", "binanceusdm":
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
	case "mt4", "mt5":
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
		"mock",
	}
}
