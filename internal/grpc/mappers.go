package grpc

import (
	"fmt"
	"strings"
	"time"

	pb "github.com/trackrecord/enclave/api/proto"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"github.com/trackrecord/enclave/internal/signing"
)

func mapMarketBreakdown(in *repository.MarketBreakdown) *pb.MarketBreakdown {
	if in == nil {
		return nil
	}

	global := aggregateMarketMetrics(
		in.Stocks,
		in.Spot,
		in.Swap,
		in.Options,
		in.Futures,
		in.CFD,
		in.Forex,
		in.Commodities,
		in.Margin,
		in.Earn,
	)

	return &pb.MarketBreakdown{
		Global:      global,
		Spot:        mapMarketMetrics(in.Spot),
		Swap:        mapMarketMetrics(in.Swap),
		Options:     mapMarketMetrics(in.Options),
		Stocks:      mapMarketMetrics(in.Stocks),
		Futures:     mapMarketMetrics(in.Futures),
		Cfd:         mapMarketMetrics(in.CFD),
		Forex:       mapMarketMetrics(in.Forex),
		Commodities: mapMarketMetrics(in.Commodities),
	}
}

func connectionKey(exchange, label string) string {
	ex := strings.ToLower(strings.TrimSpace(exchange))
	lb := strings.ToLower(strings.TrimSpace(label))
	if lb == "" {
		return ex
	}
	return ex + "/" + lb
}

func isConnectionExcluded(excluded map[string]struct{}, exchange, label string) bool {
	if len(excluded) == 0 {
		return false
	}
	if _, ok := excluded[connectionKey(exchange, label)]; ok {
		return true
	}
	_, ok := excluded[strings.ToLower(strings.TrimSpace(exchange))]
	return ok
}

func mapMarketMetrics(in *repository.MarketMetrics) *pb.MarketMetrics {
	if in == nil {
		return nil
	}
	return &pb.MarketMetrics{
		Equity:          in.Equity,
		AvailableMargin: in.AvailableMargin,
		Volume:          in.Volume,
		Trades:          int32(in.Trades),
		TradingFees:     in.TradingFees,
		FundingFees:     in.FundingFees,
		LongTrades:      int32(in.LongTrades),
		ShortTrades:     int32(in.ShortTrades),
		LongVolume:      in.LongVolume,
		ShortVolume:     in.ShortVolume,
	}
}

func aggregateMarketMetrics(metrics ...*repository.MarketMetrics) *pb.MarketMetrics {
	out := &pb.MarketMetrics{}
	hasData := false
	for _, m := range metrics {
		if m == nil {
			continue
		}
		hasData = true
		out.Equity += m.Equity
		out.AvailableMargin += m.AvailableMargin
		out.Volume += m.Volume
		out.Trades += int32(m.Trades)
		out.TradingFees += m.TradingFees
		out.FundingFees += m.FundingFees
		out.LongTrades += int32(m.LongTrades)
		out.ShortTrades += int32(m.ShortTrades)
		out.LongVolume += m.LongVolume
		out.ShortVolume += m.ShortVolume
	}
	if !hasData {
		return nil
	}
	return out
}

func mapExchangeDetails(details []signing.ExchangeInfo, exchanges []string) []*pb.ExchangeInfo {
	if len(details) > 0 {
		out := make([]*pb.ExchangeInfo, 0, len(details))
		for _, ex := range details {
			out = append(out, &pb.ExchangeInfo{
				Name:     ex.Name,
				KycLevel: ex.KYCLevel,
				IsPaper:  ex.IsPaper,
			})
		}
		return out
	}

	if len(exchanges) == 0 {
		return nil
	}

	// Backward-compatible fallback for cached reports generated before exchange_details support.
	out := make([]*pb.ExchangeInfo, 0, len(exchanges))
	for _, ex := range exchanges {
		out = append(out, &pb.ExchangeInfo{
			Name:     ex,
			KycLevel: "",
			IsPaper:  false,
		})
	}
	return out
}

func aggregateSyncResultsForSyncJobResponse(results []*service.SyncResult) (bool, int32, int32, *pb.SyncJobResponse_Snapshot, string) {
	if len(results) == 0 {
		return false, 0, 0, nil, "no results"
	}

	var (
		success          bool
		synced           int32
		snapshots        int32
		latestSnapshot   *pb.SyncJobResponse_Snapshot
		latestTimestamp  time.Time
		aggregatedErrors []string
	)

	for _, r := range results {
		if r == nil {
			continue
		}

		if r.Success {
			success = true
			synced += int32(r.TradeCount)
			snapshots++
			if latestSnapshot == nil || r.SnapshotTimestamp.After(latestTimestamp) {
				latestTimestamp = r.SnapshotTimestamp
				latestSnapshot = &pb.SyncJobResponse_Snapshot{
					Balance:   r.SnapshotEquity,
					Equity:    r.SnapshotEquity,
					Timestamp: r.SnapshotTimestamp.UnixMilli(),
				}
			}
		}

		if r.Error != "" {
			if r.Label != "" {
				aggregatedErrors = append(aggregatedErrors, fmt.Sprintf("%s/%s: %s", r.Exchange, r.Label, r.Error))
			} else if r.Exchange != "" {
				aggregatedErrors = append(aggregatedErrors, fmt.Sprintf("%s: %s", r.Exchange, r.Error))
			} else {
				aggregatedErrors = append(aggregatedErrors, r.Error)
			}
		}
	}

	if !success && len(aggregatedErrors) == 0 {
		aggregatedErrors = append(aggregatedErrors, "sync failed for all connections")
	}

	return success, synced, snapshots, latestSnapshot, strings.Join(aggregatedErrors, " | ")
}
