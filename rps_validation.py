#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# RPS回测与区间寻优工具。
# 固定参数：BACKTEST_START_DATE / BACKTEST_END_DATE / INITIAL_CAPITAL / MAX_POSITION_CAPITAL
# 卖出规则：8%止损、6日不涨、盈利回踩MA20、最大持仓天数。



import argparse
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

BACKTEST_START_DATE = "2020-01-01"
BACKTEST_END_DATE = "2025-12-31"
INITIAL_CAPITAL = 2_000_000.0
MAX_POSITION_CAPITAL = 100_000.0
MIN_INTERVAL_SAMPLE = 30
PRINT_ALL_TRADES = True


class PriceRow(object):
    def __init__(self, date: str, stock: str, close: float):
        self.date = date
        self.stock = stock
        self.close = close


class EntrySignal(object):
    def __init__(self, stock: str, entry_date: str, entry_idx: int, entry_price: float, entry_ma20: Optional[float], rps: float, interval: str):
        self.stock = stock
        self.entry_date = entry_date
        self.entry_idx = entry_idx
        self.entry_price = entry_price
        self.entry_ma20 = entry_ma20
        self.rps = rps
        self.interval = interval


class ExitDecision(object):
    def __init__(self, exit_idx: int, exit_price: float, exit_reason: str, exit_ma20: Optional[float], peak_close: float, trough_close: float):
        self.exit_idx = exit_idx
        self.exit_price = exit_price
        self.exit_reason = exit_reason
        self.exit_ma20 = exit_ma20
        self.peak_close = peak_close
        self.trough_close = trough_close


class Trade(object):
    def __init__(self, entry_date: str, exit_date: str, stock: str, rps: float, interval: str, entry_price: float, exit_price: float, shares: int, invest_amount: float, ret: float, pnl: float, hold_days: int, exit_reason: str, entry_ma20: Optional[float], exit_ma20: Optional[float], peak_close: float, trough_close: float):
        self.entry_date = entry_date
        self.exit_date = exit_date
        self.stock = stock
        self.rps = rps
        self.interval = interval
        self.entry_price = entry_price
        self.exit_price = exit_price
        self.shares = shares
        self.invest_amount = invest_amount
        self.ret = ret
        self.pnl = pnl
        self.hold_days = hold_days
        self.exit_reason = exit_reason
        self.entry_ma20 = entry_ma20
        self.exit_ma20 = exit_ma20
        self.peak_close = peak_close
        self.trough_close = trough_close


def parse_date(value: str) -> datetime:
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"无法识别日期格式: {value}")


def load_prices(path: str, start_date: str, end_date: str) -> List[PriceRow]:
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    rows: List[PriceRow] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"date", "stock", "close"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"prices CSV缺少字段: {sorted(missing)}")
        for r in reader:
            d = r["date"].strip()
            dt = parse_date(d)
            if dt < start_dt or dt > end_dt:
                continue
            rows.append(PriceRow(date=d, stock=r["stock"].strip(), close=float(r["close"])))
    rows.sort(key=lambda x: (parse_date(x.date), x.stock))
    return rows


def load_filter(path: Optional[str]) -> Dict[Tuple[str, str], int]:
    if not path:
        return {}
    filt: Dict[Tuple[str, str], int] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"date", "stock", "pass_flag"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"filter CSV缺少字段: {sorted(missing)}")
        for r in reader:
            filt[(r["date"].strip(), r["stock"].strip())] = int(r["pass_flag"])
    return filt


def rank_to_rps(returns: List[Tuple[str, float]]) -> Dict[str, float]:
    if not returns:
        return {}
    returns_sorted = sorted(returns, key=lambda x: x[1])
    total = len(returns_sorted)
    return {stock: i / total * 100.0 for i, (stock, _) in enumerate(returns_sorted, start=1)}


def group_by_stock(rows: Iterable[PriceRow]) -> Dict[str, List[PriceRow]]:
    grouped: Dict[str, List[PriceRow]] = defaultdict(list)
    for row in rows:
        grouped[row.stock].append(row)
    for stock_rows in grouped.values():
        stock_rows.sort(key=lambda x: parse_date(x.date))
    return grouped


def parse_intervals(spec: str) -> List[Tuple[float, float]]:
    intervals: List[Tuple[float, float]] = []
    for part in spec.split(","):
        lo, hi = part.split("-")
        intervals.append((float(lo), float(hi)))
    return intervals


def in_interval(v: float, lo: float, hi: float) -> bool:
    return lo < v <= hi


def moving_average(values: List[float], end_idx: int, window: int) -> Optional[float]:
    if end_idx + 1 < window:
        return None
    seg = values[end_idx - window + 1 : end_idx + 1]
    return sum(seg) / window


def compute_daily_rps(stock_rows: Dict[str, List[PriceRow]], lookback: int) -> Dict[str, Dict[str, float]]:
    daily_returns: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for stock, rows in stock_rows.items():
        closes = [r.close for r in rows]
        for i in range(lookback, len(rows)):
            base = closes[i - lookback]
            if base <= 0:
                continue
            ret = closes[i] / base - 1.0
            daily_returns[rows[i].date].append((stock, ret))

    return {d: rank_to_rps(ret_list) for d, ret_list in daily_returns.items()}


def find_exit(
    rows: List[PriceRow],
    entry_idx: int,
    max_hold_days: int,
    stop_loss_pct: float,
    buffer_days: int,
) -> Optional[ExitDecision]:
    entry_price = rows[entry_idx].close
    closes = [r.close for r in rows]
    highest_close = entry_price
    trough_close = entry_price
    ever_profitable = False

    max_idx = min(len(rows) - 1, entry_idx + max_hold_days)
    for i in range(entry_idx + 1, max_idx + 1):
        px = rows[i].close
        highest_close = max(highest_close, px)
        trough_close = min(trough_close, px)
        if px > entry_price:
            ever_profitable = True

        ret = px / entry_price - 1.0
        ma20 = moving_average(closes, i, 20)

        if ret <= -abs(stop_loss_pct):
            return ExitDecision(i, px, "stop_loss_8pct", ma20, highest_close, trough_close)

        if i - entry_idx >= buffer_days and highest_close <= entry_price:
            return ExitDecision(i, px, f"no_rise_{buffer_days}d", ma20, highest_close, trough_close)

        if ever_profitable and ma20 is not None and px < ma20:
            return ExitDecision(i, px, "take_profit_ma20_pullback", ma20, highest_close, trough_close)

    if max_idx > entry_idx:
        px = rows[max_idx].close
        ma20 = moving_average(closes, max_idx, 20)
        return ExitDecision(max_idx, px, "max_hold_timeout", ma20, highest_close, trough_close)
    return None


def build_signals(
    stock_rows: Dict[str, List[PriceRow]],
    daily_rps: Dict[str, Dict[str, float]],
    intervals: List[Tuple[float, float]],
    filt: Dict[Tuple[str, str], int],
) -> List[EntrySignal]:
    signals: List[EntrySignal] = []
    for stock, rows in stock_rows.items():
        closes = [r.close for r in rows]
        for idx, row in enumerate(rows):
            entry_date = row.date
            rps = daily_rps.get(entry_date, {}).get(stock)
            if rps is None:
                continue
            if filt and filt.get((entry_date, stock), 1) != 1:
                continue

            interval_label = None
            for lo, hi in intervals:
                if in_interval(rps, lo, hi):
                    interval_label = f"({int(lo)},{int(hi)}]"
                    break
            if not interval_label:
                continue

            signals.append(
                EntrySignal(
                    stock=stock,
                    entry_date=entry_date,
                    entry_idx=idx,
                    entry_price=row.close,
                    entry_ma20=moving_average(closes, idx, 20),
                    rps=rps,
                    interval=interval_label,
                )
            )

    signals.sort(key=lambda s: (parse_date(s.entry_date), -s.rps, s.stock))
    return signals


def backtest(
    rows: List[PriceRow],
    lookback: int,
    max_hold_days: int,
    stop_loss_pct: float,
    buffer_days: int,
    intervals: List[Tuple[float, float]],
    filt: Dict[Tuple[str, str], int],
    initial_capital: float,
    max_position_capital: float,
) -> Tuple[List[Trade], List[Dict[str, float]], Dict[str, int]]:
    stock_rows = group_by_stock(rows)
    daily_rps = compute_daily_rps(stock_rows, lookback)
    signals = build_signals(stock_rows, daily_rps, intervals, filt)

    trades: List[Trade] = []
    rejected: Dict[str, int] = defaultdict(int)

    cash = initial_capital
    active_positions: List[Tuple[datetime, float]] = []
    active_stock_until: Dict[str, datetime] = {}

    for sig in signals:
        entry_dt = parse_date(sig.entry_date)

        still_active: List[Tuple[datetime, float]] = []
        for exit_dt, amount in active_positions:
            if exit_dt < entry_dt:
                cash += amount
            else:
                still_active.append((exit_dt, amount))
        active_positions = still_active

        if sig.stock in active_stock_until and active_stock_until[sig.stock] >= entry_dt:
            rejected["stock_already_open"] += 1
            continue

        srows = stock_rows[sig.stock]
        exit_decision = find_exit(
            rows=srows,
            entry_idx=sig.entry_idx,
            max_hold_days=max_hold_days,
            stop_loss_pct=stop_loss_pct,
            buffer_days=buffer_days,
        )
        if not exit_decision:
            rejected["no_exit_found"] += 1
            continue

        slot_cap = min(max_position_capital, cash)
        shares = int(slot_cap / sig.entry_price / 100) * 100
        if shares <= 0:
            rejected["insufficient_cash"] += 1
            continue

        invest_amount = shares * sig.entry_price
        cash -= invest_amount

        exit_date = srows[exit_decision.exit_idx].date
        exit_dt = parse_date(exit_date)
        active_positions.append((exit_dt, invest_amount + shares * (exit_decision.exit_price - sig.entry_price)))
        active_stock_until[sig.stock] = exit_dt

        pnl = shares * (exit_decision.exit_price - sig.entry_price)
        ret = exit_decision.exit_price / sig.entry_price - 1.0
        trades.append(
            Trade(
                entry_date=sig.entry_date,
                exit_date=exit_date,
                stock=sig.stock,
                rps=sig.rps,
                interval=sig.interval,
                entry_price=sig.entry_price,
                exit_price=exit_decision.exit_price,
                shares=shares,
                invest_amount=invest_amount,
                ret=ret,
                pnl=pnl,
                hold_days=exit_decision.exit_idx - sig.entry_idx,
                exit_reason=exit_decision.exit_reason,
                entry_ma20=sig.entry_ma20,
                exit_ma20=exit_decision.exit_ma20,
                peak_close=exit_decision.peak_close,
                trough_close=exit_decision.trough_close,
            )
        )

    summary = summarize_by_interval(trades, intervals)
    return trades, summary, dict(sorted(rejected.items(), key=lambda x: (-x[1], x[0])))


def summarize_by_interval(trades: List[Trade], intervals: List[Tuple[float, float]]) -> List[Dict[str, float]]:
    by_interval: Dict[str, List[Trade]] = defaultdict(list)
    for t in trades:
        by_interval[t.interval].append(t)

    summary: List[Dict[str, float]] = []
    for lo, hi in intervals:
        label = f"({int(lo)},{int(hi)}]"
        tlist = by_interval.get(label, [])
        if not tlist:
            summary.append(
                {
                    "interval": label,
                    "count": 0,
                    "mean_ret": 0.0,
                    "win_rate": 0.0,
                    "pnl_ratio": 0.0,
                    "avg_hold_days": 0.0,
                    "total_pnl": 0.0,
                }
            )
            continue

        rets = [t.ret for t in tlist]
        wins = [r for r in rets if r > 0]
        losses = [r for r in rets if r <= 0]
        win_rate = len(wins) / len(rets)
        mean_ret = sum(rets) / len(rets)
        pnl_ratio = ((sum(wins) / len(wins)) / abs(sum(losses) / len(losses))) if wins and losses else 0.0
        avg_hold_days = sum(t.hold_days for t in tlist) / len(tlist)
        total_pnl = sum(t.pnl for t in tlist)
        summary.append(
            {
                "interval": label,
                "count": len(tlist),
                "mean_ret": mean_ret,
                "win_rate": win_rate,
                "pnl_ratio": pnl_ratio,
                "avg_hold_days": avg_hold_days,
                "total_pnl": total_pnl,
            }
        )
    return summary


def summarize_exit_reasons(trades: List[Trade]) -> Dict[str, int]:
    out: Dict[str, int] = defaultdict(int)
    for t in trades:
        out[t.exit_reason] += 1
    return dict(sorted(out.items(), key=lambda x: (-x[1], x[0])))


def write_trades(path: Path, trades: List[Trade]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "entry_date",
                "exit_date",
                "stock",
                "rps",
                "interval",
                "entry_price",
                "entry_ma20",
                "exit_price",
                "exit_ma20",
                "peak_close",
                "trough_close",
                "shares",
                "invest_amount",
                "ret",
                "pnl",
                "hold_days",
                "exit_reason",
            ]
        )
        for t in trades:
            w.writerow(
                [
                    t.entry_date,
                    t.exit_date,
                    t.stock,
                    f"{t.rps:.4f}",
                    t.interval,
                    t.entry_price,
                    "" if t.entry_ma20 is None else t.entry_ma20,
                    t.exit_price,
                    "" if t.exit_ma20 is None else t.exit_ma20,
                    t.peak_close,
                    t.trough_close,
                    t.shares,
                    t.invest_amount,
                    t.ret,
                    t.pnl,
                    t.hold_days,
                    t.exit_reason,
                ]
            )


def write_summary(path: Path, summary: List[Dict[str, float]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["interval", "count", "mean_ret", "win_rate", "pnl_ratio", "avg_hold_days", "total_pnl"])
        for r in summary:
            w.writerow(
                [
                    r["interval"],
                    r["count"],
                    r["mean_ret"],
                    r["win_rate"],
                    r["pnl_ratio"],
                    r["avg_hold_days"],
                    r["total_pnl"],
                ]
            )


def print_preview(rows: List[PriceRow], lookback: int) -> None:
    daily_rps = compute_daily_rps(group_by_stock(rows), lookback)
    if not daily_rps:
        print("[RPS] 无法计算（样本长度不足）")
        return
    latest_date = sorted(daily_rps.keys(), key=parse_date)[-1]
    top10 = sorted(daily_rps[latest_date].items(), key=lambda x: x[1], reverse=True)[:10]
    print(f"[RPS] 最新日期: {latest_date} Top10")
    for i, (stock, rps) in enumerate(top10, start=1):
        print(f"  {i:2d}. {stock:<12} RPS={rps:6.2f}")


def print_best_interval(summary: List[Dict[str, float]]) -> None:
    valid = [x for x in summary if x["count"] > 0]
    if not valid:
        print("[结论] 无有效交易，无法评估RPS区间。")
        return
    best = sorted(valid, key=lambda x: (x["mean_ret"], x["win_rate"], x["count"]), reverse=True)[0]
    print("[结论] 建议优先关注RPS区间:")
    print(
        f"  {best['interval']} | n={best['count']} | mean={best['mean_ret']:.4%} | "
        f"win={best['win_rate']:.2%} | pnl={best['pnl_ratio']:.3f} | avg_hold={best['avg_hold_days']:.2f}d"
    )


def print_interval_decision(summary: List[Dict[str, float]], min_sample: int) -> None:
    qualified = [x for x in summary if x["count"] >= min_sample]
    if not qualified:
        print("[判定] 无区间满足最小样本数 n >= %d，建议延长回测区间。" % min_sample)
        return

    best_mean = sorted(qualified, key=lambda x: (x["mean_ret"], x["win_rate"]), reverse=True)[0]
    best_stable = sorted(qualified, key=lambda x: (x["win_rate"], x["pnl_ratio"], x["mean_ret"]), reverse=True)[0]

    print("[判定] 仅统计 n >= %d 的区间后：" % min_sample)
    print(
        "  收益优先: %s | n=%d | mean=%.4f%% | win=%.2f%% | pnl=%.3f"
        % (
            best_mean["interval"],
            int(best_mean["count"]),
            best_mean["mean_ret"] * 100,
            best_mean["win_rate"] * 100,
            best_mean["pnl_ratio"],
        )
    )
    print(
        "  稳定优先: %s | n=%d | mean=%.4f%% | win=%.2f%% | pnl=%.3f"
        % (
            best_stable["interval"],
            int(best_stable["count"]),
            best_stable["mean_ret"] * 100,
            best_stable["win_rate"] * 100,
            best_stable["pnl_ratio"],
        )
    )


def print_all_trades(trades: List[Trade]) -> None:
    if not trades:
        print("[交易明细] 无交易样本。")
        return

    ordered = sorted(trades, key=lambda t: (t.entry_date, t.stock))
    print("[交易明细] 全部交易(%d笔):" % len(ordered))
    for i, t in enumerate(ordered, start=1):
        print(
            "  %4d) %s %s RPS=%.1f 区间=%s 入=%.2f 出=%.2f 持有=%dd 收益=%.2f%% 盈亏=%.2f 原因=%s"
            % (
                i,
                t.entry_date,
                t.stock,
                t.rps,
                t.interval,
                t.entry_price,
                t.exit_price,
                t.hold_days,
                t.ret * 100,
                t.pnl,
                t.exit_reason,
            )
        )



def build_stock_pool_smtq(ContextInfo):
    # SMT-Q股票池：沪深300 + 中证500 + 中证1000 + 科创板 + 创业板。
    pools = []
    sector_names = ["沪深300", "中证500", "中证1000", "科创板", "创业板"]
    for name in sector_names:
        try:
            stocks = ContextInfo.get_stock_list_in_sector(name)
        except Exception:
            stocks = []
        pools.extend(stocks)

    # 回退：创业板常见代码前缀 300/301
    if not any((s.startswith("300") or s.startswith("301")) for s in pools):
        try:
            all_a = ContextInfo.get_stock_list_in_sector("沪深A股")
            pools.extend([s for s in all_a if s.startswith("300") or s.startswith("301")])
        except Exception:
            pass

    unique_pool = sorted(list(set(pools)))
    print("[股票池] 沪深300/中证500/中证1000/科创板/创业板 合并后: %d 只" % len(unique_pool))
    return unique_pool


def load_prices_smtq_batched(ContextInfo, stocks, start_date, end_date, batch_size=300, count=1500):
    # 分批拉取行情，避免一次请求过大导致卡顿或内存压力。
    rows = []
    total = len(stocks)
    if total == 0:
        return rows

    total_batches = (total + batch_size - 1) // batch_size
    print("[数据] 开始分批获取行情, batch_size=%d, 批次数=%d" % (batch_size, total_batches))

    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, total)
        batch = stocks[start_idx:end_idx]
        print("  [数据] 批次 %d/%d, 股票数=%d" % (i + 1, total_batches, len(batch)))

        try:
            data = ContextInfo.get_market_data_ex(
                ['close'],
                batch,
                period='1d',
                count=count,
                end_time=end_date,
            )
        except Exception as e:
            print("  [数据] 批次失败: %s" % str(e))
            continue

        for stock in batch:
            if stock not in data:
                continue
            df = data[stock]
            if df is None or len(df) == 0:
                continue
            for idx in df.index:
                d = str(idx)[:10].replace('-', '')
                if d < start_date or d > end_date:
                    continue
                close = float(df.loc[idx, 'close'])
                rows.append(PriceRow(date=d, stock=stock, close=close))

    print("[数据] 行情记录条数: %d" % len(rows))
    return rows


def run_smtq_backtest(ContextInfo):
    # SMT-Q入口：固定股票池 + 分批数据获取 + 回测输出。
    start_date = getattr(ContextInfo, 'BACKTEST_START_DATE', BACKTEST_START_DATE).replace('-', '')
    end_date = getattr(ContextInfo, 'BACKTEST_END_DATE', BACKTEST_END_DATE).replace('-', '')
    lookback = getattr(ContextInfo, 'LOOKBACK', 120)
    max_hold_days = getattr(ContextInfo, 'MAX_HOLD_DAYS', 60)
    buffer_days = getattr(ContextInfo, 'BUFFER_DAYS', 6)
    stop_loss_pct = getattr(ContextInfo, 'STOP_LOSS_PCT', 0.08)
    batch_size = getattr(ContextInfo, 'BATCH_SIZE', 300)
    min_sample = getattr(ContextInfo, 'MIN_INTERVAL_SAMPLE', MIN_INTERVAL_SAMPLE)
    print_all = getattr(ContextInfo, 'PRINT_ALL_TRADES', PRINT_ALL_TRADES)

    print("=" * 80)
    print("[RPS回测] SMT-Q执行开始")
    print("[配置] start=%s end=%s lookback=%d" % (start_date, end_date, lookback))

    stocks = build_stock_pool_smtq(ContextInfo)
    rows = load_prices_smtq_batched(ContextInfo, stocks, start_date, end_date, batch_size=batch_size)
    if len(rows) == 0:
        print("[RPS回测] 无可用行情数据，结束。")
        return

    intervals = parse_intervals("0-50,50-60,60-70,70-80,80-90,90-100")
    trades, summary, rejected = backtest(
        rows=rows,
        lookback=lookback,
        max_hold_days=max_hold_days,
        stop_loss_pct=stop_loss_pct,
        buffer_days=buffer_days,
        intervals=intervals,
        filt={},
        initial_capital=INITIAL_CAPITAL,
        max_position_capital=MAX_POSITION_CAPITAL,
    )

    print("[RPS回测] 交易条数: %d" % len(trades))
    for r in summary:
        print("  %8s | n=%6d | mean=%8.4f%% | win=%6.2f%% | pnl=%6.3f" % (
            r['interval'], int(r['count']), r['mean_ret'] * 100, r['win_rate'] * 100, r['pnl_ratio']
        ))

    reason_counts = summarize_exit_reasons(trades)
    if reason_counts:
        print("[RPS回测] 卖出原因分布:")
        for reason, cnt in reason_counts.items():
            print("  %-24s %d" % (reason, cnt))

    if rejected:
        print("[RPS回测] 未成交信号:")
        for k, v in rejected.items():
            print("  %-24s %d" % (k, v))

    if print_all:
        print_all_trades(trades)
    print_interval_decision(summary, min_sample)
    print_best_interval(summary)
    print("[RPS回测] 执行结束")
    print("=" * 80)


def init(ContextInfo):
    # SMT-Q初始化入口（启动即回测一次）。
    ContextInfo.BACKTEST_START_DATE = BACKTEST_START_DATE
    ContextInfo.BACKTEST_END_DATE = BACKTEST_END_DATE
    ContextInfo.LOOKBACK = 120
    ContextInfo.MAX_HOLD_DAYS = 60
    ContextInfo.BUFFER_DAYS = 6
    ContextInfo.STOP_LOSS_PCT = 0.08
    ContextInfo.BATCH_SIZE = 300
    ContextInfo.MIN_INTERVAL_SAMPLE = MIN_INTERVAL_SAMPLE
    ContextInfo.PRINT_ALL_TRADES = PRINT_ALL_TRADES

    run_smtq_backtest(ContextInfo)


def handlebar(ContextInfo):
    # SMT-Q bar回调，当前不重复执行。
    pass

def main() -> None:
    parser = argparse.ArgumentParser(description="RPS回测与区间寻优（含资金管理和交易原始数据记录）")
    parser.add_argument("--prices", required=True, help="行情CSV: date,stock,close")
    parser.add_argument("--filter", default=None, help="可选过滤CSV: date,stock,pass_flag")
    parser.add_argument("--lookback", type=int, default=120, help="RPS回看周期，默认120")
    parser.add_argument("--max-hold-days", type=int, default=60, help="最大持仓天数，默认60")
    parser.add_argument("--stop-loss-pct", type=float, default=0.08, help="止损比例，默认0.08(8%)")
    parser.add_argument("--buffer-days", type=int, default=6, help="买入缓冲区天数，默认6")
    parser.add_argument("--start-date", default=BACKTEST_START_DATE, help=f"回测起始日期，默认{BACKTEST_START_DATE}")
    parser.add_argument("--end-date", default=BACKTEST_END_DATE, help=f"回测截止日期，默认{BACKTEST_END_DATE}")
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL, help=f"初始资金，默认{int(INITIAL_CAPITAL)}")
    parser.add_argument(
        "--max-position-capital",
        type=float,
        default=MAX_POSITION_CAPITAL,
        help=f"单票最大投入，默认{int(MAX_POSITION_CAPITAL)}",
    )
    parser.add_argument(
        "--intervals",
        default="0-50,50-60,60-70,70-80,80-90,90-100",
        help="RPS区间，逗号分隔，例如 50-60,60-70,70-80",
    )
    parser.add_argument("--print-all-trades", type=int, default=1, help="是否打印全部交易明细:1是0否")
    parser.add_argument("--out-dir", default="results", help="输出目录")
    args = parser.parse_args()

    rows = load_prices(args.prices, args.start_date, args.end_date)
    if not rows:
        raise ValueError("起始日期过滤后无可用行情数据。")

    filt = load_filter(args.filter)
    intervals = parse_intervals(args.intervals)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[配置] start_date={args.start_date} end_date={args.end_date} initial_capital={args.initial_capital:.0f} max_position={args.max_position_capital:.0f}")
    print_preview(rows, args.lookback)
    trades, summary, rejected = backtest(
        rows=rows,
        lookback=args.lookback,
        max_hold_days=args.max_hold_days,
        stop_loss_pct=args.stop_loss_pct,
        buffer_days=args.buffer_days,
        intervals=intervals,
        filt=filt,
        initial_capital=args.initial_capital,
        max_position_capital=args.max_position_capital,
    )

    write_trades(out_dir / "rps_trades.csv", trades)
    write_summary(out_dir / "rps_interval_summary.csv", summary)

    print(f"[回测] 交易条数: {len(trades)}")
    print("[回测] 分区间统计:")
    for r in summary:
        print(
            f"  {r['interval']:>8} | n={int(r['count']):6d} | mean={r['mean_ret']:.4%} | "
            f"win={r['win_rate']:.2%} | pnl={r['pnl_ratio']:.3f} | hold={r['avg_hold_days']:.2f}d | total_pnl={r['total_pnl']:.2f}"
        )

    reason_counts = summarize_exit_reasons(trades)
    if reason_counts:
        print("[回测] 卖出原因分布:")
        for reason, cnt in reason_counts.items():
            print(f"  {reason:<24} {cnt}")

    if rejected:
        print("[回测] 未成交信号统计:")
        for k, v in rejected.items():
            print(f"  {k:<24} {v}")

    if args.print_all_trades == 1:
        print_all_trades(trades)
    print_interval_decision(summary, MIN_INTERVAL_SAMPLE)
    print_best_interval(summary)
    print(f"[输出] {out_dir / 'rps_trades.csv'}")
    print(f"[输出] {out_dir / 'rps_interval_summary.csv'}")


if __name__ == "__main__":
    main()
