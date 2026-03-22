#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
避风港策略 v1.1 (逆势抗跌回测版)
核心思路: 弱势环境下，优先寻找抗跌(A型)与止跌企稳(B型)标的，并执行完整回测交易流程。
"""

import datetime

# ============================================================
# 全局参数
# ============================================================
BACKTEST_START = "20200101"
BACKTEST_END = "20231231"

INITIAL_CAPITAL = 1000000
POSITION_SIZE = 100000
MAX_POSITIONS = 10
MIN_LOT = 100

# 市场环境
INDEX_CODE = "000300.SH"
INDEX_MA_PERIOD = 20
INDEX_MAX_DROP = -0.08  # M2: 指数5日跌幅不得小于该值(避免股灾)

# A/B策略参数
A_BIAS = 0.02           # A1: 相对强弱阈值
B_VOL_SHRINK = 0.5      # B1: 缩量阈值

# 风控过滤
HIGH_GAIN_LIMIT = 0.50  # P2: 近60日涨幅上限
HIGH_GAIN_DAYS = 60

# 卖出规则
STOP_LOSS = -0.10
NO_RISE_DAYS = 12
MAX_HOLD_DAYS = 60


# ============================================================
# 初始化
# ============================================================
def init(ContextInfo):
    print("=" * 80)
    print("避风港策略 v1.1 (逆势抗跌回测版)")
    print("=" * 80)

    ContextInfo.capital = INITIAL_CAPITAL
    ContextInfo.available = INITIAL_CAPITAL
    ContextInfo.positions = {}
    ContextInfo.trade_records = []

    init_stock_pool(ContextInfo)

    print("\n【参数】")
    print("  回测区间: %s ~ %s" % (BACKTEST_START, BACKTEST_END))
    print("  初始资金: %.0f万 | 单票: %.0f万 | 最大持仓: %d" % (INITIAL_CAPITAL / 10000, POSITION_SIZE / 10000, MAX_POSITIONS))
    print("  环境: %s < MA%d | 5日跌幅 > %.0f%%" % (INDEX_CODE, INDEX_MA_PERIOD, INDEX_MAX_DROP * 100))
    print("  A型: 5日相对强弱 > %.0f%% | 站上MA20 | 阳线" % (A_BIAS * 100))
    print("  B型: 缩量<%.1f倍 | 收盘>=前日低点" % B_VOL_SHRINK)
    print("  风控: 排除ST | 近%d日涨幅<=%.0f%%" % (HIGH_GAIN_DAYS, HIGH_GAIN_LIMIT * 100))
    print("  卖出: 止损%.0f%% | %d日不涨 | 最大持仓%d日" % (STOP_LOSS * 100, NO_RISE_DAYS, MAX_HOLD_DAYS))
    print("=" * 80)


def init_stock_pool(ContextInfo):
    try:
        hs300 = ContextInfo.get_stock_list_in_sector("沪深300")
        zz500 = ContextInfo.get_stock_list_in_sector("中证500")
        zz1000 = ContextInfo.get_stock_list_in_sector("中证1000")

        try:
            kcb = ContextInfo.get_stock_list_in_sector("科创板")
        except Exception:
            kcb = []

        ContextInfo.stock_pool = list(set((hs300 or []) + (zz500 or []) + (zz1000 or []) + (kcb or [])))
        print("\n【股票池】沪深300:%d 中证500:%d 中证1000:%d 科创板:%d 合计:%d" % (
            len(hs300 or []), len(zz500 or []), len(zz1000 or []), len(kcb or []), len(ContextInfo.stock_pool)
        ))
    except Exception as e:
        print("初始化股票池失败: %s" % str(e))
        ContextInfo.stock_pool = []


# ============================================================
# 数据构建
# ============================================================
def get_trade_dates(ContextInfo):
    dates = ContextInfo.get_trading_dates("SH", BACKTEST_START, BACKTEST_END, 4000)
    return [str(d) for d in dates]


def load_data_batched(ContextInfo, end_date):
    all_data = {}
    batch_size = 300

    # 个股数据
    total = len(ContextInfo.stock_pool)
    total_batches = (total + batch_size - 1) // batch_size

    print("\n[数据] 分批获取个股行情...")
    for b in range(total_batches):
        s = b * batch_size
        e = min(s + batch_size, total)
        batch = ContextInfo.stock_pool[s:e]
        print("  批次 %d/%d (%d只)" % (b + 1, total_batches, len(batch)))
        try:
            d = ContextInfo.get_market_data_ex(
                ['open', 'high', 'low', 'close', 'volume'],
                batch,
                period='1d',
                count=1800,
                end_time=end_date
            )
            all_data.update(d)
        except Exception as e1:
            print("  失败: %s" % str(e1))

    # 指数数据
    try:
        idx = ContextInfo.get_market_data_ex(
            ['close'],
            [INDEX_CODE],
            period='1d',
            count=1800,
            end_time=end_date
        )
        all_data.update(idx)
    except Exception as e2:
        print("获取指数失败: %s" % str(e2))

    print("[数据] 完成, 标的数: %d" % len(all_data))
    return all_data


def build_data_maps(all_data):
    maps = {
        'open': {},
        'high': {},
        'low': {},
        'close': {},
        'volume': {},
        'idx_close': {},
    }

    for stock, df in all_data.items():
        if df is None or len(df) == 0:
            continue

        c_map, o_map, h_map, l_map, v_map = {}, {}, {}, {}, {}
        for idx in df.index:
            d = str(idx)[:8] if len(str(idx)) > 8 else str(idx)

            if 'close' in df.columns:
                try:
                    c_map[d] = float(df.loc[idx, 'close'])
                except Exception:
                    pass
            if 'open' in df.columns:
                try:
                    o_map[d] = float(df.loc[idx, 'open'])
                except Exception:
                    pass
            if 'high' in df.columns:
                try:
                    h_map[d] = float(df.loc[idx, 'high'])
                except Exception:
                    pass
            if 'low' in df.columns:
                try:
                    l_map[d] = float(df.loc[idx, 'low'])
                except Exception:
                    pass
            if 'volume' in df.columns:
                try:
                    v_map[d] = float(df.loc[idx, 'volume'])
                except Exception:
                    pass

        if stock == INDEX_CODE:
            maps['idx_close'] = c_map
        else:
            maps['close'][stock] = c_map
            maps['open'][stock] = o_map
            maps['high'][stock] = h_map
            maps['low'][stock] = l_map
            maps['volume'][stock] = v_map

    return maps


def calc_ma_on_date(price_map, dates, curr_idx, period=20):
    if curr_idx is None or curr_idx < period - 1:
        return None
    total = 0.0
    for k in range(curr_idx - period + 1, curr_idx + 1):
        px = price_map.get(dates[k])
        if px is None:
            return None
        total += px
    return total / float(period)


def build_name_st(ContextInfo):
    name_map = {}
    st_set = set()
    for stock in ContextInfo.stock_pool:
        try:
            nm = ContextInfo.get_stock_name(stock)
        except Exception:
            nm = stock[:6]
        name_map[stock] = nm
        if 'ST' in nm.upper():
            st_set.add(stock)
    return name_map, st_set


# ============================================================
# 核心策略逻辑
# ============================================================
def check_market_env(date, maps, idx_dates, idx_pos_map):
    """
    M1: 沪深300 < MA20 (弱势环境)
    M2: 5日涨跌幅 > -8% (非股灾)
    """
    idx_close = maps['idx_close']

    curr_idx = idx_pos_map.get(date)
    today_idx = idx_close.get(date)
    idx_ma20 = calc_ma_on_date(idx_close, idx_dates, curr_idx, INDEX_MA_PERIOD)

    if idx_ma20 is None or today_idx is None:
        return False, "无数据"

    if today_idx >= idx_ma20:
        return False, "环境不弱"

    dates = sorted(idx_close.keys())
    if date not in dates:
        return False, "日期错误"

    curr_idx = dates.index(date)
    if curr_idx < 5:
        return False, "数据不足"

    prev_date = dates[curr_idx - 5]
    prev_idx_px = idx_close.get(prev_date, 0)
    if prev_idx_px <= 0:
        return False, "指数数据缺失"

    drop_pct = (today_idx / prev_idx_px - 1)
    if drop_pct < INDEX_MAX_DROP:
        return False, "崩盘规避"

    return True, "弱势震荡"


def get_stock_score(stock, date, maps, idx_close, idx_dates, curr_idx):
    """
    计算个股得分并判断类型 (A型 或 B型)
    返回: (score, type_name)
    """
    close = maps['close'].get(stock, {})
    vol = maps['volume'].get(stock, {})
    opn = maps['open'].get(stock, {})
    low = maps['low'].get(stock, {})

    if date not in close:
        return 0, "无数据"

    today_px = close.get(date, 0)
    today_vol = vol.get(date, 0)
    today_open = opn.get(date, 0)

    if curr_idx is None or curr_idx < 5:
        return 0, "数据缺失"

    prev_date = idx_dates[curr_idx - 1]
    yesterday_low = low.get(prev_date, 0)

    date_5d_ago = idx_dates[curr_idx - 5]
    prev_5d_px = close.get(date_5d_ago, 0)
    idx_5d_ago_px = idx_close.get(date_5d_ago, 0)

    # P2: 高位股过滤
    if curr_idx >= HIGH_GAIN_DAYS:
        date_60d_ago = idx_dates[curr_idx - HIGH_GAIN_DAYS]
        px_60d = close.get(date_60d_ago, 0)
        if px_60d > 0 and (today_px / px_60d - 1) > HIGH_GAIN_LIMIT:
            return 0, "高位股剔除"

    # === A型-抗跌强盾 ===
    if prev_5d_px > 0 and idx_5d_ago_px > 0:
        stock_5d_ret = today_px / prev_5d_px - 1
        idx_5d_ret = idx_close.get(date, 0) / idx_5d_ago_px - 1 if idx_5d_ago_px > 0 else 0
        relative_strength = stock_5d_ret - idx_5d_ret

        ma20 = calc_ma_on_date(close, idx_dates, curr_idx, 20)
        is_above_ma = (ma20 is not None and today_px > ma20)
        is_red = (today_px > today_open)

        if relative_strength > A_BIAS and is_above_ma and is_red:
            return 50 + relative_strength * 100, "A型抗跌"

    # === B型-否极泰来 ===
    if curr_idx >= 5:
        vols_5d = []
        for k in range(1, 6):
            d = idx_dates[curr_idx - k]
            vols_5d.append(vol.get(d, 0))
        avg_vol = sum(vols_5d) / 5.0 if len(vols_5d) == 5 else 0

        is_shrink = (avg_vol > 0 and today_vol < avg_vol * B_VOL_SHRINK)
        is_stop_fall = (yesterday_low > 0 and today_px >= yesterday_low)

        if is_shrink and is_stop_fall:
            return 30 + (avg_vol / max(today_vol, 1)) * 10, "B型企稳"

    return 0, "不满足"


# ============================================================
# 交易执行
# ============================================================
def execute_buy(ContextInfo, stock, date, price, score, reason):
    shares = int(POSITION_SIZE / price / MIN_LOT) * MIN_LOT
    if shares < MIN_LOT:
        return False

    cost = shares * price
    if cost > ContextInfo.available:
        return False

    ContextInfo.available -= cost
    ContextInfo.positions[stock] = {
        'buy_date': date,
        'buy_price': price,
        'shares': shares,
        'days_held': 0,
        'max_price': price,
        'score': score,
        'type': reason,
    }
    return True


def execute_sell(ContextInfo, stock, date, price, reason, name):
    pos = ContextInfo.positions[stock]
    pnl_pct = (price / pos['buy_price'] - 1) * 100
    ContextInfo.available += pos['shares'] * price
    del ContextInfo.positions[stock]

    rec = {
        'stock': stock,
        'name': name,
        'buy_date': pos['buy_date'],
        'buy_price': pos['buy_price'],
        'sell_date': date,
        'sell_price': price,
        'pnl_pct': pnl_pct,
        'days_held': pos['days_held'],
        'reason': reason,
        'type': pos['type'],
    }
    ContextInfo.trade_records.append(rec)


# ============================================================
# 回测主逻辑
# ============================================================
def run_backtest(ContextInfo):
    print("\n" + "=" * 80)
    print("开始回测")
    print("=" * 80)

    trade_dates = get_trade_dates(ContextInfo)
    if len(trade_dates) == 0:
        print("无交易日")
        return

    all_data = load_data_batched(ContextInfo, trade_dates[-1])

    print("[处理] 构建数据映射...")
    maps = build_data_maps(all_data)
    print("[处理] 准备索引...")
    idx_dates = sorted(maps['idx_close'].keys())
    idx_pos_map = {d: i for i, d in enumerate(idx_dates)}
    name_map, st_set = build_name_st(ContextInfo)
    print("[处理] 完成，开始逐日回测...")

    buy_count = 0
    sell_count = 0

    for i, date in enumerate(trade_dates):
        curr_idx = idx_pos_map.get(date)
        # 1. 卖出逻辑
        for stock in list(ContextInfo.positions.keys()):
            if stock not in maps['close'] or date not in maps['close'][stock]:
                continue
            px = maps['close'][stock][date]
            pos = ContextInfo.positions[stock]
            pos['days_held'] += 1
            if px > pos['max_price']:
                pos['max_price'] = px

            ret = px / pos['buy_price'] - 1
            reason = None

            if ret <= STOP_LOSS:
                reason = "止损"
            elif ret > 0.05:
                ma20 = calc_ma_on_date(maps['close'].get(stock, {}), idx_dates, curr_idx, 20)
                if ma20 and px < ma20:
                    reason = "获利止盈"
            elif pos['days_held'] >= NO_RISE_DAYS and pos['max_price'] <= pos['buy_price'] * 1.02:
                reason = "%d日不涨" % NO_RISE_DAYS
            elif pos['days_held'] >= MAX_HOLD_DAYS:
                reason = "超时"

            if reason:
                execute_sell(ContextInfo, stock, date, px, reason, name_map.get(stock, stock[:6]))
                sell_count += 1

        # 2. 买入逻辑(先看环境)
        if len(ContextInfo.positions) < MAX_POSITIONS:
            if curr_idx is None or curr_idx < 5:
                continue
            is_env_ok, env_msg = check_market_env(date, maps, idx_dates, idx_pos_map)

            if i % 20 == 0:
                print("  %s [进度%d/%d] 持仓:%d 环境:%s" % (date, i + 1, len(trade_dates), len(ContextInfo.positions), env_msg))

            if is_env_ok:
                candidates = []
                for stock in ContextInfo.stock_pool:
                    if stock in ContextInfo.positions:
                        continue
                    if stock in st_set:
                        continue

                    score, typ = get_stock_score(stock, date, maps, maps['idx_close'], idx_dates, curr_idx)
                    if score > 0 and date in maps['close'].get(stock, {}):
                        px = maps['close'][stock][date]
                        candidates.append((stock, score, typ, px))

                # 优先A型，再按分数
                candidates.sort(key=lambda x: (0 if "A型" in x[2] else 1, -x[1]))

                for stock, score, typ, px in candidates:
                    if len(ContextInfo.positions) >= MAX_POSITIONS:
                        break
                    if execute_buy(ContextInfo, stock, date, px, score, typ):
                        buy_count += 1

    print("\n回测完成: 买入%d次 卖出%d次" % (buy_count, sell_count))
    print_results(ContextInfo)


def print_results(ContextInfo):
    recs = ContextInfo.trade_records
    print("\n" + "=" * 80)
    print("【回测统计】")
    print("=" * 80)

    if not recs:
        print("无成交记录")
        return

    wins = [r for r in recs if r['pnl_pct'] > 0]
    print("总交易:%d 盈利:%d 胜率:%.1f%% 平均收益:%.2f%%" % (
        len(recs), len(wins), len(wins) * 100.0 / len(recs), sum(r['pnl_pct'] for r in recs) / len(recs)
    ))

    a_grp = [r for r in recs if 'A型' in r['type']]
    b_grp = [r for r in recs if 'B型' in r['type']]

    if a_grp:
        a_win = [r for r in a_grp if r['pnl_pct'] > 0]
        print("A型(抗跌): 交易%d 胜率%.1f%% 收益%.2f%%" % (
            len(a_grp), len(a_win) * 100.0 / len(a_grp), sum(r['pnl_pct'] for r in a_grp) / len(a_grp)
        ))

    if b_grp:
        b_win = [r for r in b_grp if r['pnl_pct'] > 0]
        print("B型(企稳): 交易%d 胜率%.1f%% 收益%.2f%%" % (
            len(b_grp), len(b_win) * 100.0 / len(b_grp), sum(r['pnl_pct'] for r in b_grp) / len(b_grp)
        ))

    print("\n【交易明细示例】")
    for r in recs[:5]:
        print("%s %s %s 买入:%.2f 盈亏:%.2f%%" % (
            r['stock'], r['type'], r['buy_date'], r['buy_price'], r['pnl_pct']
        ))


# ============================================================
# 入口
# ============================================================
def handlebar(ContextInfo):
    if not hasattr(ContextInfo, 'done'):
        ContextInfo.done = True
        run_backtest(ContextInfo)
