# -*- coding: gbk -*-
"""
综合评分策略 - 回测版本 v13（排序修正 + 动量观察队列版）
兴业证券SMT-Q平台

【v13.1 补丁：新增本地文件输出（解决SMT-Q日志截断问题）】
  改动清单：
  1. 新增 LOG_DIR 参数 + log_print() 函数（同时写控制台+文件）
  2. candidate_log / factor_log / trade_records / equity_curve 各写独立CSV
  3. 统计汇总写独立 backtest_summary.txt
  4. 输出顺序：先统计汇总 → 再写CSV，确保统计永远不被截断
  5. 所有 print() 替换为 log_print()

【v12→v13 原始改动不变，此处省略重复注释】
"""
import datetime
import math
import os    # v13.1

# ============================================================
# 全局参数
# ============================================================

BACKTEST_START  = "20200201"
BACKTEST_END    = "20251231"
INITIAL_CAPITAL = 1000000
POSITION_SIZE   = 100000
MAX_POSITIONS   = 10
MAX_INDUSTRY    = 3

COMMISSION_RATE = 0.00025
STAMP_TAX       = 0.001
SLIPPAGE        = 0.001

# ===== v13.1：本地文件输出路径（★改成你的实际目录★）=====
LOG_DIR = "C:/quant_log/v13"

# ===== 买入条件 =====
BUY_REVERSAL_SCORE = 80
BUY_FUND_SCORE     = 5
BUY_ENV_COEF       = 0.75

SORT_MOM5_BOUNCE_BONUS = 15
SORT_MOM5_DROP_PENALTY = 10

RPS_MIN = 4
RPS_MAX = 15

BEAR_DRAWDOWN_MIN = -0.15

# ===== 出场条件（反转池）=====
STOP_LOSS        = -0.08
ATR_STOP_MULT_BULL = 2.5
ATR_STOP_MULT_OSC  = 2.2
ATR_STOP_MULT_BEAR = 2.0
ATR_STOP_MULT      = 2.0
ATR_TRAIL_MULT   = 1.5
TRAIL_ACTIVATE   = 0.07
MIN_HOLD_DAYS    = 3
MA20_PROFIT_MIN  = 0.08

LOSS_EXIT_PNL    = -0.03
LOSS_EXIT_DAYS   = 20
FORCE_EXIT_DAYS  = 35

SIGNAL_EXIT_SCORE = 20
SIGNAL_EXIT_DAYS  = 5

# ===== 动量池参数 =====
MOM_RPS_MIN        = 65
MOM_POSITION_SIZE  = 50000
MOM_MAX_POSITIONS  = 3
MOM_TRAIL_ACTIVATE = 0.12
MOM_ATR_TRAIL_MULT = 2.0
MOM_ATR_STOP_MULT  = 2.5
MOM_FORCE_EXIT_DAYS= 50
MOM_BUY_SCORE      = 50

# ===== 动量观察队列参数（v13新增）=====
MOM_WATCH_MAX_SIZE  = 10
MOM_WATCH_MAX_DAYS  = 7
MOM_PULLBACK_UPPER  = 1.02
MOM_PULLBACK_LOWER  = 0.99
MOM_PULLBACK_VOL    = 0.90

# ===== 反转池仓位上限 =====
MAX_POSITIONS_BULL = 10
MAX_POSITIONS_BEAR = 5

# ===== 峰值回撤熔断 =====
DD_REDUCE_RATIO  = 0.08
DD_STOP_RATIO    = 0.12
DD_RESUME_RATIO  = 0.06

EQUITY_STOP_RATIO   = 0.75
EQUITY_RESUME_RATIO = 0.78

BULL_THRESHOLD = 3
BEAR_THRESHOLD = 4

IND_BIAS_MAX = 0.15

SW1_INDEX_MAP = {
    'SW1电子':     '399364.SZ', 'SW1有色金属': '399395.SZ',
    'SW1医药生物': '000933.SH', 'SW1计算机':   '399363.SZ',
    'SW1食品饮料': '399396.SZ', 'SW1国防军工': '399967.SZ',
    'SW1银行':     '399986.SZ', 'SW1非银金融': '000934.SH',
    'SW1房地产':   '399965.SZ', 'SW1电力设备': '399808.SZ',
    'SW1农林牧渔': '399812.SZ', 'SW1煤炭':     '399998.SZ',
    'SW1建筑材料': '399976.SZ', 'SW1通信':     '000935.SH',
    'SW1传媒':     '000935.SH', 'SW1家用电器': '000932.SH',
    'SW1商贸零售': '000932.SH', 'SW1公用事业': '000998.SH',
}

SW1_SECTORS = [
    'SW1农林牧渔', 'SW1基础化工', 'SW1钢铁', 'SW1有色金属', 'SW1电子',
    'SW1汽车', 'SW1家用电器', 'SW1食品饮料', 'SW1纺织服饰', 'SW1轻工制造',
    'SW1医药生物', 'SW1公用事业', 'SW1交通运输', 'SW1房地产', 'SW1商贸零售',
    'SW1社会服务', 'SW1银行', 'SW1非银金融', 'SW1综合', 'SW1建筑材料',
    'SW1建筑装饰', 'SW1电力设备', 'SW1机械设备', 'SW1国防军工', 'SW1计算机',
    'SW1传媒', 'SW1通信', 'SW1煤炭', 'SW1石油石化', 'SW1环保', 'SW1美容护理',
]


# ============================================================
# v13.1：文件日志工具（新增）
# ============================================================

_log_file = None

def init_log_dir():
    """创建日志目录，打开日志文件"""
    global _log_file
    try:
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        log_path = os.path.join(LOG_DIR, "backtest_log.txt")
        _log_file = open(log_path, 'w', encoding='utf-8')
        print("[v13.1] 日志输出已开启: %s" % LOG_DIR)
    except Exception as e:
        print("[v13.1 警告] 无法创建日志文件: %s，仅输出到控制台" % str(e))
        _log_file = None

def close_log():
    global _log_file
    if _log_file:
        try: _log_file.close()
        except: pass
        _log_file = None

def log_print(msg):
    """同时输出到控制台和日志文件"""
    print(msg)
    if _log_file:
        try:
            _log_file.write(msg + '\n')
            _log_file.flush()
        except: pass

def write_csv_file(filepath, header, rows):
    """写CSV到独立文件"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(header + '\n')
            for row in rows:
                f.write(row + '\n')
        log_print("  [文件] %s (%d行)" % (filepath, len(rows)))
    except Exception as e:
        log_print("  [写入失败] %s | %s" % (filepath, str(e)))

def write_text_file(filepath, lines):
    """写文本文件"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            for line in lines:
                f.write(line + '\n')
    except: pass


# ============================================================
# 初始化
# ============================================================

def init(ContextInfo):
    # v13.1：先初始化日志，后续所有输出走 log_print
    init_log_dir()

    log_print("=" * 80)
    log_print("综合评分策略 v13（双池并行版 + 文件输出 v13.1）")
    log_print("=" * 80)

    ContextInfo.capital   = INITIAL_CAPITAL
    ContextInfo.available = INITIAL_CAPITAL
    ContextInfo.positions = {}
    ContextInfo.trade_records  = []
    ContextInfo.equity_curve   = []
    ContextInfo.max_equity     = INITIAL_CAPITAL
    ContextInfo.max_drawdown   = 0
    ContextInfo.factor_log     = []
    ContextInfo.candidate_log  = []
    ContextInfo.mom_watch      = {}
    ContextInfo.mom_watch_stats = {
        'added': 0, 'triggered': 0, 'expired': 0, 'displaced': 0, 'bear_skip': 0,
    }
    ContextInfo.circuit_broken = False
    ContextInfo.dd_reduced     = False
    ContextInfo.dd_stopped     = False
    ContextInfo.dd_stop_day    = 0
    ContextInfo.stats = {
        'total_trades': 0, 'win_trades': 0, 'loss_trades': 0,
        'exit_reasons': {
            '动态止损':0,'移动止盈':0,'止盈':0,
            '亏损超时':0,'强制出场':0,'信号消失':0,'反转完成':0,'趋势破位':0
        },
        'regime_trades': {'牛市':0,'震荡':0,'熊市':0},
        'regime_wins':   {'牛市':0,'震荡':0,'熊市':0},
        'pool_rev':  {'n':0,'wins':0,'pnl':0},
        'pool_mom':  {'n':0,'wins':0,'pnl':0},
    }

    init_stock_pool(ContextInfo)
    init_sw1_mapping(ContextInfo)

    log_print("\n【回测参数 v13】")
    log_print("  策略类型: 超跌反转（双池并行：反转池+动量观察队列）")
    log_print("  回测区间: %s ~ %s" % (BACKTEST_START, BACKTEST_END))
    log_print("  初始资金: %.0f万 | 反转池每只:%.0f万 | 动量池每只:%.0f万×%d只" %
          (INITIAL_CAPITAL/10000, POSITION_SIZE/10000,
           MOM_POSITION_SIZE/10000, MOM_MAX_POSITIONS))
    log_print("  反转池：RPS %.0f~%.0f | 牛市/震荡%d只 | 熊市%d只 | 评分阈值%.0f(v13↑)" %
          (RPS_MIN, RPS_MAX, MAX_POSITIONS_BULL, MAX_POSITIONS_BEAR, BUY_REVERSAL_SCORE))
    log_print("  排序公式: total_score + hard_cnt×5 + mom5方向加权(v13新增)")
    log_print("  动量池：RPS>=%.0f | 持仓≤%d只 | 观察队列≤%d只 | 等回踩MA20入场(v13新增)" %
          (MOM_RPS_MIN, MOM_MAX_POSITIONS, MOM_WATCH_MAX_SIZE))
    log_print("  回踩条件: 价格在MA20±%.0f%% 且 量能缩至vol_accel<%.1f 且 ≤%d天" %
          ((MOM_PULLBACK_UPPER-1)*100, MOM_PULLBACK_VOL, MOM_WATCH_MAX_DAYS))
    log_print("  日志目录: %s" % LOG_DIR)
    log_print("  峰值回撤熔断: >8%%降仓 | >12%%停买 | <6%%恢复（40天强制重置）")
    log_print("=" * 80)


def init_stock_pool(ContextInfo):
    try:
        hs300  = ContextInfo.get_stock_list_in_sector("沪深300")
        zz500  = ContextInfo.get_stock_list_in_sector("中证500")
        zz1000 = ContextInfo.get_stock_list_in_sector("中证1000")
        try:    kcb = ContextInfo.get_stock_list_in_sector("科创板")
        except: kcb = []
        if not kcb:
            try:
                all_sh = ContextInfo.get_stock_list_in_sector("沪深A股")
                kcb = [s for s in all_sh if s.startswith("688")]
            except: kcb = []
        try:    cyb = ContextInfo.get_stock_list_in_sector("创业板")
        except: cyb = []
        if not cyb:
            try:
                all_sz = ContextInfo.get_stock_list_in_sector("沪深A股")
                cyb = [s for s in all_sz if s.startswith("300")]
            except: cyb = []

        ContextInfo.hs300_set  = set(hs300)
        ContextInfo.zz500_set  = set(zz500)
        ContextInfo.zz1000_set = set(zz1000)
        ContextInfo.kcb_set    = set(kcb)
        ContextInfo.cyb_set    = set(cyb)
        all_stocks = list(set(hs300+zz500+zz1000+kcb+cyb))
        ContextInfo.stock_pool = all_stocks
        log_print("【股票池】沪深300:%d 中证500:%d 中证1000:%d 科创:%d 创业:%d 合计:%d" %
              (len(hs300),len(zz500),len(zz1000),len(kcb),len(cyb),len(all_stocks)))
    except Exception as e:
        log_print("获取股票池失败: %s" % str(e))
        ContextInfo.stock_pool = []


def init_sw1_mapping(ContextInfo):
    log_print("【构建SW1行业映射】")
    ContextInfo.stock_sw1 = {}
    for sector_name in SW1_SECTORS:
        try:
            members = ContextInfo.get_stock_list_in_sector(sector_name)
            if members:
                for st in members:
                    ContextInfo.stock_sw1[st] = sector_name
        except: pass
    covered = sum(1 for s in ContextInfo.stock_pool if s in ContextInfo.stock_sw1)
    total   = len(ContextInfo.stock_pool)
    log_print("  覆盖: %d / %d (%.1f%%)" % (covered, total, covered/total*100 if total else 0))


# ============================================================
# 市场阶段
# ============================================================

def get_market_regime(date, env_data):
    index_list = ['000300.SH','000905.SH','000852.SH','000688.SH','399006.SZ']
    normal = 0; bear = 0; total = 0
    for idx in index_list:
        if idx in env_data and date in env_data[idx]:
            v = env_data[idx][date][0]; total += 1
            if v >= 1.0: normal += 1
            elif v <= 0.5: bear += 1
    if total == 0: return '震荡', 0, 0
    if normal >= BULL_THRESHOLD: return '牛市', normal, bear
    if bear   >= BEAR_THRESHOLD: return '熊市', normal, bear
    return '震荡', normal, bear


# ============================================================
# 基础数学工具（不动）
# ============================================================

def rolling_mean(arr, period):
    n = len(arr); result = [None]*n; s = 0.0
    for i in range(n):
        v = arr[i] if arr[i] is not None else 0.0
        s += v
        if i >= period:
            pv = arr[i-period] if arr[i-period] is not None else 0.0
            s -= pv
        if i >= period-1: result[i] = s/period
    return result

def ema_series(arr, period):
    n = len(arr); result = [None]*n; mult = 2.0/(period+1)
    if n < period: return result
    sma = sum(arr[i] if arr[i] is not None else 0.0 for i in range(period))/float(period)
    result[period-1] = sma; ema_val = sma
    for i in range(period, n):
        v = arr[i] if arr[i] is not None else 0.0
        ema_val = (v-ema_val)*mult+ema_val; result[i] = ema_val
    return result

def rsi_series(closes, period=14):
    n = len(closes); result = [None]*n
    if n < period+1: return result
    gains=[0.0]*n; losses=[0.0]*n
    for i in range(1,n):
        d = closes[i]-closes[i-1]
        if d>0: gains[i]=d
        else:   losses[i]=-d
    ag = sum(gains[1:period+1])/float(period)
    al = sum(losses[1:period+1])/float(period)
    result[period] = 100.0 if al==0 else 100.0-100.0/(1+ag/al)
    for i in range(period+1,n):
        ag = (ag*(period-1)+gains[i])/float(period)
        al = (al*(period-1)+losses[i])/float(period)
        result[i] = 100.0 if al==0 else 100.0-100.0/(1+ag/al)
    return result

def wilder_smooth(arr, period):
    n=len(arr); result=[0.0]*n
    if n<=period: return result
    result[period]=sum(arr[1:period+1])
    for i in range(period+1,n):
        result[i]=result[i-1]-result[i-1]/period+arr[i]
    return result


# ============================================================
# 预计算技术指标（print→log_print）
# ============================================================

def precompute_all_indicators(all_market_data, trade_dates):
    log_print("\n预计算技术指标...")
    indicators = {}
    total = len(all_market_data); count = 0

    for stock, df in all_market_data.items():
        count += 1
        if count % 500 == 0:
            log_print("  已计算 %d / %d ..." % (count, total))

        dates   = [str(d)[:8] if len(str(d))>8 else str(d) for d in df.index]
        closes  = list(df['close'].values)
        highs   = list(df['high'].values)
        lows    = list(df['low'].values)
        volumes = list(df['volume'].values)
        opens   = list(df['open'].values) if 'open' in df.columns else closes[:]
        n = len(closes)
        if n < 60: continue

        ma5  = rolling_mean(closes,5);  ma10 = rolling_mean(closes,10)
        ma20 = rolling_mean(closes,20); ma60 = rolling_mean(closes,60)

        e12=ema_series(closes,12); e26=ema_series(closes,26)
        dif_arr=[0.0]*n
        for j in range(n):
            if e12[j] and e26[j]: dif_arr[j]=e12[j]-e26[j]
        dea_arr  = ema_series(dif_arr,9)
        macd_arr = [0.0]*n
        for j in range(n):
            if dea_arr[j] is not None: macd_arr[j]=2*(dif_arr[j]-dea_arr[j])

        rsi_arr = rsi_series(closes,14)

        boll_u=[None]*n; boll_m=[None]*n; boll_l=[None]*n
        for j in range(19,n):
            seg=closes[j-19:j+1]; ma=sum(seg)/20.0
            std=(sum((p-ma)**2 for p in seg)/20.0)**0.5
            boll_u[j]=ma+2*std; boll_m[j]=ma; boll_l[j]=ma-2*std

        vol5=rolling_mean(volumes,5); vol20=rolling_mean(volumes,20)
        vol_accel=[1.0]*n
        for j in range(n):
            if vol5[j] and vol20[j] and vol20[j]>0:
                vol_accel[j]=vol5[j]/vol20[j]

        pdm=[0.0]*n; mdm=[0.0]*n; tr_r=[0.0]*n
        for j in range(1,n):
            up=highs[j]-highs[j-1]; dn=lows[j-1]-lows[j]
            pdm[j]=up if (up>dn and up>0) else 0.0
            mdm[j]=dn if (dn>up and dn>0) else 0.0
            tr_r[j]=max(highs[j]-lows[j],abs(highs[j]-closes[j-1]),abs(lows[j]-closes[j-1]))
        p=14
        atr14_w=wilder_smooth(tr_r,p)
        pdm14=wilder_smooth(pdm,p); mdm14=wilder_smooth(mdm,p)
        pdi_arr=[0.0]*n; mdi_arr=[0.0]*n; adx_arr=[0.0]*n; dx_arr=[0.0]*n
        for j in range(p,n):
            at=atr14_w[j]
            pdi=100*pdm14[j]/at if at>0 else 0
            mdi=100*mdm14[j]/at if at>0 else 0
            pdi_arr[j]=pdi; mdi_arr[j]=mdi
            sm=pdi+mdi; dx_arr[j]=100*abs(pdi-mdi)/sm if sm>0 else 0
        dx_s=wilder_smooth(dx_arr,p)
        for j in range(p*2,n): adx_arr[j]=dx_s[j]/p

        mom5=[0.0]*n; mom10=[0.0]*n; mom20=[0.0]*n; mom60=[0.0]*n
        for j in range(n):
            if j>=5  and closes[j-5]>0:  mom5[j]  =closes[j]/closes[j-5]  -1
            if j>=10 and closes[j-10]>0: mom10[j] =closes[j]/closes[j-10] -1
            if j>=20 and closes[j-20]>0: mom20[j] =closes[j]/closes[j-20] -1
            if j>=60 and closes[j-60]>0: mom60[j] =closes[j]/closes[j-60] -1

        atr14_s=[0.0]*n
        for j in range(1,n):
            atr14_s[j]=max(highs[j]-lows[j],abs(highs[j]-closes[j-1]),abs(lows[j]-closes[j-1]))
        atr14=rolling_mean(atr14_s,14)

        ret=[0.0]*n
        for j in range(1,n):
            ret[j]=(closes[j]/closes[j-1]-1) if closes[j-1]>0 else 0
        hv20=[0.0]*n; hv60=[0.0]*n
        for j in range(20,n):
            seg=ret[j-19:j+1]; mu=sum(seg)/20.0
            hv20[j]=(sum((r-mu)**2 for r in seg)/20.0)**0.5*(252**0.5)
        for j in range(60,n):
            seg=ret[j-59:j+1]; mu=sum(seg)/60.0
            hv60[j]=(sum((r-mu)**2 for r in seg)/60.0)**0.5*(252**0.5)

        mfv=[0.0]*n
        for j in range(n):
            hl=highs[j]-lows[j]
            mfm=(2*closes[j]-highs[j]-lows[j])/hl if hl>0 else 0.0
            mfv[j]=mfm*volumes[j]
        mfv_sum=rolling_mean(mfv,20); vol_sum=rolling_mean(volumes,20)
        cmf_arr=[0.0]*n
        for j in range(19,n):
            if vol_sum[j] and vol_sum[j]>0:
                cmf_arr[j]=mfv_sum[j]/vol_sum[j]

        hammer=[0]*n; engulf=[0]*n; doji=[0]*n
        for j in range(1,n):
            o,h,l,c=opens[j],highs[j],lows[j],closes[j]
            body=abs(c-o); tr_=h-l
            if tr_<1e-6: continue
            us=h-max(o,c); ls=min(o,c)-l
            if ls>=2*body and us<0.5*body and body<0.3*tr_ and c>o: hammer[j]=1
            if body<0.05*tr_: doji[j]=1
            o1,c1=opens[j-1],closes[j-1]
            if c1<o1 and c>o and o<c1 and c>o1: engulf[j]=1

        stock_ind={}
        for j in range(n):
            d=dates[j]
            s20=max(0,j-19); hd20=max(highs[s20:j+1])
            drawdown=(closes[j]/hd20-1) if hd20>0 else 0
            chg=(closes[j]/closes[j-1]-1) if j>0 and closes[j-1]>0 else 0

            bu=boll_u[j]; bl=boll_l[j]; bm=boll_m[j]
            boll_pos=(closes[j]-bl)/max(bu-bl,1e-6) if bu and bl else 0.5

            atr_v=atr14[j] if atr14[j] else closes[j]*0.02
            atr_pct=atr_v/closes[j] if closes[j]>0 else 0.02

            mom_all_neg = (mom5[j]<0 and mom10[j]<0 and mom20[j]<0)
            mom_neg_cnt = sum(1 for m in [mom5[j],mom10[j],mom20[j],mom60[j]] if m<0)

            stock_ind[d]={
                'close':    float(closes[j]),
                'high':     float(highs[j]),
                'low':      float(lows[j]),
                'volume':   float(volumes[j]),
                'change':   chg,
                'ma5':ma5[j], 'ma10':ma10[j], 'ma20':ma20[j], 'ma60':ma60[j],
                'dif':dif_arr[j],
                'dea':dea_arr[j] if dea_arr[j] is not None else 0,
                'macd':macd_arr[j],
                'prev_dif':  dif_arr[j-1]  if j>0 else 0,
                'prev_dea':  dea_arr[j-1]  if j>0 and dea_arr[j-1] is not None else 0,
                'prev_macd': macd_arr[j-1] if j>0 else 0,
                'rsi':rsi_arr[j],
                'prev_rsi':rsi_arr[j-1] if j>0 else None,
                'boll_upper':boll_u[j], 'boll_mid':boll_m[j], 'boll_lower':boll_l[j],
                'boll_pos':boll_pos,
                'near_lower':1 if boll_pos<0.2 else 0,
                'adx':adx_arr[j], 'pdi':pdi_arr[j], 'mdi':mdi_arr[j],
                'mdi_gt_pdi':1 if mdi_arr[j]>pdi_arr[j] else 0,
                'mom5':mom5[j], 'mom10':mom10[j], 'mom20':mom20[j], 'mom60':mom60[j],
                'mom_all_neg':1 if mom_all_neg else 0,
                'mom_neg_cnt':mom_neg_cnt,
                'hv20':hv20[j], 'hv60':hv60[j],
                'atr14':atr_v, 'atr_pct':atr_pct,
                'vol_accel':vol_accel[j],
                'cmf':cmf_arr[j],
                'drawdown':drawdown,
                'dd_deep':1 if drawdown<-0.20 else 0,
                'dd_medium':1 if -0.20<=drawdown<-0.10 else 0,
                'dd_shallow':1 if -0.10<=drawdown<-0.02 else 0,
                'hammer':hammer[j], 'engulf':engulf[j], 'doji':doji[j],
                'prev_close':closes[j-1] if j>0 else closes[j],
            }
        indicators[stock]=stock_ind

    log_print("  预计算完成,共 %d 只" % len(indicators))
    return indicators


# ============================================================
# 评分函数（完全不动，仅此处省略注释节省篇幅，逻辑100%保留）
# ============================================================

def calc_reversal_score(ind, regime):
    score = 0; signals = []
    rsi   = ind.get('rsi', 50) or 50
    mdi   = ind.get('mdi', 0);  pdi   = ind.get('pdi', 0)
    mom60 = ind.get('mom60', 0); mom20 = ind.get('mom20', 0)
    mom10 = ind.get('mom10', 0); mom5  = ind.get('mom5', 0)
    hv20  = ind.get('hv20',  0); hv60  = ind.get('hv60',  0)
    dd    = ind.get('drawdown', 0);  bpos = ind.get('boll_pos', 0.5)
    va    = ind.get('vol_accel', 1.0); cmf  = ind.get('cmf', 0)
    neg_cnt = ind.get('mom_neg_cnt', 0)
    prev_rsi = ind.get('prev_rsi', rsi)
    dif  = ind.get('dif', 0);  dea  = ind.get('dea', 0)
    pdif = ind.get('prev_dif', 0); pdea = ind.get('prev_dea', 0)

    if regime == '牛市':
        if hv60 > 0:
            if hv60 < 0.20:    score += 18; signals.append("[牛]hv60极低+18")
            elif hv60 < 0.30:  score += 12; signals.append("[牛]hv60低+12")
            elif hv60 < 0.45:  score +=  6; signals.append("[牛]hv60中+6")
            elif hv60 > 0.70:  score -= 10; signals.append("[牛]hv60高-10")
        if hv20 > 0:
            if hv20 < 0.20:    score += 12; signals.append("[牛]hv20极低+12")
            elif hv20 < 0.35:  score +=  7; signals.append("[牛]hv20低+7")
            elif hv20 > 0.70:  score -=  8; signals.append("[牛]hv20高-8")
        if mom60 < -0.20:   score += 12; signals.append("[牛]mom60<-20%+12")
        elif mom60 < -0.10: score +=  7; signals.append("[牛]mom60<-10%+7")
        elif mom60 < 0:     score +=  3; signals.append("[牛]mom60负+3")
        elif mom60 > 0.20:  score -=  8; signals.append("[牛]mom60>20%-8")
        if mdi > 30 and mdi > pdi:  score += 14; signals.append("[牛]MDI>PDI强+14")
        elif mdi > 20 and mdi > pdi: score +=  8; signals.append("[牛]MDI>PDI+8")
        elif mdi > pdi:              score +=  4; signals.append("[牛]MDI>PDI弱+4")
        else:                        score -=  6; signals.append("[牛]PDI>MDI-6")
        if dif < 0 and dea < 0:
            if dif > dea and pdif <= pdea: score += 14; signals.append("[牛]MACD负区金叉+14")
            elif dif > dea:                score +=  6; signals.append("[牛]MACD负区dif>dea+6")
        elif dif > 0 and dea > 0:         score -=  6; signals.append("[牛]MACD正区-6")
        if -0.15 < dd < -0.05:  score += 10; signals.append("[牛]适度回撤+10")
        elif -0.05 <= dd < 0:   score +=  5; signals.append("[牛]浅回撤+5")
        elif dd < -0.25:        score -=  5; signals.append("[牛]过深回撤-5")
        if rsi < 30:    score += 10; signals.append("[牛]RSI极低+10")
        elif rsi < 40:  score +=  6; signals.append("[牛]RSI低+6")
        elif rsi < 50:  score +=  3; signals.append("[牛]RSI偏低+3")
        elif rsi > 70:  score -= 10; signals.append("[牛]RSI超买-10")
        elif rsi > 60:  score -=  5; signals.append("[牛]RSI偏高-5")
        if prev_rsi and rsi > prev_rsi and rsi < 45:
            score += 4; signals.append("[牛]RSI回升+4")
        if 1.0 <= va <= 2.5:  score +=  6; signals.append("[牛]放量+6")
        elif va < 0.3:        score -= 10; signals.append("[牛]极缩量-10")
        if bpos > 0.80: score -= 8; signals.append("[牛]近上轨-8")
        if cmf < -0.10:  score +=  5; signals.append("[牛]CMF负+5")
        elif cmf > 0.15: score -=  6; signals.append("[牛]CMF高-6")

    elif regime == '震荡':
        if mom60 < -0.20:   score += 18; signals.append("[震]mom60<-20%+18")
        elif mom60 < -0.10: score += 12; signals.append("[震]mom60<-10%+12")
        elif mom60 < 0:     score +=  5; signals.append("[震]mom60负+5")
        elif mom60 > 0.20:  score -= 10; signals.append("[震]mom60>20%-10")
        if mom5 < -0.05:    score += 14; signals.append("[震]mom5负<-5%+14")
        elif mom5 < 0:      score +=  8; signals.append("[震]mom5负+8")
        elif mom5 > 0.05:   score -=  8; signals.append("[震]mom5正-8")
        if mdi > 30 and mdi > pdi:  score += 20; signals.append("[震]MDI>PDI强+20")
        elif mdi > 20 and mdi > pdi: score += 13; signals.append("[震]MDI>PDI+13")
        elif mdi > pdi:              score +=  6; signals.append("[震]MDI>PDI弱+6")
        else:                        score -=  8; signals.append("[震]PDI>MDI-8")
        if rsi < 25:    score += 18; signals.append("[震]RSI极超卖+18")
        elif rsi < 35:  score += 12; signals.append("[震]RSI超卖+12")
        elif rsi < 45:  score +=  6; signals.append("[震]RSI偏低+6")
        elif rsi > 70:  score -= 12; signals.append("[震]RSI超买-12")
        elif rsi > 60:  score -=  6; signals.append("[震]RSI偏高-6")
        if prev_rsi and rsi > prev_rsi and rsi < 45:
            score += 5; signals.append("[震]RSI回升+5")
        if bpos < 0.10:   score += 14; signals.append("[震]极近下轨+14")
        elif bpos < 0.20: score +=  9; signals.append("[震]近下轨+9")
        elif bpos < 0.30: score +=  4; signals.append("[震]偏近下轨+4")
        elif bpos > 0.80: score -= 10; signals.append("[震]近上轨-10")
        if hv60 > 0:
            if hv60 < 0.25:   score += 10; signals.append("[震]hv60低+10")
            elif hv60 < 0.40: score +=  5; signals.append("[震]hv60中+5")
            elif hv60 > 0.80: score -=  8; signals.append("[震]hv60高-8")
        if 1.2 <= va <= 2.5:  score += 12; signals.append("[震]放量+12")
        elif 1.0 <= va < 1.2: score +=  6; signals.append("[震]温和放量+6")
        elif va < 0.3:        score -= 10; signals.append("[震]极缩量-10")
        if dd < -0.20:    score += 10; signals.append("[震]极深回撤+10")
        elif dd < -0.10:  score +=  7; signals.append("[震]深回撤+7")
        elif dd < -0.05:  score +=  3; signals.append("[震]中回撤+3")
        if dif < 0 and dea < 0:
            if dif > dea and pdif <= pdea: score += 8; signals.append("[震]MACD负区金叉+8")
            elif dif > dea:               score += 3; signals.append("[震]MACD负区dif>dea+3")
        if cmf < -0.10:  score +=  4; signals.append("[震]CMF负+4")
        elif cmf > 0.15: score -=  5; signals.append("[震]CMF高-5")

    else:  # 熊市
        if ind.get('mom_all_neg', 0):
            score += 20; signals.append("[熊]动量全负+20")
        else:
            if neg_cnt == 3: score += 14; signals.append("[熊]三周期负+14")
            elif neg_cnt == 2: score +=  8; signals.append("[熊]双周期负+8")
            elif neg_cnt == 0: score -= 12; signals.append("[熊]动量全正-12")
        if mom10 < -0.15:   score += 14; signals.append("[熊]mom10<-15%+14")
        elif mom10 < -0.08: score +=  9; signals.append("[熊]mom10<-8%+9")
        elif mom10 < 0:     score +=  4; signals.append("[熊]mom10负+4")
        elif mom10 > 0.10:  score -= 10; signals.append("[熊]mom10正-10")
        if mom20 < -0.15:   score += 12; signals.append("[熊]mom20<-15%+12")
        elif mom20 < -0.08: score +=  7; signals.append("[熊]mom20<-8%+7")
        if rsi < 25:    score += 18; signals.append("[熊]RSI极超卖+18")
        elif rsi < 35:  score += 12; signals.append("[熊]RSI超卖+12")
        elif rsi < 45:  score +=  6; signals.append("[熊]RSI偏低+6")
        elif rsi > 70:  score -= 14; signals.append("[熊]RSI超买-14")
        elif rsi > 60:  score -=  7; signals.append("[熊]RSI偏高-7")
        if prev_rsi and rsi > prev_rsi and rsi < 45:
            score += 5; signals.append("[熊]RSI回升+5")
        if bpos < 0.10:   score += 16; signals.append("[熊]极近下轨+16")
        elif bpos < 0.20: score += 11; signals.append("[熊]近下轨+11")
        elif bpos < 0.30: score +=  5; signals.append("[熊]偏近下轨+5")
        elif bpos > 0.80: score -= 12; signals.append("[熊]近上轨-12")
        if ind.get('near_lower', 0):
            score += 12; signals.append("[熊]near_lower+12")
        if mdi > 30 and mdi > pdi:  score += 22; signals.append("[熊]MDI>PDI强+22")
        elif mdi > 20 and mdi > pdi: score += 14; signals.append("[熊]MDI>PDI+14")
        elif mdi > pdi:              score +=  7; signals.append("[熊]MDI>PDI弱+7")
        else:                        score -= 10; signals.append("[熊]PDI>MDI-10")
        if dd < -0.25:    score += 16; signals.append("[熊]极深回撤+16")
        elif dd < -0.15:  score += 10; signals.append("[熊]深回撤+10")
        elif dd < -0.10:  score +=  5; signals.append("[熊]中回撤+5")
        if dif < 0 and dea < 0:
            if dif > dea and pdif <= pdea:
                score -= 8;  signals.append("[熊]MACD金叉反向-8")
            elif dif > dea:
                score -= 3;  signals.append("[熊]MACD dif>dea反向-3")
            else:
                score += 6;  signals.append("[熊]MACD死叉区域+6")
        elif dif > 0 and dea > 0:
            score -= 10; signals.append("[熊]MACD正区-10")
        if hv60 > 0:
            if hv60 < 0.25:  score +=  5; signals.append("[熊]hv60低+5")
            elif hv60 > 0.80: score -=  6; signals.append("[熊]hv60高-6")
        if cmf < -0.15:  score +=  8; signals.append("[熊]CMF极负+8")
        elif cmf < -0.05: score += 4; signals.append("[熊]CMF负+4")
        elif cmf > 0.15: score -=  8; signals.append("[熊]CMF高-8")
        if va < 0.3:     score -= 10; signals.append("[熊]极缩量-10")
        elif 1.0 <= va <= 2.0: score += 5; signals.append("[熊]温和放量+5")

    if ind.get('hammer', 0): score += 7; signals.append("锤子线+7")
    if ind.get('engulf', 0): score += 5; signals.append("吞没+5")
    return score, signals


def calc_momentum_score(ind):
    score = 0; signals = []
    ma5=ind.get('ma5'); ma10=ind.get('ma10'); ma20=ind.get('ma20'); ma60=ind.get('ma60')
    close=ind.get('close',0); dif=ind.get('dif',0); dea=ind.get('dea',0)
    pdif=ind.get('prev_dif',0); pdea=ind.get('prev_dea',0)
    va=ind.get('vol_accel',1.0); mom20=ind.get('mom20',0); mom60=ind.get('mom60',0)
    rsi=ind.get('rsi',50) or 50

    if ma5 and ma10 and ma20 and ma60:
        if ma5 > ma10 > ma20 > ma60: score += 25; signals.append("[动]MA全多头+25")
        elif ma5 > ma10 > ma20:      score += 15; signals.append("[动]MA短期多头+15")
        elif close > ma20 and close > ma60: score += 8; signals.append("[动]站MA20/60+8")
        elif close < ma20:           score -= 15; signals.append("[动]跌破MA20-15")
    if va > 2.0:    score += 18; signals.append("[动]强放量+18")
    elif va > 1.5:  score += 12; signals.append("[动]放量+12")
    elif va > 1.2:  score +=  6; signals.append("[动]温和放量+6")
    elif va < 0.5:  score -= 12; signals.append("[动]极缩量-12")
    if dif > 0 and dea > 0:
        macd_v=ind.get('macd',0); prev_macd=ind.get('prev_macd',0)
        if macd_v > 0 and prev_macd > 0 and macd_v > prev_macd:
            score += 15; signals.append("[动]MACD红柱扩大+15")
        elif dif > dea and pdif <= pdea: score += 12; signals.append("[动]MACD金叉+12")
        elif dif > dea: score += 6; signals.append("[动]MACD多头+6")
    elif dif < 0 and dea < 0:
        if dif > dea and pdif <= pdea: score += 5; signals.append("[动]MACD负区金叉+5")
        else: score -= 10; signals.append("[动]MACD空头区-10")
    if mom20 > 0.10:   score += 12
    elif mom20 > 0.05: score +=  7
    elif mom20 > 0:    score +=  3
    elif mom20 < -0.10:score -= 10
    if mom60 > 0.20:   score +=  8
    elif mom60 > 0.10: score +=  4
    elif mom60 < 0:    score -=  5
    if 45 <= rsi <= 70: score += 5
    elif rsi > 80:      score -= 8
    elif rsi < 40:      score -= 5
    return score, signals


def calc_fund_score_cached(ContextInfo, stock_code, fund_cache):
    if stock_code in fund_cache: return fund_cache[stock_code]
    score = 0; signals = []; got_data = False
    try:
        fin = ContextInfo.get_financial_data(
            ['PERSHAREINDEX.du_profit_rate','PERSHAREINDEX.du_return_on_equity',
             'PERSHAREINDEX.gear_ratio','PERSHAREINDEX.s_fa_ocfps'],
            [stock_code],'20200101','20251231')
        if fin is not None and len(fin)>0:
            latest = fin.iloc[-1] if hasattr(fin,'iloc') else fin
            def safe_get(key):
                try:
                    v = latest.get(key,None) if hasattr(latest,'get') else None
                    if v is None and hasattr(latest,'index'):
                        for col in latest.index:
                            if key in str(col): v=latest[col]; break
                    return v
                except: return None
            def valid(v):
                try: return v is not None and not (isinstance(v,float) and math.isnan(v))
                except: return False
            profit_yoy=safe_get('du_profit_rate'); roe=safe_get('du_return_on_equity')
            cf=safe_get('s_fa_ocfps')
            if valid(profit_yoy):
                got_data=True
                if profit_yoy>0: score+=5
                else: score-=3
            if valid(roe):
                got_data=True
                if roe>10: score+=5
                elif roe>0: score+=2
            if valid(cf):
                got_data=True
                if cf>0: score+=5
    except: pass
    if not got_data: score=5; signals=["默认5"]
    fund_cache[stock_code]=(score,signals)
    return score, signals


# ============================================================
# 市场环境预计算（print→log_print）
# ============================================================

def precompute_index_env(ContextInfo):
    log_print("\n预计算指数环境...")
    index_list=['000300.SH','000905.SH','000852.SH','000688.SH','399006.SZ']
    try:
        bt=datetime.datetime.strptime(BACKTEST_START,"%Y%m%d")
        ds=(bt-datetime.timedelta(days=60)).strftime("%Y%m%d")
    except: ds=BACKTEST_START
    env_data={}
    for idx in index_list:
        try:
            raw=ContextInfo.get_market_data_ex(['close'],[idx],period='1d',
                                                start_time=ds,end_time=BACKTEST_END)
            if idx in raw:
                df=raw[idx]; closes=df['close'].values
                dates=[str(d)[:8] if len(str(d))>8 else str(d) for d in df.index]
                ebd={}
                for j in range(len(dates)):
                    if j<19: ebd[dates[j]]=(1.0,"数据不足"); continue
                    ma20=sum(closes[j-19:j+1])/20.0; curr=closes[j]
                    chg5=(closes[j]/closes[j-5]-1) if j>=5 else 0
                    if curr>ma20 and chg5>0:  ebd[dates[j]]=(1.0,"正常")
                    elif curr>ma20:           ebd[dates[j]]=(0.8,"谨慎")
                    else:                     ebd[dates[j]]=(0.5,"防守")
                env_data[idx]=ebd
        except: pass
    log_print("  完成,共 %d 个指数" % len(env_data))
    return env_data


def precompute_industry_index(ContextInfo):
    log_print("\n预计算行业指数乖离率...")
    index_codes=list(set(SW1_INDEX_MAP.values()))
    try:
        bt=datetime.datetime.strptime(BACKTEST_START,"%Y%m%d")
        ds=(bt-datetime.timedelta(days=60)).strftime("%Y%m%d")
    except: ds=BACKTEST_START
    try:
        raw=ContextInfo.get_market_data_ex(['close'],index_codes,period='1d',
                                            start_time=ds,end_time=BACKTEST_END)
    except Exception as e:
        log_print("  获取失败: %s"%str(e)); return {}
    ind_env={}
    for idx in index_codes:
        if idx not in raw or len(raw[idx])==0: continue
        df=raw[idx]; closes=df['close'].values
        dates=[str(d)[:8] if len(str(d))>8 else str(d) for d in df.index]
        n=len(closes); idx_env={}
        for j in range(n):
            if j<19:
                idx_env[dates[j]]={'close':closes[j],'ma20':None,'bias':0,'allow':True}
                continue
            ma20=sum(closes[j-19:j+1])/20.0
            bias=(closes[j]/ma20-1) if ma20>0 else 0
            allow = not (bias >= 0.20)
            idx_env[dates[j]]={'close':closes[j],'ma20':ma20,'bias':bias,'allow':allow}
        ind_env[idx]=idx_env
    log_print("  完成: %d个" % len(ind_env))
    return ind_env


def get_env_coef(ContextInfo, stock_code, date, env_data):
    if   stock_code in ContextInfo.hs300_set:  idx='000300.SH'
    elif stock_code in ContextInfo.zz500_set:   idx='000905.SH'
    elif stock_code in ContextInfo.zz1000_set:  idx='000852.SH'
    elif stock_code in ContextInfo.kcb_set:     idx='000688.SH'
    elif stock_code in ContextInfo.cyb_set:     idx='399006.SZ'
    else: return 1.0, "默认"
    if idx in env_data and date in env_data[idx]: return env_data[idx][date]
    return 1.0, "缺失"

def check_industry_index(ContextInfo, stock_code, date, ind_env):
    sw1=ContextInfo.stock_sw1.get(stock_code,None)
    if not sw1: return True, 0, "无行业"
    idx=SW1_INDEX_MAP.get(sw1,None)
    if not idx: return True, 0, "无指数"
    if idx not in ind_env or date not in ind_env[idx]: return True, 0, "无数据"
    env=ind_env[idx][date]
    if not env['allow']: return False, env['bias']*100, "%s过热" % sw1
    return True, env['bias']*100, "%s正常" % sw1

def get_industry(name):
    keywords={
        '银行':['银行'],'证券':['证券','券商'],'保险':['保险','人寿'],
        '房地产':['地产','置业','物业'],'医药':['医药','制药','药业','生物','医疗'],
        '白酒':['酒','茅台','五粮液','汾酒'],'新能源':['新能源','锂电','光伏','风电'],
        '半导体':['芯片','半导体','集成电路'],'汽车':['汽车','车业'],
        '钢铁':['钢铁','特钢'],'有色':['铜','铝','锌','稀土'],
        '煤炭':['煤炭','煤业'],'电力':['电力','发电'],'科技':['科技','信息','软件']
    }
    for ind,kws in keywords.items():
        for kw in kws:
            if kw in name: return ind
    return '其他'

def get_industry_sw1(ContextInfo, stock_code, name):
    sw1=ContextInfo.stock_sw1.get(stock_code,None)
    if sw1: return sw1
    ind=get_industry(name)
    if ind!='其他': return ind
    if stock_code.startswith('688'): return '科创板'
    elif stock_code.startswith('300'): return '创业板'
    return '其他'


# ============================================================
# 交易执行（不动）
# ============================================================

def execute_buy(ContextInfo, stock, price, date, score_info, industry, ind, regime, pool='rev'):
    if pool == 'mom': pos_size = MOM_POSITION_SIZE; atr_mult = MOM_ATR_STOP_MULT
    elif regime == '牛市': pos_size = POSITION_SIZE; atr_mult = ATR_STOP_MULT_BULL
    elif regime == '震荡': pos_size = POSITION_SIZE; atr_mult = ATR_STOP_MULT_OSC
    else: pos_size = POSITION_SIZE; atr_mult = ATR_STOP_MULT_BEAR
    shares = int(pos_size / price / 100) * 100
    if shares < 100: return False
    cost = price * (1 + COMMISSION_RATE + SLIPPAGE)
    if shares * cost > ContextInfo.available: return False
    atr14 = ind.get('atr14', price * 0.02)
    if atr14 <= 0: atr14 = price * 0.02
    atr_stop = price - atr_mult * atr14
    fixed_stop = price * (1 + STOP_LOSS)
    dyn_stop = max(atr_stop, fixed_stop)
    ContextInfo.available -= shares * cost
    ContextInfo.positions[stock] = {
        'cost': cost, 'shares': shares, 'buy_date': date, 'buy_price': price,
        'buy_score': score_info['total'], 'buy_rps': score_info.get('rps', 0),
        'buy_regime': regime, 'pool': pool, 'days_held': 0, 'industry': industry,
        'atr14': atr14, 'atr_mult': atr_mult, 'dynamic_stop': dyn_stop, 'trail_high': price,
    }
    return True

def execute_sell(ContextInfo, stock, price, date, reason, name):
    if stock not in ContextInfo.positions: return False
    pos=ContextInfo.positions[stock]
    sell_price=price*(1-COMMISSION_RATE-STAMP_TAX-SLIPPAGE)
    proceeds=pos['shares']*sell_price
    pnl=proceeds-pos['shares']*pos['cost']
    pnl_pct=(sell_price/pos['cost']-1)*100
    regime=pos.get('buy_regime','震荡'); pool=pos.get('pool','rev')
    ContextInfo.trade_records.append({
        'stock':stock,'name':name,'buy_date':pos['buy_date'],'buy_price':pos['buy_price'],
        'sell_date':date,'sell_price':price,'shares':pos['shares'],'pnl':pnl,'pnl_pct':pnl_pct,
        'days_held':pos['days_held'],'reason':reason,'buy_rps':pos.get('buy_rps',0),
        'buy_regime':regime,'pool':pool,
    })
    ContextInfo.stats['total_trades'] += 1
    if pnl > 0:
        ContextInfo.stats['win_trades'] += 1
        ContextInfo.stats['regime_wins'][regime]=ContextInfo.stats['regime_wins'].get(regime,0)+1
        ContextInfo.stats['pool_'+pool]['wins'] += 1
    else: ContextInfo.stats['loss_trades'] += 1
    ContextInfo.stats['exit_reasons'][reason]=ContextInfo.stats['exit_reasons'].get(reason,0)+1
    ContextInfo.stats['regime_trades'][regime]=ContextInfo.stats['regime_trades'].get(regime,0)+1
    ContextInfo.stats['pool_'+pool]['n'] += 1
    ContextInfo.stats['pool_'+pool]['pnl'] += pnl_pct
    ContextInfo.available += proceeds
    del ContextInfo.positions[stock]
    return True


# ============================================================
# 因子/候选记录（不动）
# ============================================================

def record_factor_log(ContextInfo, stock_code, name, date, price,
                      ind, reversal_score, fund_score, rps, regime, sw1):
    ContextInfo.factor_log.append({
        'stock':stock_code, 'name':name[:6], 'buy_date':date,
        'buy_price':price, 'regime':regime, 'sw1':sw1,
        'reversal_score':round(reversal_score,1), 'fund_score':fund_score, 'rps':round(rps,1),
        'mdi':round(ind.get('mdi',0),2), 'pdi':round(ind.get('pdi',0),2),
        'mdi_gt_pdi':ind.get('mdi_gt_pdi',0),
        'rsi':round(ind.get('rsi',50) or 50,2),
        'mom60':round(ind.get('mom60',0),4), 'mom20':round(ind.get('mom20',0),4),
        'mom_all_neg':ind.get('mom_all_neg',0), 'mom_neg_cnt':ind.get('mom_neg_cnt',0),
        'hv20':round(ind.get('hv20',0),4), 'hv60':round(ind.get('hv60',0),4),
        'drawdown':round(ind.get('drawdown',0),4), 'boll_pos':round(ind.get('boll_pos',0.5),4),
        'near_lower':ind.get('near_lower',0), 'vol_accel':round(ind.get('vol_accel',1),4),
        'cmf':round(ind.get('cmf',0),4), 'atr_pct':round(ind.get('atr_pct',0.02),4),
        'dif':round(ind.get('dif',0),4), 'dea':round(ind.get('dea',0),4),
        'dd_deep':ind.get('dd_deep',0), 'hammer':ind.get('hammer',0), 'engulf':ind.get('engulf',0),
    })

def record_candidate_log(ContextInfo, stock_code, name, date, price,
                         ind, pool, regime, rank,
                         total_score, rps, rev_score=0, mom_score=0,
                         fund_score=0, hard_cnt=0, sw1='其他'):
    ContextInfo.candidate_log.append({
        'stock':stock_code, 'name':name[:6], 'candidate_date':date,
        'price':round(price,4), 'pool':pool, 'regime':regime, 'rank':rank,
        'total_score':round(total_score,1), 'rps':round(rps,1), 'hard_cnt':hard_cnt,
        'rev_score':round(rev_score,1), 'mom_score':round(mom_score,1),
        'fund_score':fund_score, 'sw1':sw1,
        'mdi':round(ind.get('mdi',0),2), 'pdi':round(ind.get('pdi',0),2),
        'rsi':round(ind.get('rsi',50) or 50,2),
        'mom60':round(ind.get('mom60',0),4), 'mom20':round(ind.get('mom20',0),4),
        'mom5':round(ind.get('mom5',0),4),
        'drawdown':round(ind.get('drawdown',0),4), 'vol_accel':round(ind.get('vol_accel',1),4),
        'boll_pos':round(ind.get('boll_pos',0.5),4), 'hv60':round(ind.get('hv60',0),4),
        'atr_pct':round(ind.get('atr_pct',0.02),4), 'near_lower':ind.get('near_lower',0),
        'ma20_ok':1 if (ind.get('ma20') and ind.get('close',0) > ind.get('ma20',0)) else 0,
        'actually_bought':False, 'skip_reason':'',
        'pnl_pct':'', 'win':'', 'days_held':'', 'sell_reason':'',
        'fwd_5d_pct':'', 'fwd_10d_pct':'', 'fwd_20d_pct':'',
    })
    return len(ContextInfo.candidate_log) - 1


# ============================================================
# 回测主循环（print→log_print，末尾改为写文件）
# ============================================================

def run_backtest(ContextInfo):
    log_print("\n" + "="*80)
    log_print("开始回测 v13（排序修正+动量观察队列+文件输出v13.1）...")
    log_print("="*80)

    try:
        trade_dates=ContextInfo.get_trading_dates("SH",BACKTEST_START,BACKTEST_END,2000)
        trade_dates=[str(d) for d in trade_dates]
    except Exception as e:
        log_print("获取交易日失败: %s"%str(e)); return
    log_print("交易日: %d 天 (%s ~ %s)" % (len(trade_dates),trade_dates[0],trade_dates[-1]))

    log_print("\n获取行情数据(分批)...")
    try:
        bt=datetime.datetime.strptime(BACKTEST_START,"%Y%m%d")
        ds=(bt-datetime.timedelta(days=180)).strftime("%Y%m%d")
    except: ds="20230701"

    all_market_data={}
    batch_size=300
    total_batches=(len(ContextInfo.stock_pool)+batch_size-1)//batch_size
    for b in range(total_batches):
        s=b*batch_size; e=min(s+batch_size,len(ContextInfo.stock_pool))
        batch=ContextInfo.stock_pool[s:e]
        log_print("  第 %d/%d 批 (%d只)..." % (b+1,total_batches,len(batch)))
        try:
            bd=ContextInfo.get_market_data_ex(['open','high','low','close','volume'],
                batch,period='1d',start_time=ds,end_time=BACKTEST_END)
            all_market_data.update(bd)
        except Exception as ex: log_print("  失败: %s"%str(ex))
    log_print("  完成,共 %d 只" % len(all_market_data))

    indicators=precompute_all_indicators(all_market_data, trade_dates)
    env_data=precompute_index_env(ContextInfo)
    ind_env=precompute_industry_index(ContextInfo)

    log_print("\n计算逐日RPS(120日)...")
    rps_data={}; stock_close_by_date={}
    for stock,df in all_market_data.items():
        dates_list=[str(d)[:8] if len(str(d))>8 else str(d) for d in df.index]
        closes_list=df['close'].values
        stock_close_by_date[stock]=dict(zip(dates_list,closes_list))
    all_dates_sorted=sorted(set(d for dmap in stock_close_by_date.values() for d in dmap))
    date_to_idx={d:i for i,d in enumerate(all_dates_sorted)}
    rps_period=120
    for di,date in enumerate(trade_dates):
        if date not in date_to_idx: continue
        didx=date_to_idx[date]
        if didx<rps_period: continue
        prev_date=all_dates_sorted[didx-rps_period]
        daily_returns={}
        for stock,dmap in stock_close_by_date.items():
            if date in dmap and prev_date in dmap and dmap[prev_date]>0:
                daily_returns[stock]=dmap[date]/dmap[prev_date]-1
        if daily_returns:
            sorted_st=sorted(daily_returns.items(),key=lambda x:x[1])
            total_st=len(sorted_st)
            rps_data[date]={st:(rank+1)/total_st*100 for rank,(st,_) in enumerate(sorted_st)}
        if (di+1)%20==0: log_print("  RPS已计算 %d/%d ..." % (di+1,len(trade_dates)))
    log_print("  RPS完成, %d 天" % len(rps_data))

    st_set=set(); name_map={}; ind_map={}; sw1_map={}
    for stock in ContextInfo.stock_pool:
        try:
            nm=ContextInfo.get_stock_name(stock); name_map[stock]=nm
            if 'ST' in nm: st_set.add(stock)
            sw1_map[stock]=get_industry_sw1(ContextInfo,stock,nm)
            ind_map[stock]=get_industry(nm)
            if ind_map[stock]=='其他':
                if stock.startswith('688'): ind_map[stock]='科创板'
                elif stock.startswith('300'): ind_map[stock]='创业板'
        except: name_map[stock]=stock[:6]; ind_map[stock]='其他'; sw1_map[stock]='其他'
    log_print("  ST股票: %d 只" % len(st_set))
    fund_cache={}

    log_print("\n开始逐日回测...")
    buy_count=0; sell_count=0

    for i, date in enumerate(trade_dates):
        day_buy=0; day_sell=0
        regime, normal_cnt, bear_cnt = get_market_regime(date, env_data)
        current_equity=ContextInfo.available
        for sc,pos in ContextInfo.positions.items():
            if sc in indicators and date in indicators[sc]:
                current_equity+=pos['shares']*indicators[sc][date]['close']

        # ===== 卖出 =====
        for stock_code in list(ContextInfo.positions.keys()):
            pos=ContextInfo.positions[stock_code]; pos['days_held']+=1
            if stock_code not in indicators or date not in indicators[stock_code]: continue
            ind=indicators[stock_code][date]; cur=ind['close']; cost=pos['cost']
            pnl_pct=cur/cost-1; ma20=ind.get('ma20'); pool=pos.get('pool','rev')
            if cur>pos.get('trail_high',0): pos['trail_high']=cur
            sell_reason=None

            if pool == 'mom':
                if cur <= pos.get('dynamic_stop', cost*(1+STOP_LOSS)): sell_reason='动态止损'
                elif ma20 and cur < ma20 and pnl_pct <= 0: sell_reason='趋势破位'
                elif pnl_pct > MOM_TRAIL_ACTIVATE:
                    trail_stop=pos['trail_high']-MOM_ATR_TRAIL_MULT*pos.get('atr14',cur*0.02)
                    if cur < trail_stop: sell_reason='移动止盈'
                elif pos['days_held'] >= MIN_HOLD_DAYS:
                    if pos['days_held'] > MOM_FORCE_EXIT_DAYS: sell_reason='强制出场'
                    elif pnl_pct < LOSS_EXIT_PNL and pos['days_held'] > LOSS_EXIT_DAYS: sell_reason='亏损超时'
            else:
                if cur<=pos.get('dynamic_stop',cost*(1+STOP_LOSS)): sell_reason='动态止损'
                elif pnl_pct>TRAIL_ACTIVATE:
                    trail_stop=pos['trail_high']-ATR_TRAIL_MULT*pos.get('atr14',cur*0.02)
                    if cur<trail_stop: sell_reason='移动止盈'
                elif pos['days_held']>=MIN_HOLD_DAYS:
                    if pos['days_held'] > FORCE_EXIT_DAYS: sell_reason='强制出场'
                    elif pnl_pct>=MA20_PROFIT_MIN and ma20 and cur<ma20: sell_reason='止盈'
                    elif pnl_pct < LOSS_EXIT_PNL and pos['days_held'] > LOSS_EXIT_DAYS: sell_reason='亏损超时'
                    if sell_reason is None and pos['days_held']>=SIGNAL_EXIT_DAYS:
                        rev_s,_=calc_reversal_score(ind, regime)
                        fund_s,_=calc_fund_score_cached(ContextInfo,stock_code,fund_cache)
                        env_c,_=get_env_coef(ContextInfo,stock_code,date,env_data)
                        total_s=(rev_s+fund_s)*env_c
                        if total_s<SIGNAL_EXIT_SCORE and pnl_pct<=0: sell_reason='信号消失'
                        elif ind.get('rsi',50) and ind['rsi']>60 and pnl_pct>0.05: sell_reason='反转完成'

            if sell_reason:
                nm=name_map.get(stock_code,'')
                log_print("  %s | 卖出[%s] | %-8s | %.2f | %s(%.1f%%) [持%d天][%s]" %
                      (date,pool,nm[:8],cur,sell_reason,pnl_pct*100,pos['days_held'],pos.get('buy_regime','?')))
                execute_sell(ContextInfo,stock_code,cur,date,sell_reason,nm)
                sell_count+=1; day_sell+=1

        # ===== 买入 =====
        max_pos = MAX_POSITIONS_BEAR if regime == '熊市' else MAX_POSITIONS_BULL
        peak_dd = (ContextInfo.max_equity - current_equity) / ContextInfo.max_equity if ContextInfo.max_equity > 0 else 0

        if not ContextInfo.dd_stopped and peak_dd >= DD_STOP_RATIO:
            ContextInfo.dd_stopped=True; ContextInfo.dd_reduced=True; ContextInfo.dd_stop_day=i
            log_print("  [峰值回撤停买 %s] 从峰值回撤%.1f%%>=%.0f%%，停止新买" % (date, peak_dd*100, DD_STOP_RATIO*100))
        elif not ContextInfo.dd_reduced and peak_dd >= DD_REDUCE_RATIO:
            ContextInfo.dd_reduced=True; ContextInfo.dd_stop_day=i
            log_print("  [峰值回撤降仓 %s] 从峰值回撤%.1f%%>=%.0f%%，仓位上限降至%d只" % (date, peak_dd*100, DD_REDUCE_RATIO*100, MAX_POSITIONS_BEAR))
        elif (ContextInfo.dd_reduced or ContextInfo.dd_stopped):
            days_stopped=i-ContextInfo.dd_stop_day
            force_resume=(ContextInfo.dd_stopped and days_stopped>=40)
            if peak_dd <= DD_RESUME_RATIO or force_resume:
                ContextInfo.dd_reduced=False; ContextInfo.dd_stopped=False
                if force_resume:
                    ContextInfo.max_equity=current_equity
                    log_print("  [强制恢复 %s] 停买已%d天，重置峰值基准%.2f万，恢复买入" % (date,days_stopped,current_equity/10000))
                else:
                    log_print("  [峰值回撤恢复 %s] 回撤收窄至%.1f%%，恢复正常" % (date, peak_dd*100))

        if ContextInfo.dd_reduced: max_pos = min(max_pos, MAX_POSITIONS_BEAR)

        rev_pool_cnt = sum(1 for p in ContextInfo.positions.values() if p.get('pool','rev')=='rev')
        mom_now = sum(1 for p in ContextInfo.positions.values() if p.get('pool')=='mom')
        has_work = (rev_pool_cnt < max_pos) or (regime != '熊市')

        if has_work:
            equity_ratio = current_equity / INITIAL_CAPITAL
            if not ContextInfo.circuit_broken and equity_ratio < EQUITY_STOP_RATIO:
                ContextInfo.circuit_broken=True
                log_print("  [绝对值熔断 %s] 权益%.1f%%，停止买入" % (date, equity_ratio*100))
            elif ContextInfo.circuit_broken and equity_ratio >= EQUITY_RESUME_RATIO:
                ContextInfo.circuit_broken=False
                log_print("  [绝对值熔断解除 %s] 权益%.1f%%，恢复买入" % (date, equity_ratio*100))

            if ContextInfo.circuit_broken or ContextInfo.dd_stopped:
                if i % 20 == 0:
                    log_print("  [停买中 %s] 峰值回撤%.1f%% 权益%.1f%%" % (date, peak_dd*100, equity_ratio*100))
            else:
                ind_count={}
                for st,po in ContextInfo.positions.items():
                    inm=po.get('industry','其他'); ind_count[inm]=ind_count.get(inm,0)+1

                buy_candidates=[]; dbg={'no_data':0,'rps_fail':0,'hard_fail':0,'bear_dd_fail':0,'score_fail':0,'fund_fail':0,'ind_fail':0,'pass':0}
                debug_day=(i==0 or (i+1)%20==0)

                for stock_code in ContextInfo.stock_pool:
                    if stock_code in ContextInfo.positions or stock_code in st_set: continue
                    if stock_code not in indicators or date not in indicators[stock_code]: dbg['no_data']+=1; continue
                    ind=indicators[stock_code][date]
                    stock_rps=rps_data.get(date,{}).get(stock_code,50)
                    if stock_rps < RPS_MIN or stock_rps > RPS_MAX: dbg['rps_fail']+=1; continue
                    hard_cnt=0; rsi_v=ind.get('rsi',50) or 50
                    if rsi_v<45: hard_cnt+=1
                    if ind.get('mdi_gt_pdi',0): hard_cnt+=1
                    if ind.get('mom_all_neg',0): hard_cnt+=1
                    if ind.get('near_lower',0): hard_cnt+=1
                    if ind.get('drawdown',0)<-0.10: hard_cnt+=1
                    if ind.get('mom60',0)<-0.10: hard_cnt+=1
                    if hard_cnt<2: dbg['hard_fail']+=1; continue
                    if ind.get('vol_accel',1)<0.3: dbg['hard_fail']+=1; continue
                    if regime=='熊市':
                        if ind.get('drawdown',0)>BEAR_DRAWDOWN_MIN: dbg['bear_dd_fail']+=1; continue
                    rev_score,rev_sigs=calc_reversal_score(ind, regime)
                    fund_score,_=calc_fund_score_cached(ContextInfo,stock_code,fund_cache)
                    env_c,_=get_env_coef(ContextInfo,stock_code,date,env_data)
                    env_min=BUY_ENV_COEF if regime!='熊市' else 0.5
                    if env_c<env_min: dbg['score_fail']+=1; continue
                    total_score=(rev_score+fund_score)*env_c
                    if total_score<BUY_REVERSAL_SCORE: dbg['score_fail']+=1; continue
                    if fund_score<BUY_FUND_SCORE: dbg['fund_fail']+=1; continue
                    industry=ind_map.get(stock_code,'其他')
                    if ind_count.get(industry,0)>=MAX_INDUSTRY: dbg['ind_fail']+=1; continue
                    ind_allow,_,_=check_industry_index(ContextInfo,stock_code,date,ind_env)
                    if not ind_allow: dbg['ind_fail']+=1; continue
                    dbg['pass']+=1
                    buy_candidates.append({'stock':stock_code,'price':ind['close'],
                        'total_score':total_score,'rev_score':rev_score,'fund_score':fund_score,
                        'rps':stock_rps,'industry':industry,'hard_cnt':hard_cnt,
                        'rev_signals':rev_sigs,'ind':ind,'sw1':sw1_map.get(stock_code,'其他')})

                if debug_day:
                    log_print("  [调试 %s][%s 正常%d/防守%d] RPS[%.0f~%.0f]" % (date,regime,normal_cnt,bear_cnt,RPS_MIN,RPS_MAX))
                    log_print("    无数据:%d RPS:%d 硬:%d 熊市回撤:%d 评分:%d 基本面:%d 行业:%d 通过:%d" %
                          (dbg['no_data'],dbg['rps_fail'],dbg['hard_fail'],dbg['bear_dd_fail'],dbg['score_fail'],dbg['fund_fail'],dbg['ind_fail'],dbg['pass']))
                if buy_candidates:
                    log_print("  [筛选 %s][%s] RPS[%.0f~%.0f] → 候选:%d (仓位上限:%d)" % (date,regime,RPS_MIN,RPS_MAX,len(buy_candidates),max_pos))

                buy_candidates.sort(key=lambda x: (x['total_score']+x['hard_cnt']*5
                    +(SORT_MOM5_BOUNCE_BONUS if x['ind'].get('mom5',0)>0 else 0)
                    -(SORT_MOM5_DROP_PENALTY if x['ind'].get('mom5',0)<-0.05 else 0)), reverse=True)

                cand_idx_rev={}
                for rank_i, cand in enumerate(buy_candidates):
                    nm_c=name_map.get(cand['stock'],'')
                    idx=record_candidate_log(ContextInfo,cand['stock'],nm_c,date,cand['price'],
                        cand['ind'],'rev',regime,rank=rank_i+1,total_score=cand['total_score'],
                        rps=cand['rps'],rev_score=cand['rev_score'],fund_score=cand['fund_score'],
                        hard_cnt=cand['hard_cnt'],sw1=cand['sw1'])
                    cand_idx_rev[cand['stock']]=idx

                for cand in buy_candidates:
                    idx=cand_idx_rev[cand['stock']]
                    if rev_pool_cnt >= max_pos:
                        ContextInfo.candidate_log[idx]['skip_reason']='满仓_反转'; continue
                    cur_ind_count=sum(1 for p in ContextInfo.positions.values() if p.get('industry')==cand['industry'])
                    if cur_ind_count>=MAX_INDUSTRY:
                        ContextInfo.candidate_log[idx]['skip_reason']='行业上限'; continue
                    score_info={'total':cand['total_score'],'rps':cand['rps']}
                    if execute_buy(ContextInfo,cand['stock'],cand['price'],date,score_info,cand['industry'],cand['ind'],regime,'rev'):
                        ContextInfo.candidate_log[idx]['actually_bought']=True
                        buy_count+=1; day_buy+=1; rev_pool_cnt+=1
                        ind_count[cand['industry']]=ind_count.get(cand['industry'],0)+1
                        nm=name_map.get(cand['stock'],''); pos=ContextInfo.positions[cand['stock']]
                        log_print("  %s | 买入[反转/%s] | %-8s | %.2f | 分%.0f RPS:%.1f 硬:%d 回撤%.1f%% 止损%.2f" %
                              (date,regime,nm[:8],cand['price'],cand['total_score'],cand['rps'],cand['hard_cnt'],cand['ind'].get('drawdown',0)*100,pos['dynamic_stop']))
                        record_factor_log(ContextInfo,cand['stock'],nm,date,cand['price'],cand['ind'],cand['rev_score'],cand['fund_score'],cand['rps'],regime,cand['sw1'])
                    else: ContextInfo.candidate_log[idx]['skip_reason']='资金不足'

                # ===== 动量池 =====
                if regime == '熊市':
                    if ContextInfo.mom_watch:
                        cleared=len(ContextInfo.mom_watch); ContextInfo.mom_watch.clear()
                        ContextInfo.mom_watch_stats['bear_skip']+=cleared
                else:
                    mom_now=sum(1 for p in ContextInfo.positions.values() if p.get('pool')=='mom')
                    to_remove=[]
                    for watch_stock, watch in list(ContextInfo.mom_watch.items()):
                        if watch_stock in ContextInfo.positions: to_remove.append(watch_stock); continue
                        watch['days_waiting']+=1
                        if watch['days_waiting']>MOM_WATCH_MAX_DAYS:
                            to_remove.append(watch_stock); ContextInfo.mom_watch_stats['expired']+=1
                            log_print("  [观察超时] %s 等待%d天未回踩，放弃" % (name_map.get(watch_stock,watch_stock[:6]),watch['days_waiting']))
                            continue
                        if watch_stock not in indicators or date not in indicators[watch_stock]: continue
                        ind_w=indicators[watch_stock][date]; close_w=ind_w.get('close',0)
                        ma20_w=ind_w.get('ma20'); va_w=ind_w.get('vol_accel',1.0); ma5_w=ind_w.get('ma5')
                        if not ma20_w or ma20_w<=0 or close_w<=0: continue
                        near_ma20=(close_w<=ma20_w*MOM_PULLBACK_UPPER)
                        above_ma20=(close_w>=ma20_w*MOM_PULLBACK_LOWER)
                        vol_ok=(va_w<MOM_PULLBACK_VOL)
                        ma5_ok=(ma5_w is None or ma5_w>=ma20_w*0.99)
                        if near_ma20 and above_ma20 and vol_ok and ma5_ok:
                            if mom_now<MOM_MAX_POSITIONS:
                                score_info=watch['score_info']; industry_w=watch['industry']
                                nm_w=name_map.get(watch_stock,'')
                                if execute_buy(ContextInfo,watch_stock,close_w,date,score_info,industry_w,ind_w,regime,'mom'):
                                    buy_count+=1; day_buy+=1; mom_now+=1
                                    ContextInfo.mom_watch_stats['triggered']+=1
                                    pos_w=ContextInfo.positions[watch_stock]
                                    log_print("  %s | 买入[动量回踩/%s] | %-8s | %.2f | 信号价%.2f(%.1f%%) | 等%d天 | 量%.2f | 止损%.2f" %
                                          (date,regime,nm_w[:8],close_w,watch['signal_price'],(close_w/watch['signal_price']-1)*100,watch['days_waiting'],va_w,pos_w['dynamic_stop']))
                                    record_factor_log(ContextInfo,watch_stock,nm_w,date,close_w,ind_w,score_info['total'],0,score_info['rps'],regime,watch.get('sw1','其他'))
                                to_remove.append(watch_stock)
                    for s in to_remove: ContextInfo.mom_watch.pop(s,None)

                    mom_candidates=[]
                    for stock_code in ContextInfo.stock_pool:
                        if stock_code in ContextInfo.positions or stock_code in ContextInfo.mom_watch or stock_code in st_set: continue
                        if stock_code not in indicators or date not in indicators[stock_code]: continue
                        ind=indicators[stock_code][date]
                        stock_rps=rps_data.get(date,{}).get(stock_code,50)
                        if stock_rps<MOM_RPS_MIN: continue
                        ma20_v=ind.get('ma20'); ma5_v=ind.get('ma5'); ma10_v=ind.get('ma10')
                        if not ma20_v or ind['close']<ma20_v: continue
                        if ma5_v and ma10_v and not (ma5_v>ma10_v>ma20_v): continue
                        if ind.get('vol_accel',1.0)<1.2: continue
                        mom_score,_=calc_momentum_score(ind)
                        fund_score,_=calc_fund_score_cached(ContextInfo,stock_code,fund_cache)
                        env_c,_=get_env_coef(ContextInfo,stock_code,date,env_data)
                        total_score=(mom_score+fund_score)*env_c
                        if total_score<MOM_BUY_SCORE: continue
                        mom_candidates.append({'stock':stock_code,'price':ind['close'],
                            'total_score':total_score,'mom_score':mom_score,'fund_score':fund_score,
                            'rps':stock_rps,'industry':ind_map.get(stock_code,'其他'),
                            'sw1':sw1_map.get(stock_code,'其他'),'ind':ind})

                    if mom_candidates:
                        mom_candidates.sort(key=lambda x:x['total_score'],reverse=True)
                        cand_idx_mom={}
                        for rank_i,cand in enumerate(mom_candidates):
                            nm_c=name_map.get(cand['stock'],'')
                            idx=record_candidate_log(ContextInfo,cand['stock'],nm_c,date,cand['price'],
                                cand['ind'],'mom',regime,rank=rank_i+1,total_score=cand['total_score'],
                                rps=cand['rps'],mom_score=cand['mom_score'],fund_score=cand['fund_score'],sw1=cand['sw1'])
                            cand_idx_mom[cand['stock']]=idx
                        added_today=0
                        for cand in mom_candidates:
                            if len(ContextInfo.mom_watch)>=MOM_WATCH_MAX_SIZE:
                                min_stock=min(ContextInfo.mom_watch,key=lambda s:ContextInfo.mom_watch[s]['score_info']['total'])
                                if cand['total_score']>ContextInfo.mom_watch[min_stock]['score_info']['total']:
                                    ContextInfo.mom_watch.pop(min_stock); ContextInfo.mom_watch_stats['displaced']+=1
                                else:
                                    if cand['stock'] in cand_idx_mom: ContextInfo.candidate_log[cand_idx_mom[cand['stock']]]['skip_reason']='观察队列满'
                                    continue
                            ContextInfo.mom_watch[cand['stock']]={
                                'signal_date':date,'signal_price':cand['price'],
                                'ma20_at_signal':cand['ind'].get('ma20',cand['price']),
                                'score_info':{'total':cand['total_score'],'rps':cand['rps']},
                                'industry':cand['industry'],'sw1':cand['sw1'],'days_waiting':0}
                            ContextInfo.mom_watch_stats['added']+=1; added_today+=1
                        log_print("  [动量观察 %s][%s] RPS>=%.0f → 新候选%d只 加入队列%d只 队列总%d只" %
                              (date,regime,MOM_RPS_MIN,len(mom_candidates),added_today,len(ContextInfo.mom_watch)))

        # 净值更新
        equity=ContextInfo.available
        for stock,pos in ContextInfo.positions.items():
            if stock in indicators and date in indicators[stock]:
                equity+=pos['shares']*indicators[stock][date]['close']
        ContextInfo.equity_curve.append({'date':date,'equity':equity,'positions':len(ContextInfo.positions),'regime':regime})
        if equity>ContextInfo.max_equity: ContextInfo.max_equity=equity
        dd=(ContextInfo.max_equity-equity)/ContextInfo.max_equity
        if dd>ContextInfo.max_drawdown: ContextInfo.max_drawdown=dd

        _rev_n=sum(1 for p in ContextInfo.positions.values() if p.get('pool','rev')=='rev')
        _mom_n=sum(1 for p in ContextInfo.positions.values() if p.get('pool')=='mom')
        if day_buy>0 or day_sell>0:
            log_print("  %s ---- [%s] 持仓:%d(反转%d+动量%d) 权益:%.2f万 买%d卖%d" % (date,regime,len(ContextInfo.positions),_rev_n,_mom_n,equity/10000,day_buy,day_sell))
        elif i%5==0:
            log_print("  %s      [%s] 持仓:%d(反转%d+动量%d) 权益:%.2f万" % (date,regime,len(ContextInfo.positions),_rev_n,_mom_n,equity/10000))

    log_print("\n  回测完成! 买入%d次 卖出%d次 因子记录:%d笔 候选记录:%d笔" %
          (buy_count, sell_count, len(ContextInfo.factor_log), len(ContextInfo.candidate_log)))

    # ===== 回填候选股后续收益 =====
    log_print("  回填候选股后续收益...")
    for rec in ContextInfo.candidate_log:
        stock=rec['stock']; cdate=rec['candidate_date']; entry_price=rec['price']
        if stock not in stock_close_by_date or entry_price<=0: continue
        if cdate not in date_to_idx: continue
        didx=date_to_idx[cdate]; smap=stock_close_by_date[stock]
        for fwd,key in [(5,'fwd_5d_pct'),(10,'fwd_10d_pct'),(20,'fwd_20d_pct')]:
            fidx=didx+fwd
            if fidx<len(all_dates_sorted):
                fwd_date=all_dates_sorted[fidx]; fwd_close=smap.get(fwd_date)
                if fwd_close and fwd_close>0:
                    rec[key]=round((fwd_close/entry_price-1)*100,4)
    log_print("  完成")

    # ===== v13.1：先输出统计（保证不截断），再写CSV到文件 =====
    print_results(ContextInfo)
    print_candidate_analysis(ContextInfo)

    # 写独立CSV文件
    write_factor_csv_file(ContextInfo)
    write_candidate_csv_file(ContextInfo)
    write_trade_csv_file(ContextInfo)
    write_equity_csv(ContextInfo)

    log_print("\n" + "="*60)
    log_print("【v13.1 文件输出完成】目录: %s" % LOG_DIR)
    log_print("  backtest_log.txt     完整回测日志")
    log_print("  backtest_summary.txt 统计汇总")
    log_print("  candidate_log.csv    全量候选 (%d行)" % len(ContextInfo.candidate_log))
    log_print("  factor_log.csv       因子记录 (%d行)" % len(ContextInfo.factor_log))
    log_print("  trade_records.csv    交易记录 (%d行)" % len(ContextInfo.trade_records))
    log_print("  equity_curve.csv     权益曲线 (%d行)" % len(ContextInfo.equity_curve))
    log_print("="*60)
    close_log()


# ============================================================
# 统计汇总（同时输出控制台+日志+独立汇总文件）
# ============================================================

def print_results(ContextInfo):
    S = []  # 收集汇总行
    def sp(msg):
        log_print(msg); S.append(msg)

    sp("\n" + "="*80)
    sp("【回测统计】%s ~ %s  v13 排序修正+动量观察队列版" % (BACKTEST_START,BACKTEST_END))
    sp("="*80)
    tt=ContextInfo.stats['total_trades']; wt=ContextInfo.stats['win_trades']; lt=ContextInfo.stats['loss_trades']
    sp("\n【交易统计】总:%d | 盈:%d | 亏:%d | 胜率:%.1f%%" % (tt,wt,lt,(wt/tt*100 if tt>0 else 0)))

    if ContextInfo.equity_curve:
        fe=ContextInfo.equity_curve[-1]['equity']; tr=(fe/INITIAL_CAPITAL-1)*100
        days=len(ContextInfo.equity_curve); ar=((fe/INITIAL_CAPITAL)**(252.0/days)-1)*100 if days>0 and fe>0 else 0
        sp("\n【收益统计】")
        sp("  初始:%.0f万 | 最终:%.2f万 | 总收益:%.2f%% | 年化:%.2f%% | 最大回撤:%.2f%%" %
              (INITIAL_CAPITAL/10000,fe/10000,tr,ar,ContextInfo.max_drawdown*100))
    if ContextInfo.trade_records:
        profits=[t['pnl_pct'] for t in ContextInfo.trade_records if t['pnl']>0]
        losses=[t['pnl_pct'] for t in ContextInfo.trade_records if t['pnl']<=0]
        dh=[t['days_held'] for t in ContextInfo.trade_records]
        if profits: sp("  平均盈利:%.2f%%" % (sum(profits)/len(profits)))
        if losses:  sp("  平均亏损:%.2f%%" % (sum(losses)/len(losses)))
        if profits and losses:
            ap=sum(profits)/len(profits); al=abs(sum(losses)/len(losses))
            if al>0: sp("  盈亏比:%.2f" % (ap/al))
        sp("  平均持仓:%.1f天" % (sum(dh)/len(dh)))

    sp("\n【卖出原因】")
    for r,c in ContextInfo.stats['exit_reasons'].items():
        if c>0: sp("  %s: %d次 (%.1f%%)" % (r,c,c/tt*100 if tt>0 else 0))
    sp("\n【各市场阶段统计】")
    for regime in ['牛市','震荡','熊市']:
        trades=ContextInfo.stats['regime_trades'].get(regime,0); wins=ContextInfo.stats['regime_wins'].get(regime,0)
        wr=wins/trades*100 if trades>0 else 0
        max_p=MAX_POSITIONS_BEAR if regime=='熊市' else MAX_POSITIONS_BULL
        sp("  %-6s %-6d 胜%.1f%% (仓位上限%d)" % (regime,trades,wr,max_p))

    sp("\n【双池对比】")
    sp("  %-8s  %6s  %6s  %8s  %8s" % ("池类型","笔数","胜率","均收益","总贡献"))
    sp("  "+"-"*45)
    for pool_key,label in [('pool_rev','反转池(主)'),('pool_mom','动量池(副)')]:
        ps=ContextInfo.stats[pool_key]; n=ps['n']; wins_p=ps['wins']; pnl_sum=ps['pnl']
        wr=wins_p/n*100 if n>0 else 0; avg=pnl_sum/n if n>0 else 0
        sp("  %-8s  %6d  %5.1f%%  %+7.2f%%  %+8.1f%%" % (label,n,wr,avg,pnl_sum))

    ws=ContextInfo.mom_watch_stats; total_added=ws['added']
    if total_added>0:
        sp("\n【动量观察队列统计】")
        sp("  加入队列:%d | 触发入场:%d(%.1f%%) | 超时放弃:%d(%.1f%%)" %
              (total_added,ws['triggered'],ws['triggered']/total_added*100,ws['expired'],ws['expired']/total_added*100))
        sp("  被替换:%d | 熊市清空:%d | 回踩条件:MA20±%.0f%% 量能<%.1f ≤%d天" %
              (ws['displaced'],ws['bear_skip'],(MOM_PULLBACK_UPPER-1)*100,MOM_PULLBACK_VOL,MOM_WATCH_MAX_DAYS))

    sp("\n【RPS分区间胜率（反转池）】")
    rev_records=[t for t in ContextInfo.trade_records if t.get('pool','rev')=='rev']
    for lo,hi in [(0,5),(5,10),(10,15),(15,20)]:
        bin_t=[t for t in rev_records if lo<=t.get('buy_rps',0)<hi]
        if not bin_t: continue
        wins=[t for t in bin_t if t['pnl']>0]; wr=len(wins)/len(bin_t)*100
        avg=sum(t['pnl_pct'] for t in bin_t)/len(bin_t)
        sp("  RPS %d~%d: %d笔 胜率%.1f%% 均盈亏%.2f%%" % (lo,hi,len(bin_t),wr,avg))

    sp("\n【市场阶段分布】")
    rc={}
    for r in ContextInfo.equity_curve: rc[r.get('regime','震荡')]=rc.get(r.get('regime','震荡'),0)+1
    total_days=len(ContextInfo.equity_curve)
    for rg,cnt in sorted(rc.items()):
        sp("  %s: %d天(%.1f%%)" % (rg,cnt,cnt/total_days*100 if total_days else 0))
    sp("="*80)

    # v13.1：写独立汇总文件
    write_text_file(os.path.join(LOG_DIR,"backtest_summary.txt"), S)


def print_candidate_analysis(ContextInfo):
    """候选分析汇总（输出到控制台+日志，不含原始CSV）"""
    if not ContextInfo.candidate_log: return

    # 回填持仓结果
    pnl_map={}
    for t in ContextInfo.trade_records:
        pnl_map[(t['stock'],t['buy_date'])]={'pnl_pct':round(t['pnl_pct'],4),'win':1 if t['pnl']>0 else 0,'days_held':t['days_held'],'sell_reason':t['reason']}
    for rec in ContextInfo.candidate_log:
        if rec['actually_bought']:
            res=pnl_map.get((rec['stock'],rec['candidate_date']),{})
            if res: rec['pnl_pct']=res['pnl_pct']; rec['win']=res['win']; rec['days_held']=res['days_held']; rec['sell_reason']=res['sell_reason']

    log_print("\n" + "="*80)
    log_print("【全量候选分析】v13")
    log_print("="*80)

    rev_all=[r for r in ContextInfo.candidate_log if r['pool']=='rev']
    mom_all=[r for r in ContextInfo.candidate_log if r['pool']=='mom']
    rev_bought=[r for r in rev_all if r['actually_bought']]

    def safe_avg(lst, key):
        vals=[r[key] for r in lst if r.get(key)!='' and r.get(key) is not None]
        return sum(vals)/len(vals) if vals else 0.0

    log_print("\n  总候选: 反转池%d笔 | 动量池%d笔" % (len(rev_all),len(mom_all)))
    log_print("  实际买入: 反转%d笔(%.1f%%) | 动量%d笔" % (len(rev_bought),len(rev_bought)/len(rev_all)*100 if rev_all else 0,
        sum(1 for r in mom_all if r['actually_bought'])))

    log_print("\n  【反转池 排名 vs 后续收益】")
    log_print("  %-10s  %6s  %8s  %8s  %8s" % ("排名区间","只数","5日均收","10日均收","20日均收"))
    log_print("  "+"-"*50)
    for lo,hi,label in [(1,3,'排名1~3'),(4,6,'排名4~6'),(7,10,'排名7~10'),(11,999,'排名11+')]:
        grp=[r for r in rev_all if lo<=r['rank']<=hi]
        if not grp: continue
        log_print("  %-10s  %6d  %+7.2f%%  %+7.2f%%  %+7.2f%%" %
              (label,len(grp),safe_avg(grp,'fwd_5d_pct'),safe_avg(grp,'fwd_10d_pct'),safe_avg(grp,'fwd_20d_pct')))

    log_print("\n  【反转池 已买入 vs 未买入 5日收益】")
    rev_not_bought=[r for r in rev_all if not r['actually_bought']]
    avg5_b=safe_avg(rev_bought,'fwd_5d_pct'); avg5_nb=safe_avg(rev_not_bought,'fwd_5d_pct')
    log_print("  已买入(n=%d): 均5日 %+.2f%%" % (len(rev_bought),avg5_b))
    log_print("  未买入(n=%d): 均5日 %+.2f%%" % (len(rev_not_bought),avg5_nb))
    diff=avg5_b-avg5_nb
    log_print("  选股溢价: %+.2f%% (%s)" % (diff,"排序有效" if diff>0 else "警告：排序需检查"))

    log_print("\n  【未买入原因分布】")
    skip_cnt={}
    for r in ContextInfo.candidate_log:
        if not r['actually_bought']:
            reason=r['skip_reason'] or '其他'; skip_cnt[reason]=skip_cnt.get(reason,0)+1
    for reason,cnt in sorted(skip_cnt.items(),key=lambda x:-x[1]):
        log_print("  %-12s: %d笔" % (reason,cnt))

    log_print("\n  【反转池 评分分段 vs 5日收益】（阈值=%.0f）" % BUY_REVERSAL_SCORE)
    for lo,hi,label in [(60,80,'60~80'),(80,100,'80~100'),(100,120,'100~120'),(120,999,'120+')]:
        grp=[r for r in rev_all if lo<=r['total_score']<hi]
        if not grp: continue
        log_print("  %-8s: %d只 均5日 %+.2f%%" % (label,len(grp),safe_avg(grp,'fwd_5d_pct')))

    if mom_all:
        log_print("\n  【动量池 vol_accel 分布（诊断回踩条件）】")
        va_vals=[r['vol_accel'] for r in mom_all if r.get('vol_accel')]
        if va_vals:
            va_s=sorted(va_vals); n_va=len(va_vals)
            log_print("  样本:%d 均值:%.2f 中位:%.2f 最小:%.2f 最大:%.2f" %
                  (n_va,sum(va_vals)/n_va,va_s[n_va//2],va_s[0],va_s[-1]))
            for lo_v,hi_v,lb in [(1.2,1.5,'1.2~1.5'),(1.5,2.0,'1.5~2.0'),(2.0,3.0,'2.0~3.0'),(3.0,999,'3.0+')]:
                cnt=sum(1 for v in va_vals if lo_v<=v<hi_v)
                log_print("  %s: %d只(%.1f%%)" % (lb,cnt,cnt/n_va*100))
            log_print("  当前回踩阈值: vol_accel<%.1f（建议改为1.20）" % MOM_PULLBACK_VOL)
    log_print("="*80)


# ============================================================
# v13.1：独立CSV文件写入
# ============================================================

def write_factor_csv_file(ContextInfo):
    if not ContextInfo.factor_log: return
    pnl_map={}
    for t in ContextInfo.trade_records:
        pnl_map[t['stock']+'_'+t['buy_date']]={'pnl_pct':round(t['pnl_pct'],4),'win':1 if t['pnl']>0 else 0,'days_held':t['days_held'],'reason':t['reason']}
    keys=list(ContextInfo.factor_log[0].keys())+['pnl_pct','win','days_held','sell_reason']
    header=','.join(keys); rows=[]
    for rec in ContextInfo.factor_log:
        key=rec['stock']+'_'+rec['buy_date']; res=pnl_map.get(key,{})
        row=[str(rec.get(k,'')) for k in list(rec.keys())]
        row+=[str(res.get('pnl_pct','')),str(res.get('win','')),str(res.get('days_held','')),str(res.get('reason',''))]
        rows.append(','.join(row))
    write_csv_file(os.path.join(LOG_DIR,"factor_log.csv"),header,rows)

def write_candidate_csv_file(ContextInfo):
    if not ContextInfo.candidate_log: return
    CAND_FIELDS=['stock','name','candidate_date','price','pool','regime','rank',
        'total_score','rps','hard_cnt','rev_score','mom_score','fund_score','sw1',
        'mdi','pdi','rsi','mom60','mom20','mom5','drawdown','vol_accel',
        'boll_pos','hv60','atr_pct','near_lower','ma20_ok',
        'actually_bought','skip_reason','pnl_pct','win','days_held','sell_reason',
        'fwd_5d_pct','fwd_10d_pct','fwd_20d_pct']
    header=','.join(CAND_FIELDS); rows=[]
    for rec in ContextInfo.candidate_log:
        rows.append(','.join([str(rec.get(f,'')) for f in CAND_FIELDS]))
    write_csv_file(os.path.join(LOG_DIR,"candidate_log.csv"),header,rows)

def write_trade_csv_file(ContextInfo):
    if not ContextInfo.trade_records: return
    FIELDS=['stock','name','pool','buy_date','buy_price','buy_rps','buy_regime','sell_date','sell_price','shares','pnl','pnl_pct','days_held','reason']
    header=','.join(FIELDS); rows=[]
    for rec in ContextInfo.trade_records:
        rows.append(','.join([str(rec.get(f,'')) for f in FIELDS]))
    write_csv_file(os.path.join(LOG_DIR,"trade_records.csv"),header,rows)

def write_equity_csv(ContextInfo):
    if not ContextInfo.equity_curve: return
    header='date,equity,positions,regime'; rows=[]
    for rec in ContextInfo.equity_curve:
        rows.append('%s,%.2f,%d,%s' % (rec['date'],rec['equity'],rec['positions'],rec['regime']))
    write_csv_file(os.path.join(LOG_DIR,"equity_curve.csv"),header,rows)


def handlebar(ContextInfo):
    if not hasattr(ContextInfo,'backtest_done'):
        ContextInfo.backtest_done=True
        run_backtest(ContextInfo)
