# -*- coding: gbk -*-
"""
综合评分策略 - 回测版本 v10.2（阶段分化版）
兴业证券SMT-Q平台

【v10.1→v10.2：基于因子挖掘结论，按市场阶段分化止损和评分】

V10.1回测结果（最大回撤14.01%达标，但总收益损失32%）：
- 牛市：212笔，均收益+0.13%（几乎白忙），101笔动态止损均亏-8.22%
  → ATR×2.0对超跌股牛市波动太紧，正常回调被打出去后股票自己涨回来
- 熊市：122笔，均收益+3.78%，胜率45.1% → 质量最好的阶段
- 移动止盈从200笔降到141笔，少了59笔大涨机会

【v10.2两项核心改动，基于14万样本因子挖掘结论】

改动一：阶段分化止损（解决牛市止损太紧的问题）
  牛市：ATR × 2.5（放宽，容纳正常波动，因子挖掘显示牛市hv60/atr_pct是关键）
  震荡：ATR × 2.2
  熊市：ATR × 2.0（保持，深回撤+低RPS股波动本来就大）

改动二：阶段分化评分（三个阶段因子方向差异显著）

  【牛市因子】：hv60低★★★(-0.112)、atr_pct低★★★(-0.105)、hv20低★★★(-0.104)
    → 买低波动回调股，MACD金叉有效(+0.033)，near_lower反向(-0.008)
    → mdi权重降低，drawdown浅回撤(+0.047)比深回撤更好

  【震荡因子】：mom60低★★★(-0.120)、rps低★★★(-0.105)、mdi高★★★(+0.084)
    → 最优双因子：mom60低+mom5低 均收益+3.05%，胜率55.7%
    → rsi_oversold是震荡专属正向信号(+0.067)，但牛市方向相反！
    → vol_accel震荡专属正向(+0.040)，震荡放量好

  【熊市因子】：mom10低★★★(-0.109)、mom20低★★★(-0.109)、rsi低★★★(-0.104)
    → 最优双因子：mom10低+mom20低 均收益+3.30%，胜率54.7%
    → MACD金叉在熊市是负向因子(-0.045)！必须反向处理
    → near_lower熊市专属强信号(+0.080)，mom_all_neg熊市专属(+0.080)

【继承v10.1所有其他设定（三刀回撤控制保持不变）】
"""

import datetime
import math

# ============================================================
# 全局参数
# ============================================================

BACKTEST_START  = "20200201"
BACKTEST_END    = "20251231"
INITIAL_CAPITAL = 1000000
POSITION_SIZE   = 100000
MAX_POSITIONS   = 10   # 保留兼容，实际由 MAX_POSITIONS_BULL/BEAR 控制
MAX_INDUSTRY    = 3

COMMISSION_RATE = 0.00025
STAMP_TAX       = 0.001
SLIPPAGE        = 0.001

# ===== 买入条件 =====
BUY_REVERSAL_SCORE = 60
BUY_FUND_SCORE     = 5
BUY_ENV_COEF       = 0.75

# ===== RPS范围（v10：统一收窄至甜区4~15）=====
# 十分位分析：2~10分位（RPS 2~10）是全场收益最高区间
# 不再分阶段设不同上限，统一用15（留一点余量覆盖2~10段）
RPS_MIN = 4    # 排除极超跌（RPS<4可能是退市/停牌）
RPS_MAX = 15   # 收窄上限，聚焦甜区

# 熊市专属：深回撤硬性门槛（验证：低RPS+深回撤=+2.05%，胜率51%）
BEAR_DRAWDOWN_MIN = -0.15   # 熊市买入必须回撤>=15%

# ===== 出场条件 =====
STOP_LOSS        = -0.08   # 固定底线-8%
# 阶段分化ATR止损倍数（v10.2核心改动一）
# 牛市2.5：超跌股牛市波动大，2.0太紧导致101笔止损白打（均收益+0.13%）
# 熊市2.0：深回撤高波动股，保持收紧
ATR_STOP_MULT_BULL = 2.5   # 牛市
ATR_STOP_MULT_OSC  = 2.2   # 震荡
ATR_STOP_MULT_BEAR = 2.0   # 熊市
ATR_STOP_MULT      = 2.0   # 兜底默认值
ATR_TRAIL_MULT   = 1.5
TRAIL_ACTIVATE   = 0.07
MIN_HOLD_DAYS    = 3
MA20_PROFIT_MIN  = 0.08

# ===== 亏损超时（v10修复）=====
# 数据：持仓20天+胜率才过半，12天时只有15%
# 改为：亏损>3%才认错出场，且须持仓>20天
LOSS_EXIT_PNL    = -0.03   # 亏损超过3%才超时出场
LOSS_EXIT_DAYS   = 20      # 最短等待20天
FORCE_EXIT_DAYS  = 35      # 兜底：持仓超35天无论盈亏强制出场（防死持）

# 反转信号消失出场
SIGNAL_EXIT_SCORE = 20
SIGNAL_EXIT_DAYS  = 5

# ===== 仓位上限（刀二：熊市减半）=====
MAX_POSITIONS_BULL = 10   # 牛市/震荡：最多10只
MAX_POSITIONS_BEAR = 5    # 熊市：最多5只，减少坏市场资金暴露

# ===== 峰值回撤熔断（刀三：比绝对值更灵敏）=====
# 从历史最高权益回撤超过阈值时触发，恢复条件是回撤收窄到RESUME以内
DD_REDUCE_RATIO  = 0.08   # 峰值回撤>8%：降至半仓（MAX_POSITIONS_BEAR）
DD_STOP_RATIO    = 0.12   # 峰值回撤>12%：停止新买
DD_RESUME_RATIO  = 0.06   # 峰值回撤收窄到6%：完全恢复

# ===== 原绝对值熔断（保留作兜底）=====
EQUITY_STOP_RATIO   = 0.75
EQUITY_RESUME_RATIO = 0.78

# 市场阶段判断
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
# 初始化
# ============================================================

def init(ContextInfo):
    print("=" * 80)
    print("综合评分策略 v10.2（阶段分化版）")
    print("=" * 80)

    ContextInfo.capital   = INITIAL_CAPITAL
    ContextInfo.available = INITIAL_CAPITAL
    ContextInfo.positions = {}
    ContextInfo.trade_records  = []
    ContextInfo.equity_curve   = []
    ContextInfo.max_equity     = INITIAL_CAPITAL
    ContextInfo.max_drawdown   = 0
    ContextInfo.factor_log     = []
    ContextInfo.circuit_broken = False   # 绝对值熔断标记
    ContextInfo.dd_reduced     = False   # 峰值回撤>8%降仓标记
    ContextInfo.dd_stopped     = False   # 峰值回撤>12%停买标记
    ContextInfo.dd_stop_day    = 0       # 触发停买时的交易日序号
    ContextInfo.stats = {
        'total_trades': 0, 'win_trades': 0, 'loss_trades': 0,
        'exit_reasons': {
            '动态止损':0,'移动止盈':0,'止盈':0,
            '亏损超时':0,'强制出场':0,'信号消失':0,'反转完成':0
        },
        'regime_trades': {'牛市':0,'震荡':0,'熊市':0},
        'regime_wins':   {'牛市':0,'震荡':0,'熊市':0},
    }

    init_stock_pool(ContextInfo)
    init_sw1_mapping(ContextInfo)

    print("\n【回测参数 v10.2】")
    print("  策略类型: 超跌反转（RPS甜区4~15 + 阶段分化评分/止损）")
    print("  回测区间: %s ~ %s" % (BACKTEST_START, BACKTEST_END))
    print("  初始资金: %.0f万 | 每只: %.0f万" % (INITIAL_CAPITAL/10000, POSITION_SIZE/10000))
    print("  仓位上限: 牛市/震荡%d只 | 熊市%d只" % (MAX_POSITIONS_BULL, MAX_POSITIONS_BEAR))
    print("  RPS范围: %.0f~%.0f | 熊市附加: 回撤>=%.0f%%" % (RPS_MIN, RPS_MAX, abs(BEAR_DRAWDOWN_MIN)*100))
    print("  ATR止损: 牛市×%.1f | 震荡×%.1f | 熊市×%.1f（阶段分化）" %
          (ATR_STOP_MULT_BULL, ATR_STOP_MULT_OSC, ATR_STOP_MULT_BEAR))
    print("  评分: 按牛市/震荡/熊市因子IC权重分化")
    print("  峰值回撤熔断: >8%%降仓 | >12%%停买 | <6%%恢复（40天强制重置）")
    print("=" * 80)


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
        print("【股票池】沪深300:%d 中证500:%d 中证1000:%d 科创:%d 创业:%d 合计:%d" %
              (len(hs300),len(zz500),len(zz1000),len(kcb),len(cyb),len(all_stocks)))
    except Exception as e:
        print("获取股票池失败: %s" % str(e))
        ContextInfo.stock_pool = []


def init_sw1_mapping(ContextInfo):
    print("【构建SW1行业映射】")
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
    print("  覆盖: %d / %d (%.1f%%)" % (covered, total, covered/total*100 if total else 0))


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
# 基础数学工具
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
# 预计算技术指标
# ============================================================

def precompute_all_indicators(all_market_data, trade_dates):
    print("\n预计算技术指标...")
    indicators = {}
    total = len(all_market_data); count = 0

    for stock, df in all_market_data.items():
        count += 1
        if count % 500 == 0:
            print("  已计算 %d / %d ..." % (count, total))

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

    print("  预计算完成,共 %d 只" % len(indicators))
    return indicators


# ============================================================
# 反转评分函数（与v9相同，不动）
# ============================================================

def calc_reversal_score(ind, regime):
    """
    v10.2阶段分化评分
    基于14万样本因子挖掘结论，三个阶段权重完全不同

    牛市核心：hv60低(-0.112)、atr_pct低(-0.105)、hv20低(-0.104)、mom60低(-0.093)
              → 低波动回调股，MACD金叉有效(+0.033)，near_lower牛市反向(-0.008)
    震荡核心：mom60低(-0.120)、rps低(-0.105)、mdi高(+0.084)、mom5低(-0.071)
              → 双动量负+MDI，rsi_oversold震荡专属(+0.067，牛市方向相反！)
              → vol_accel震荡专属正向(+0.040)
    熊市核心：mom10低(-0.109)、mom20低(-0.109)、rsi低(-0.104)、boll_pos低(-0.103)
              → MACD金叉熊市负向(-0.045)！必须反向，near_lower熊市强信号(+0.080)
    """
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
        # ======================================================
        # 牛市：核心是低波动回调，MACD金叉有效，near_lower无效
        # ======================================================

        # A. 波动率（牛市最强因子组：hv60★★★ + hv20★★★ + atr_pct★★★）
        if hv60 > 0:
            if hv60 < 0.20:    score += 18; signals.append("[牛]hv60极低+18")
            elif hv60 < 0.30:  score += 12; signals.append("[牛]hv60低+12")
            elif hv60 < 0.45:  score +=  6; signals.append("[牛]hv60中+6")
            elif hv60 > 0.70:  score -= 10; signals.append("[牛]hv60高-10")
        if hv20 > 0:
            if hv20 < 0.20:    score += 12; signals.append("[牛]hv20极低+12")
            elif hv20 < 0.35:  score +=  7; signals.append("[牛]hv20低+7")
            elif hv20 > 0.70:  score -=  8; signals.append("[牛]hv20高-8")

        # B. 动量（mom60负向有效，但权重低于震荡/熊市）
        if mom60 < -0.20:   score += 12; signals.append("[牛]mom60<-20%+12")
        elif mom60 < -0.10: score +=  7; signals.append("[牛]mom60<-10%+7")
        elif mom60 < 0:     score +=  3; signals.append("[牛]mom60负+3")
        elif mom60 > 0.20:  score -=  8; signals.append("[牛]mom60>20%-8")

        # C. MDI（牛市中等强度，IC=+0.040）
        if mdi > 30 and mdi > pdi:  score += 14; signals.append("[牛]MDI>PDI强+14")
        elif mdi > 20 and mdi > pdi: score +=  8; signals.append("[牛]MDI>PDI+8")
        elif mdi > pdi:              score +=  4; signals.append("[牛]MDI>PDI弱+4")
        else:                        score -=  6; signals.append("[牛]PDI>MDI-6")

        # D. MACD（牛市正向！dif_gt_dea IC=+0.033）
        if dif < 0 and dea < 0:
            if dif > dea and pdif <= pdea: score += 14; signals.append("[牛]MACD负区金叉+14")
            elif dif > dea:                score +=  6; signals.append("[牛]MACD负区dif>dea+6")
        elif dif > 0 and dea > 0:         score -=  6; signals.append("[牛]MACD正区-6")

        # E. 回撤（牛市drawdown正向+0.047，浅回撤比深回撤好）
        if -0.15 < dd < -0.05:  score += 10; signals.append("[牛]适度回撤+10")
        elif -0.05 <= dd < 0:   score +=  5; signals.append("[牛]浅回撤+5")
        elif dd < -0.25:        score -=  5; signals.append("[牛]过深回撤-5")

        # F. RSI（牛市IC=-0.031，弱反向）
        if rsi < 30:    score += 10; signals.append("[牛]RSI极低+10")
        elif rsi < 40:  score +=  6; signals.append("[牛]RSI低+6")
        elif rsi < 50:  score +=  3; signals.append("[牛]RSI偏低+3")
        elif rsi > 70:  score -= 10; signals.append("[牛]RSI超买-10")
        elif rsi > 60:  score -=  5; signals.append("[牛]RSI偏高-5")
        if prev_rsi and rsi > prev_rsi and rsi < 45:
            score += 4; signals.append("[牛]RSI回升+4")

        # G. 量能（牛市vol_accel弱正向+0.024）
        if 1.0 <= va <= 2.5:  score +=  6; signals.append("[牛]放量+6")
        elif va < 0.3:        score -= 10; signals.append("[牛]极缩量-10")

        # H. near_lower（牛市IC=-0.008，几乎无效，不加不减）
        # 牛市布林上轨才是不好的信号
        if bpos > 0.80: score -= 8; signals.append("[牛]近上轨-8")

        # I. CMF（牛市IC=-0.043，负CMF反而好）
        if cmf < -0.10:  score +=  5; signals.append("[牛]CMF负+5")
        elif cmf > 0.15: score -=  6; signals.append("[牛]CMF高-6")

    elif regime == '震荡':
        # ======================================================
        # 震荡：核心是双动量负+MDI，rsi_oversold是专属信号
        # ======================================================

        # A. 动量（震荡最强双因子：mom60低+mom5低，均收益+3.05%）
        if mom60 < -0.20:   score += 18; signals.append("[震]mom60<-20%+18")
        elif mom60 < -0.10: score += 12; signals.append("[震]mom60<-10%+12")
        elif mom60 < 0:     score +=  5; signals.append("[震]mom60负+5")
        elif mom60 > 0.20:  score -= 10; signals.append("[震]mom60>20%-10")

        if mom5 < -0.05:    score += 14; signals.append("[震]mom5负<-5%+14")
        elif mom5 < 0:      score +=  8; signals.append("[震]mom5负+8")
        elif mom5 > 0.05:   score -=  8; signals.append("[震]mom5正-8")

        # B. MDI（震荡最强单因子之一，IC=+0.084）
        if mdi > 30 and mdi > pdi:  score += 20; signals.append("[震]MDI>PDI强+20")
        elif mdi > 20 and mdi > pdi: score += 13; signals.append("[震]MDI>PDI+13")
        elif mdi > pdi:              score +=  6; signals.append("[震]MDI>PDI弱+6")
        else:                        score -=  8; signals.append("[震]PDI>MDI-8")

        # C. RSI超卖（震荡专属！IC=+0.067，牛市方向相反）
        if rsi < 25:    score += 18; signals.append("[震]RSI极超卖+18")
        elif rsi < 35:  score += 12; signals.append("[震]RSI超卖+12")
        elif rsi < 45:  score +=  6; signals.append("[震]RSI偏低+6")
        elif rsi > 70:  score -= 12; signals.append("[震]RSI超买-12")
        elif rsi > 60:  score -=  6; signals.append("[震]RSI偏高-6")
        if prev_rsi and rsi > prev_rsi and rsi < 45:
            score += 5; signals.append("[震]RSI回升+5")

        # D. 布林下轨（震荡IC=+0.052，有效）
        if bpos < 0.10:   score += 14; signals.append("[震]极近下轨+14")
        elif bpos < 0.20: score +=  9; signals.append("[震]近下轨+9")
        elif bpos < 0.30: score +=  4; signals.append("[震]偏近下轨+4")
        elif bpos > 0.80: score -= 10; signals.append("[震]近上轨-10")

        # E. hv60（震荡IC=-0.092，有效）
        if hv60 > 0:
            if hv60 < 0.25:   score += 10; signals.append("[震]hv60低+10")
            elif hv60 < 0.40: score +=  5; signals.append("[震]hv60中+5")
            elif hv60 > 0.80: score -=  8; signals.append("[震]hv60高-8")

        # F. 量能（震荡专属正向！vol_accel IC=+0.040，震荡放量是好信号）
        if 1.2 <= va <= 2.5:  score += 12; signals.append("[震]放量+12")
        elif 1.0 <= va < 1.2: score +=  6; signals.append("[震]温和放量+6")
        elif va < 0.3:        score -= 10; signals.append("[震]极缩量-10")

        # G. 回撤
        if dd < -0.20:    score += 10; signals.append("[震]极深回撤+10")
        elif dd < -0.10:  score +=  7; signals.append("[震]深回撤+7")
        elif dd < -0.05:  score +=  3; signals.append("[震]中回撤+3")

        # H. MACD（震荡IC=+0.021，弱正向，不强调）
        if dif < 0 and dea < 0:
            if dif > dea and pdif <= pdea: score += 8; signals.append("[震]MACD负区金叉+8")
            elif dif > dea:               score += 3; signals.append("[震]MACD负区dif>dea+3")

        # I. CMF
        if cmf < -0.10:  score +=  4; signals.append("[震]CMF负+4")
        elif cmf > 0.15: score -=  5; signals.append("[震]CMF高-5")

    else:  # 熊市
        # ======================================================
        # 熊市：核心是中短期动量全负+boll_pos低+rsi低
        # MACD金叉是负向信号！near_lower是强正向信号
        # ======================================================

        # A. 中短期动量（熊市最强双因子：mom10低+mom20低，均收益+3.30%）
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

        # B. RSI（熊市IC=-0.104，★★★强）
        if rsi < 25:    score += 18; signals.append("[熊]RSI极超卖+18")
        elif rsi < 35:  score += 12; signals.append("[熊]RSI超卖+12")
        elif rsi < 45:  score +=  6; signals.append("[熊]RSI偏低+6")
        elif rsi > 70:  score -= 14; signals.append("[熊]RSI超买-14")
        elif rsi > 60:  score -=  7; signals.append("[熊]RSI偏高-7")
        if prev_rsi and rsi > prev_rsi and rsi < 45:
            score += 5; signals.append("[熊]RSI回升+5")

        # C. boll_pos低（熊市IC=-0.103，★★★，是熊市专属强信号）
        if bpos < 0.10:   score += 16; signals.append("[熊]极近下轨+16")
        elif bpos < 0.20: score += 11; signals.append("[熊]近下轨+11")
        elif bpos < 0.30: score +=  5; signals.append("[熊]偏近下轨+5")
        elif bpos > 0.80: score -= 12; signals.append("[熊]近上轨-12")

        # D. near_lower（熊市专属正向！IC=+0.080）
        if ind.get('near_lower', 0):
            score += 12; signals.append("[熊]near_lower+12")

        # E. MDI（熊市IC=+0.096，★★★，最强正向因子）
        if mdi > 30 and mdi > pdi:  score += 22; signals.append("[熊]MDI>PDI强+22")
        elif mdi > 20 and mdi > pdi: score += 14; signals.append("[熊]MDI>PDI+14")
        elif mdi > pdi:              score +=  7; signals.append("[熊]MDI>PDI弱+7")
        else:                        score -= 10; signals.append("[熊]PDI>MDI-10")

        # F. 回撤深度（熊市dd_deep IC=+0.072，专属正向）
        if dd < -0.25:    score += 16; signals.append("[熊]极深回撤+16")
        elif dd < -0.15:  score += 10; signals.append("[熊]深回撤+10")
        elif dd < -0.10:  score +=  5; signals.append("[熊]中回撤+5")

        # G. MACD（熊市负向！dif_gt_dea IC=-0.045，金叉在熊市反向！）
        if dif < 0 and dea < 0:
            if dif > dea and pdif <= pdea:
                score -= 8;  signals.append("[熊]MACD金叉反向-8（熊市不追金叉）")
            elif dif > dea:
                score -= 3;  signals.append("[熊]MACD dif>dea反向-3")
            else:
                score += 6;  signals.append("[熊]MACD死叉区域+6")
        elif dif > 0 and dea > 0:
            score -= 10; signals.append("[熊]MACD正区-10")

        # H. hv60（熊市IC=-0.032，弱反向）
        if hv60 > 0:
            if hv60 < 0.25:  score +=  5; signals.append("[熊]hv60低+5")
            elif hv60 > 0.80: score -=  6; signals.append("[熊]hv60高-6")

        # I. CMF（熊市IC=-0.078，负CMF好）
        if cmf < -0.15:  score +=  8; signals.append("[熊]CMF极负+8")
        elif cmf < -0.05: score += 4; signals.append("[熊]CMF负+4")
        elif cmf > 0.15: score -=  8; signals.append("[熊]CMF高-8")

        # J. 量能（熊市vol_accel弱负向，缩量不好）
        if va < 0.3:     score -= 10; signals.append("[熊]极缩量-10")
        elif 1.0 <= va <= 2.0: score += 5; signals.append("[熊]温和放量+5")

    # ======================================================
    # 全阶段通用：K线形态（各阶段IC差异不大）
    # ======================================================
    if ind.get('hammer', 0): score += 7; signals.append("锤子线+7")
    if ind.get('engulf', 0): score += 5; signals.append("吞没+5")

    return score, signals


def calc_fund_score_cached(ContextInfo, stock_code, fund_cache):
    if stock_code in fund_cache:
        return fund_cache[stock_code]
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
            profit_yoy = safe_get('du_profit_rate')
            roe        = safe_get('du_return_on_equity')
            cf         = safe_get('s_fa_ocfps')
            if valid(profit_yoy):
                got_data = True
                if profit_yoy>0: score+=5; signals.append("盈利正增长+5")
                else:            score-=3; signals.append("盈利负增长-3")
            if valid(roe):
                got_data = True
                if roe>10:   score+=5; signals.append("ROE>10%+5")
                elif roe>0:  score+=2
            if valid(cf):
                got_data = True
                if cf>0: score+=5; signals.append("现金流正+5")
    except: pass
    if not got_data: score=5; signals=["默认5"]
    fund_cache[stock_code]=(score,signals)
    return score, signals


# ============================================================
# 市场环境预计算
# ============================================================

def precompute_index_env(ContextInfo):
    print("\n预计算指数环境...")
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
    print("  完成,共 %d 个指数" % len(env_data))
    return env_data


def precompute_industry_index(ContextInfo):
    print("\n预计算行业指数乖离率...")
    index_codes=list(set(SW1_INDEX_MAP.values()))
    try:
        bt=datetime.datetime.strptime(BACKTEST_START,"%Y%m%d")
        ds=(bt-datetime.timedelta(days=60)).strftime("%Y%m%d")
    except: ds=BACKTEST_START
    try:
        raw=ContextInfo.get_market_data_ex(['close'],index_codes,period='1d',
                                            start_time=ds,end_time=BACKTEST_END)
    except Exception as e:
        print("  获取失败: %s"%str(e)); return {}
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
    print("  完成: %d个" % len(ind_env))
    return ind_env


def get_env_coef(ContextInfo, stock_code, date, env_data):
    if   stock_code in ContextInfo.hs300_set:  idx='000300.SH'
    elif stock_code in ContextInfo.zz500_set:   idx='000905.SH'
    elif stock_code in ContextInfo.zz1000_set:  idx='000852.SH'
    elif stock_code in ContextInfo.kcb_set:     idx='000688.SH'
    elif stock_code in ContextInfo.cyb_set:     idx='399006.SZ'
    else:                                        return 1.0, "默认"
    if idx in env_data and date in env_data[idx]:
        return env_data[idx][date]
    return 1.0, "缺失"


def check_industry_index(ContextInfo, stock_code, date, ind_env):
    sw1=ContextInfo.stock_sw1.get(stock_code,None)
    if not sw1: return True, 0, "无行业"
    idx=SW1_INDEX_MAP.get(sw1,None)
    if not idx: return True, 0, "无指数"
    if idx not in ind_env or date not in ind_env[idx]: return True, 0, "无数据"
    env=ind_env[idx][date]
    if not env['allow']:
        return False, env['bias']*100, "%s过热" % sw1
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
# 交易执行
# ============================================================

def execute_buy(ContextInfo, stock, price, date, score_info, industry, ind, regime):
    shares=int(POSITION_SIZE/price/100)*100
    if shares<100: return False
    cost=price*(1+COMMISSION_RATE+SLIPPAGE)
    if shares*cost>ContextInfo.available: return False

    atr14=ind.get('atr14',price*0.02)
    if atr14<=0: atr14=price*0.02

    # 阶段分化ATR止损倍数（v10.2核心改动一）
    if regime == '牛市':    atr_mult = ATR_STOP_MULT_BULL
    elif regime == '震荡':  atr_mult = ATR_STOP_MULT_OSC
    else:                   atr_mult = ATR_STOP_MULT_BEAR

    atr_stop  = price - atr_mult * atr14
    fixed_stop= price*(1+STOP_LOSS)
    dyn_stop  = max(atr_stop, fixed_stop)

    ContextInfo.available -= shares*cost
    ContextInfo.positions[stock]={
        'cost':cost, 'shares':shares,
        'buy_date':date, 'buy_price':price,
        'buy_score':score_info['total'],
        'buy_rps':score_info.get('rps',0),
        'buy_regime':regime,
        'days_held':0, 'industry':industry,
        'atr14':atr14,
        'atr_mult':atr_mult,
        'dynamic_stop':dyn_stop,
        'trail_high':price,
    }
    return True


def execute_sell(ContextInfo, stock, price, date, reason, name):
    if stock not in ContextInfo.positions: return False
    pos=ContextInfo.positions[stock]
    sell_price=price*(1-COMMISSION_RATE-STAMP_TAX-SLIPPAGE)
    proceeds=pos['shares']*sell_price
    pnl=proceeds-pos['shares']*pos['cost']
    pnl_pct=(sell_price/pos['cost']-1)*100
    regime=pos.get('buy_regime','震荡')

    ContextInfo.trade_records.append({
        'stock':stock,'name':name,
        'buy_date':pos['buy_date'],'buy_price':pos['buy_price'],
        'sell_date':date,'sell_price':price,
        'shares':pos['shares'],'pnl':pnl,'pnl_pct':pnl_pct,
        'days_held':pos['days_held'],'reason':reason,
        'buy_rps':pos.get('buy_rps',0),'buy_regime':regime,
    })
    ContextInfo.stats['total_trades'] += 1
    if pnl>0:
        ContextInfo.stats['win_trades'] += 1
        ContextInfo.stats['regime_wins'][regime]=ContextInfo.stats['regime_wins'].get(regime,0)+1
    else:
        ContextInfo.stats['loss_trades'] += 1
    ContextInfo.stats['exit_reasons'][reason]=ContextInfo.stats['exit_reasons'].get(reason,0)+1
    ContextInfo.stats['regime_trades'][regime]=ContextInfo.stats['regime_trades'].get(regime,0)+1

    ContextInfo.available += proceeds
    del ContextInfo.positions[stock]
    return True


# ============================================================
# 因子记录
# ============================================================

def record_factor_log(ContextInfo, stock_code, name, date, price,
                      ind, reversal_score, fund_score, rps, regime, sw1):
    ContextInfo.factor_log.append({
        'stock':stock_code, 'name':name[:6], 'buy_date':date,
        'buy_price':price, 'regime':regime, 'sw1':sw1,
        'reversal_score':round(reversal_score,1),
        'fund_score':fund_score, 'rps':round(rps,1),
        'mdi':round(ind.get('mdi',0),2),
        'pdi':round(ind.get('pdi',0),2),
        'mdi_gt_pdi':ind.get('mdi_gt_pdi',0),
        'rsi':round(ind.get('rsi',50) or 50,2),
        'mom60':round(ind.get('mom60',0),4),
        'mom20':round(ind.get('mom20',0),4),
        'mom_all_neg':ind.get('mom_all_neg',0),
        'mom_neg_cnt':ind.get('mom_neg_cnt',0),
        'hv20':round(ind.get('hv20',0),4),
        'hv60':round(ind.get('hv60',0),4),
        'drawdown':round(ind.get('drawdown',0),4),
        'boll_pos':round(ind.get('boll_pos',0.5),4),
        'near_lower':ind.get('near_lower',0),
        'vol_accel':round(ind.get('vol_accel',1),4),
        'cmf':round(ind.get('cmf',0),4),
        'atr_pct':round(ind.get('atr_pct',0.02),4),
        'dif':round(ind.get('dif',0),4),
        'dea':round(ind.get('dea',0),4),
        'dd_deep':ind.get('dd_deep',0),
        'hammer':ind.get('hammer',0),
        'engulf':ind.get('engulf',0),
    })


# ============================================================
# 回测主循环
# ============================================================

def run_backtest(ContextInfo):
    print("\n" + "="*80)
    print("开始回测 v10（RPS甜区精修）...")
    print("="*80)

    # 1. 交易日
    try:
        trade_dates=ContextInfo.get_trading_dates("SH",BACKTEST_START,BACKTEST_END,2000)
        trade_dates=[str(d) for d in trade_dates]
    except Exception as e:
        print("获取交易日失败: %s"%str(e)); return
    print("交易日: %d 天 (%s ~ %s)" % (len(trade_dates),trade_dates[0],trade_dates[-1]))

    # 2. 行情数据
    print("\n获取行情数据(分批)...")
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
        print("  第 %d/%d 批 (%d只)..." % (b+1,total_batches,len(batch)))
        try:
            bd=ContextInfo.get_market_data_ex(
                ['open','high','low','close','volume'],
                batch,period='1d',start_time=ds,end_time=BACKTEST_END)
            all_market_data.update(bd)
        except Exception as ex:
            print("  失败: %s"%str(ex))
    print("  完成,共 %d 只" % len(all_market_data))

    # 3. 预计算
    indicators=precompute_all_indicators(all_market_data, trade_dates)
    env_data  =precompute_index_env(ContextInfo)
    ind_env   =precompute_industry_index(ContextInfo)

    # 4. RPS（120日）
    print("\n计算逐日RPS(120日)...")
    rps_data={}
    stock_close_by_date={}
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
        if (di+1)%20==0:
            print("  RPS已计算 %d/%d ..." % (di+1,len(trade_dates)))
    print("  RPS完成, %d 天" % len(rps_data))

    # 5. 辅助数据
    st_set=set(); name_map={}; ind_map={}; sw1_map={}
    for stock in ContextInfo.stock_pool:
        try:
            nm=ContextInfo.get_stock_name(stock)
            name_map[stock]=nm
            if 'ST' in nm: st_set.add(stock)
            sw1_map[stock]=get_industry_sw1(ContextInfo,stock,nm)
            ind_map[stock]=get_industry(nm)
            if ind_map[stock]=='其他':
                if stock.startswith('688'):   ind_map[stock]='科创板'
                elif stock.startswith('300'): ind_map[stock]='创业板'
        except:
            name_map[stock]=stock[:6]; ind_map[stock]='其他'; sw1_map[stock]='其他'
    print("  ST股票: %d 只" % len(st_set))

    fund_cache={}

    # 6. 逐日回测
    print("\n开始逐日回测...")
    buy_count=0; sell_count=0

    for i, date in enumerate(trade_dates):
        day_buy=0; day_sell=0

        regime, normal_cnt, bear_cnt = get_market_regime(date, env_data)

        # 权益计算
        current_equity=ContextInfo.available
        for sc,pos in ContextInfo.positions.items():
            if sc in indicators and date in indicators[sc]:
                current_equity+=pos['shares']*indicators[sc][date]['close']

        # ===== 卖出检查 =====
        for stock_code in list(ContextInfo.positions.keys()):
            pos=ContextInfo.positions[stock_code]
            pos['days_held']+=1
            if stock_code not in indicators or date not in indicators[stock_code]: continue

            ind=indicators[stock_code][date]
            cur=ind['close']; cost=pos['cost']
            pnl_pct=cur/cost-1
            ma20=ind.get('ma20')

            if cur>pos.get('trail_high',0): pos['trail_high']=cur

            sell_reason=None

            # 1. ATR动态止损
            if cur<=pos.get('dynamic_stop',cost*(1+STOP_LOSS)):
                sell_reason='动态止损'

            # 2. 移动止盈（浮盈>7%激活）
            elif pnl_pct>TRAIL_ACTIVATE:
                trail_stop=pos['trail_high']-ATR_TRAIL_MULT*pos.get('atr14',cur*0.02)
                if cur<trail_stop:
                    sell_reason='移动止盈'

            elif pos['days_held']>=MIN_HOLD_DAYS:
                # 3. 兜底强制出场：持仓>35天无论盈亏
                if pos['days_held'] > FORCE_EXIT_DAYS:
                    sell_reason='强制出场'

                # 4. 止盈：盈利>=8%且跌破MA20
                elif pnl_pct>=MA20_PROFIT_MIN and ma20 and cur<ma20:
                    sell_reason='止盈'

                # 5. 亏损超时：亏损>3%且持仓>20天（数据支撑：20天+胜率才过半）
                elif pnl_pct < LOSS_EXIT_PNL and pos['days_held'] > LOSS_EXIT_DAYS:
                    sell_reason='亏损超时'

                # 6. 反转信号消失
                if sell_reason is None and pos['days_held']>=SIGNAL_EXIT_DAYS:
                    rev_s,_=calc_reversal_score(ind, regime)
                    fund_s,_=calc_fund_score_cached(ContextInfo,stock_code,fund_cache)
                    env_c,_ =get_env_coef(ContextInfo,stock_code,date,env_data)
                    total_s =(rev_s+fund_s)*env_c
                    if total_s<SIGNAL_EXIT_SCORE and pnl_pct<=0:
                        sell_reason='信号消失'
                    elif ind.get('rsi',50) and ind['rsi']>60 and pnl_pct>0.05:
                        sell_reason='反转完成'

            if sell_reason:
                nm=name_map.get(stock_code,'')
                print("  %s | 卖出 | %-8s | %.2f | %s(%.1f%%) [持%d天][%s]" %
                      (date,nm[:8],cur,sell_reason,pnl_pct*100,
                       pos['days_held'],pos.get('buy_regime','?')))
                execute_sell(ContextInfo,stock_code,cur,date,sell_reason,nm)
                sell_count+=1; day_sell+=1

        # ===== 买入检查 =====
        # 刀二：根据市场阶段动态设置仓位上限
        max_pos = MAX_POSITIONS_BEAR if regime == '熊市' else MAX_POSITIONS_BULL

        # 刀三：峰值回撤熔断（比绝对值更灵敏）
        peak_dd = (ContextInfo.max_equity - current_equity) / ContextInfo.max_equity if ContextInfo.max_equity > 0 else 0

        if not ContextInfo.dd_stopped and peak_dd >= DD_STOP_RATIO:
            ContextInfo.dd_stopped = True
            ContextInfo.dd_reduced = True
            ContextInfo.dd_stop_day = i   # 记录触发时的交易日序号
            print("  [峰值回撤停买 %s] 从峰值回撤%.1f%%>=%.0f%%，停止新买" %
                  (date, peak_dd*100, DD_STOP_RATIO*100))
        elif not ContextInfo.dd_reduced and peak_dd >= DD_REDUCE_RATIO:
            ContextInfo.dd_reduced = True
            ContextInfo.dd_stop_day = i
            print("  [峰值回撤降仓 %s] 从峰值回撤%.1f%%>=%.0f%%，仓位上限降至%d只" %
                  (date, peak_dd*100, DD_REDUCE_RATIO*100, MAX_POSITIONS_BEAR))
        elif (ContextInfo.dd_reduced or ContextInfo.dd_stopped):
            # 恢复条件：回撤收窄到6%，或停买超过40个交易日（防死锁）
            days_stopped = i - ContextInfo.dd_stop_day
            force_resume = (ContextInfo.dd_stopped and days_stopped >= 40)
            if peak_dd <= DD_RESUME_RATIO or force_resume:
                ContextInfo.dd_reduced = False
                ContextInfo.dd_stopped = False
                if force_resume:
                    # 重置峰值，以当前权益为新基准，重新出发
                    ContextInfo.max_equity = current_equity
                    print("  [强制恢复 %s] 停买已%d天，重置峰值基准%.2f万，恢复买入" %
                          (date, days_stopped, current_equity/10000))
                else:
                    print("  [峰值回撤恢复 %s] 回撤收窄至%.1f%%，恢复正常" %
                          (date, peak_dd*100))

        # 峰值回撤降仓时，仓位上限进一步收紧
        if ContextInfo.dd_reduced:
            max_pos = min(max_pos, MAX_POSITIONS_BEAR)

        if len(ContextInfo.positions) < max_pos:

            equity_ratio = current_equity / INITIAL_CAPITAL

            # 绝对值熔断（兜底）
            if not ContextInfo.circuit_broken and equity_ratio < EQUITY_STOP_RATIO:
                ContextInfo.circuit_broken = True
                print("  [绝对值熔断 %s] 权益%.1f%%，停止买入" %
                      (date, equity_ratio*100))
            elif ContextInfo.circuit_broken and equity_ratio >= EQUITY_RESUME_RATIO:
                ContextInfo.circuit_broken = False
                print("  [绝对值熔断解除 %s] 权益%.1f%%，恢复买入" % (date, equity_ratio*100))

            if ContextInfo.circuit_broken or ContextInfo.dd_stopped:
                if i % 20 == 0:
                    print("  [停买中 %s] 峰值回撤%.1f%% 权益%.1f%%" %
                          (date, peak_dd*100, equity_ratio*100))
            else:
                ind_count={}
                for st,po in ContextInfo.positions.items():
                    inm=po.get('industry','其他')
                    ind_count[inm]=ind_count.get(inm,0)+1

                buy_candidates=[]
                dbg={'no_data':0,'rps_fail':0,'hard_fail':0,
                     'bear_dd_fail':0,'score_fail':0,'fund_fail':0,'ind_fail':0,'pass':0}
                debug_day=(i==0 or (i+1)%20==0)

                for stock_code in ContextInfo.stock_pool:
                    if stock_code in ContextInfo.positions or stock_code in st_set:
                        continue
                    if stock_code not in indicators or date not in indicators[stock_code]:
                        dbg['no_data']+=1; continue

                    ind=indicators[stock_code][date]

                    # ===== RPS过滤（v10：统一4~15甜区）=====
                    stock_rps=rps_data.get(date,{}).get(stock_code,50)
                    if stock_rps < RPS_MIN or stock_rps > RPS_MAX:
                        dbg['rps_fail']+=1; continue

                    # ===== 硬性反转条件（至少2个）=====
                    hard_cnt=0
                    rsi_v=ind.get('rsi',50) or 50
                    if rsi_v<45:                   hard_cnt+=1
                    if ind.get('mdi_gt_pdi',0):    hard_cnt+=1
                    if ind.get('mom_all_neg',0):   hard_cnt+=1
                    if ind.get('near_lower',0):    hard_cnt+=1
                    if ind.get('drawdown',0)<-0.10: hard_cnt+=1
                    if ind.get('mom60',0)<-0.10:   hard_cnt+=1

                    if hard_cnt<2:
                        dbg['hard_fail']+=1; continue

                    # 排除极缩量
                    if ind.get('vol_accel',1)<0.3:
                        dbg['hard_fail']+=1; continue

                    # ===== 熊市专属：深回撤硬性门槛（v10新增）=====
                    # 数据依据：低RPS+深回撤>15% → 均收益+2.05%，胜率51%
                    if regime == '熊市':
                        if ind.get('drawdown', 0) > BEAR_DRAWDOWN_MIN:
                            dbg['bear_dd_fail']+=1; continue

                    # ===== 反转评分 =====
                    rev_score,rev_sigs=calc_reversal_score(ind, regime)
                    fund_score,_=calc_fund_score_cached(ContextInfo,stock_code,fund_cache)
                    env_c,_=get_env_coef(ContextInfo,stock_code,date,env_data)

                    env_min=BUY_ENV_COEF if regime!='熊市' else 0.5
                    if env_c<env_min:
                        dbg['score_fail']+=1; continue

                    total_score=(rev_score+fund_score)*env_c

                    if total_score<BUY_REVERSAL_SCORE:
                        dbg['score_fail']+=1; continue

                    if fund_score<BUY_FUND_SCORE:
                        dbg['fund_fail']+=1; continue

                    # 行业分散
                    industry=ind_map.get(stock_code,'其他')
                    if ind_count.get(industry,0)>=MAX_INDUSTRY:
                        dbg['ind_fail']+=1; continue

                    # 行业过热过滤
                    ind_allow,_,_=check_industry_index(ContextInfo,stock_code,date,ind_env)
                    if not ind_allow:
                        dbg['ind_fail']+=1; continue

                    dbg['pass']+=1
                    buy_candidates.append({
                        'stock':stock_code,
                        'price':ind['close'],
                        'total_score':total_score,
                        'rev_score':rev_score,
                        'fund_score':fund_score,
                        'rps':stock_rps,
                        'industry':industry,
                        'hard_cnt':hard_cnt,
                        'rev_signals':rev_sigs,
                        'ind':ind,
                        'sw1':sw1_map.get(stock_code,'其他'),
                    })

                if debug_day:
                    print("  [调试 %s][%s 正常%d/防守%d] RPS[%.0f~%.0f]" %
                          (date,regime,normal_cnt,bear_cnt,RPS_MIN,RPS_MAX))
                    print("    无数据:%d RPS:%d 硬:%d 熊市回撤:%d 评分:%d 基本面:%d 行业:%d 通过:%d" %
                          (dbg['no_data'],dbg['rps_fail'],dbg['hard_fail'],
                           dbg['bear_dd_fail'],dbg['score_fail'],
                           dbg['fund_fail'],dbg['ind_fail'],dbg['pass']))

                if buy_candidates:
                    print("  [筛选 %s][%s] RPS[%.0f~%.0f] → 候选:%d (仓位上限:%d)" %
                          (date,regime,RPS_MIN,RPS_MAX,len(buy_candidates),max_pos))

                buy_candidates.sort(
                    key=lambda x:(x['total_score']+x['hard_cnt']*5),
                    reverse=True)

                for cand in buy_candidates:
                    if len(ContextInfo.positions)>=max_pos: break
                    cur_ind_count=sum(1 for p in ContextInfo.positions.values()
                                      if p.get('industry')==cand['industry'])
                    if cur_ind_count>=MAX_INDUSTRY: continue

                    score_info={'total':cand['total_score'],'rps':cand['rps']}
                    if execute_buy(ContextInfo,cand['stock'],cand['price'],date,
                                   score_info,cand['industry'],cand['ind'],regime):
                        buy_count+=1; day_buy+=1
                        ind_count[cand['industry']]=ind_count.get(cand['industry'],0)+1
                        nm=name_map.get(cand['stock'],'')
                        pos=ContextInfo.positions[cand['stock']]
                        print("  %s | 买入[%s] | %-8s | %.2f | 分%.0f RPS:%.1f 硬:%d 回撤%.1f%% 止损%.2f" %
                              (date,regime,nm[:8],cand['price'],
                               cand['total_score'],cand['rps'],cand['hard_cnt'],
                               cand['ind'].get('drawdown',0)*100,pos['dynamic_stop']))

                        record_factor_log(
                            ContextInfo,cand['stock'],nm,date,cand['price'],
                            cand['ind'],cand['rev_score'],cand['fund_score'],
                            cand['rps'],regime,cand['sw1'])

        # 净值更新
        equity=ContextInfo.available
        for stock,pos in ContextInfo.positions.items():
            if stock in indicators and date in indicators[stock]:
                equity+=pos['shares']*indicators[stock][date]['close']
        ContextInfo.equity_curve.append({'date':date,'equity':equity,
                                          'positions':len(ContextInfo.positions),
                                          'regime':regime})
        if equity>ContextInfo.max_equity: ContextInfo.max_equity=equity
        dd=(ContextInfo.max_equity-equity)/ContextInfo.max_equity
        if dd>ContextInfo.max_drawdown: ContextInfo.max_drawdown=dd

        if day_buy>0 or day_sell>0:
            print("  %s ---- [%s] 持仓:%d 权益:%.2f万 买%d卖%d" %
                  (date,regime,len(ContextInfo.positions),equity/10000,day_buy,day_sell))
        elif i%5==0:
            print("  %s      [%s] 持仓:%d 权益:%.2f万" %
                  (date,regime,len(ContextInfo.positions),equity/10000))

    print("\n  回测完成! 买入%d次 卖出%d次 因子记录:%d笔" %
          (buy_count,sell_count,len(ContextInfo.factor_log)))
    print_results(ContextInfo)
    print_factor_csv(ContextInfo)


# ============================================================
# 输出结果
# ============================================================

def print_results(ContextInfo):
    print("\n" + "="*80)
    print("【回测统计】%s ~ %s  v10.2 阶段分化版" % (BACKTEST_START,BACKTEST_END))
    print("="*80)

    tt=ContextInfo.stats['total_trades']
    wt=ContextInfo.stats['win_trades']
    lt=ContextInfo.stats['loss_trades']
    print("\n【交易统计】总:%d | 盈:%d | 亏:%d | 胜率:%.1f%%" %
          (tt,wt,lt,(wt/tt*100 if tt>0 else 0)))

    if ContextInfo.equity_curve:
        fe=ContextInfo.equity_curve[-1]['equity']
        tr=(fe/INITIAL_CAPITAL-1)*100
        days=len(ContextInfo.equity_curve)
        ar=((fe/INITIAL_CAPITAL)**(252.0/days)-1)*100 if days>0 and fe>0 else 0
        print("\n【收益统计】")
        print("  初始:%.0f万 | 最终:%.2f万 | 总收益:%.2f%% | 年化:%.2f%% | 最大回撤:%.2f%%" %
              (INITIAL_CAPITAL/10000,fe/10000,tr,ar,ContextInfo.max_drawdown*100))

    if ContextInfo.trade_records:
        profits=[t['pnl_pct'] for t in ContextInfo.trade_records if t['pnl']>0]
        losses =[t['pnl_pct'] for t in ContextInfo.trade_records if t['pnl']<=0]
        dh=[t['days_held'] for t in ContextInfo.trade_records]
        if profits: print("  平均盈利:%.2f%%" % (sum(profits)/len(profits)))
        if losses:  print("  平均亏损:%.2f%%" % (sum(losses)/len(losses)))
        if profits and losses:
            ap=sum(profits)/len(profits); al=abs(sum(losses)/len(losses))
            if al>0: print("  盈亏比:%.2f" % (ap/al))
        print("  平均持仓:%.1f天" % (sum(dh)/len(dh)))

    print("\n【卖出原因】")
    for r,c in ContextInfo.stats['exit_reasons'].items():
        if c>0: print("  %s: %d次 (%.1f%%)" % (r,c,c/tt*100 if tt>0 else 0))

    print("\n【各市场阶段统计】")
    for regime in ['牛市','震荡','熊市']:
        trades=ContextInfo.stats['regime_trades'].get(regime,0)
        wins  =ContextInfo.stats['regime_wins'].get(regime,0)
        wr=wins/trades*100 if trades>0 else 0
        max_p = MAX_POSITIONS_BEAR if regime=='熊市' else MAX_POSITIONS_BULL
        if regime=='熊市':
            bear_note=" (回撤>=15%%硬条件,仓位上限%d)" % max_p
        else:
            bear_note=" (仓位上限%d)" % max_p
        print("  %-6s %-6d 胜%.1f%%%s" % (regime,trades,wr,bear_note))

    print("\n【RPS分区间胜率（v10买入RPS %.0f~%.0f）】" % (RPS_MIN, RPS_MAX))
    print("-"*70)
    for lo,hi in [(0,5),(5,10),(10,15),(15,20)]:
        bin_t=[t for t in ContextInfo.trade_records if lo<=t.get('buy_rps',0)<hi]
        if not bin_t: continue
        wins=[t for t in bin_t if t['pnl']>0]
        wr=len(wins)/len(bin_t)*100
        avg=sum(t['pnl_pct'] for t in bin_t)/len(bin_t)
        print("  RPS %d~%d: %d笔 胜率%.1f%% 均盈亏%.2f%%" % (lo,hi,len(bin_t),wr,avg))
    print("-"*70)

    print("\n【市场阶段分布】")
    rc={}
    for r in ContextInfo.equity_curve: rc[r.get('regime','震荡')]=rc.get(r.get('regime','震荡'),0)+1
    total_days=len(ContextInfo.equity_curve)
    for rg,cnt in sorted(rc.items()):
        print("  %s: %d天(%.1f%%)" % (rg,cnt,cnt/total_days*100 if total_days else 0))
    print("="*80)


def print_factor_csv(ContextInfo):
    if not ContextInfo.factor_log: return
    pnl_map={}
    for t in ContextInfo.trade_records:
        pnl_map[t['stock']+'_'+t['buy_date']]={
            'pnl_pct':round(t['pnl_pct'],4),
            'win':1 if t['pnl']>0 else 0,
            'days_held':t['days_held'],
            'reason':t['reason'],
        }
    print("\n===== FACTOR_CSV_START =====")
    keys=list(ContextInfo.factor_log[0].keys())+['pnl_pct','win','days_held','sell_reason']
    print(','.join(keys))
    for rec in ContextInfo.factor_log:
        key=rec['stock']+'_'+rec['buy_date']
        res=pnl_map.get(key,{})
        row=[str(rec.get(k,'')) for k in list(rec.keys())]
        row+=[str(res.get('pnl_pct','')),str(res.get('win','')),
              str(res.get('days_held','')),str(res.get('reason',''))]
        print(','.join(row))
    print("===== FACTOR_CSV_END =====")
    print("  共记录 %d 笔" % len(ContextInfo.factor_log))


def handlebar(ContextInfo):
    if not hasattr(ContextInfo,'backtest_done'):
        ContextInfo.backtest_done=True
        run_backtest(ContextInfo)
