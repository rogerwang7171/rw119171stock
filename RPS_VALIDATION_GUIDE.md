# RPS回测程序使用说明（含资金管理与交易原始数据记录）

本工具用于：
- 仅按RPS分层回测并打印RPS；
- 保持其他技术标准不变（通过外部 `pass_flag` 过滤接入）；
- 生成交易并统计，寻找合适RPS区间。

脚本：`rps_validation.py`

## 1) 文件顶部默认参数

在脚本开头定义了四个核心默认值：
- `BACKTEST_START_DATE = "2020-01-01"`（回测起始日期）
- `BACKTEST_END_DATE = "2025-12-31"`（回测截止日期，防止数据过大导致内存压力）
- `INITIAL_CAPITAL = 2_000_000`（总资金200万）
- `MAX_POSITION_CAPITAL = 100_000`（单票最大10万）

也可通过CLI覆盖：`--start-date`、`--end-date`、`--initial-capital`、`--max-position-capital`。

## 2) 输入数据

### A. 行情数据（必需）
CSV字段：
- `date`（如 `2024-01-02`）
- `stock`
- `close`

### B. 其他技术标准过滤（可选）
CSV字段：
- `date`
- `stock`
- `pass_flag`（1=通过，0=不通过）

用法：传 `--filter tech_filter.csv`，只保留 `pass_flag=1` 样本。

## 3) 卖出规则

回测中每笔交易按以下顺序判断卖出：
1. **止损**：亏损达到或超过8%（`ret <= -8%`）立即卖出。
2. **买入缓冲区**：买入后第6个交易日仍未上涨（期间最高收盘价 `<=` 入场价）清仓。
3. **盈利回踩MA20止盈**：仓位曾盈利后，若收盘价回踩到 MA20 下方则卖出。
4. **兜底超时**：超过 `--max-hold-days` 强制卖出（避免无限持仓）。

## 4) 运行示例

```bash
python3 rps_validation.py \
  --prices prices.csv \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --initial-capital 2000000 \
  --max-position-capital 100000 \
  --lookback 120 \
  --buffer-days 6 \
  --stop-loss-pct 0.08 \
  --max-hold-days 60 \
  --intervals 0-50,50-60,60-70,70-80,80-90,90-100 \
  --print-all-trades 1 \
  --out-dir results
```

## 5) 输出文件

- `results/rps_trades.csv`
  - 每笔交易的原始数据字段：
  - `entry_date, exit_date, stock, rps, interval, entry_price, entry_ma20, exit_price, exit_ma20, peak_close, trough_close, shares, invest_amount, ret, pnl, hold_days, exit_reason`

- `results/rps_interval_summary.csv`
  - 区间统计：`count, mean_ret, win_rate, pnl_ratio, avg_hold_days, total_pnl`

终端会打印：
- 最新交易日 RPS Top10
- 各区间回测指标
- 卖出原因分布
- 未成交信号统计（如资金不足、已有持仓）
- 建议优先RPS区间

## 6) 资金管理说明

- 总资金池按 `initial_capital` 管理；
- 单票下单金额上限为 `max_position_capital`；
- 按A股常见规则下单为100股整数倍；
- 同一只股票在前一笔未平仓前不重复开仓。


## 7) 兼容性说明（SMT-Q）

- 已移除 `dataclasses` 依赖，避免低版本Python环境出现 `ModuleNotFoundError: No module named dataclasses`。
- 当前脚本使用普通类实现数据结构，功能不变。

- 已去除文件顶部三引号模块文档字符串，规避部分SMT-Q环境在脚本加载时出现 `EOF while scanning triple-quoted string literal`。


## 8) SMT-Q 直接运行说明（你这次需求）

脚本已新增 `init(ContextInfo)` 入口，支持在平台里直接运行并输出日志，不需要命令行参数。

- 回测股票池固定为：**沪深300 + 中证500 + 中证1000 + 科创板 + 创业板**。
- 行情采用 **分批获取**（默认每批 300 只），避免一次拉取过大导致卡顿或无输出。
- 默认在 `init` 中启动即执行一次回测并打印结果摘要。
- 默认打印全部交易明细（每笔交易收益/亏损与卖出原因），便于人工逐笔核对。

若在平台中需要调参，可在 `init` 中修改：
- `ContextInfo.BACKTEST_START_DATE`
- `ContextInfo.BACKTEST_END_DATE`
- `ContextInfo.BATCH_SIZE`
- `ContextInfo.LOOKBACK` 等。

另外两个与“如何确定RPS区间”直接相关的参数：
- `ContextInfo.MIN_INTERVAL_SAMPLE`：区间最小样本数门槛（默认30）
- `ContextInfo.PRINT_ALL_TRADES`：是否打印全部交易（默认True）

程序会新增两类判定输出：
- **收益优先区间**（在样本数达标区间中按均收益优先）
- **稳定优先区间**（在样本数达标区间中按胜率/盈亏比优先）

这样可以避免只看单一均值导致“小样本高收益”的误判。
