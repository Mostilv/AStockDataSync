from vnpy_ctastrategy.backtesting import BacktestingEngine, OptimizationSetting
from vnpy_ctastrategy import CtaTemplate
from datetime import datetime
from strategies.double_ma import DoubleMaStrategy

def main():
    engine = BacktestingEngine()
    engine.set_parameters(
        vt_symbol="600435",
        interval="d",
        start=datetime(2024, 1, 1),
        end=datetime(2024, 12, 31),
        rate=0.0005,
        slippage=0.2,
        size=100,
        pricetick=0.01,
        capital=1_000_000,
    )
    engine.add_strategy(DoubleMaStrategy, {"fast_window": 10, "slow_window": 20})
    engine.load_data()
    engine.run_backtest()
    df = engine.calculate_result()
    statistics = engine.calculate_statistics(output=True)
    engine.show_chart()

if __name__ == "__main__":
    main()