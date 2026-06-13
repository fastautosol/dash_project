// 2026.06.13 Premium Multi-Pane Chart Rendering Pipeline

window.LWCharts = function(data, selectedIndicators) {

    console.log("renderMultiCharts() called with premium metrics");

    if (typeof LightweightCharts === "undefined") {
        console.error("LightweightCharts library is not loaded globally.");
        return "LWCharts missing";
    }

    if (!data) { console.error("No chart data delivered"); return "No data"; }
    const symbols = Object.keys(data);
    if (!window.lwcCharts) { window.lwcCharts = {}; }

    // -------------------------------------------------
    // LOOP GLOBAL SYMBOLS
    // -------------------------------------------------
    symbols.forEach((symbol) => {
        try {
            const chartData = data[symbol];
            const candles    = chartData.candles    || [];
            const indicators = chartData.indicators || [];
        
            if (!Array.isArray(candles) || candles.length === 0) { return; }
            const safeId = symbol.replace("/", "-");
            const el = document.getElementById(`chart-${safeId}`);
            if (!el) { return; }

            // -------------------------------------------------
            // GARBAGE COLLECTION & RE-INITIALIZATION
            // -------------------------------------------------
            if (window.lwcCharts[safeId]) {
                try {
                    window.lwcCharts[safeId].remove();
                } catch(e) {
                    console.warn("Chart removal failed:", e);
                }
                window.lwcCharts[safeId] = null;
            }
            el.innerHTML = "";

            // -------------------------------------------------
            // BASE CHART CREATION
            // -------------------------------------------------
            const chart = LightweightCharts.createChart(el, {
                autoSize: true,
                layout: { background: { color: "#111111" }, textColor: "#DDDDDD" },
                grid: { vertLines: { color: "#222222" }, horzLines: { color: "#222222" } },
                crosshair: { mode: 1 },
                rightPriceScale: { borderColor: "#444" },
                timeScale: { barSpacing: 3, rightOffset: 5, borderColor: "#444", timeVisible: true, secondsVisible: false }
            });

            // -------------------------------------------------
            // MAIN CANDLESTICK SERIES
            // -------------------------------------------------
            const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
                upColor: "#26a69a", downColor: "#ef5350", borderVisible: false, wickUpColor: "#26a69a", wickDownColor: "#ef5350"
            });
            candleSeries.setData(candles.map(c => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })));

            // -------------------------------------------------
            // DYNAMIC SUB-PANE INDEX TRACKING
            // -------------------------------------------------
            let currentPaneIndex = 0;

            // -------------------------------------------------
            // OVERLAY: VOLUME PROFILE (VP) - Renders on Price Pane
            // -------------------------------------------------
            if (selectedIndicators.includes("volume_profile") && chartData.volume_profile && chartData.volume_profile.length > 0) {
                const maxVol = Math.max(...chartData.volume_profile.map(v => v.volume));
                const totalBars = candles.length;
                const profileWidthScale = Math.max(15, Math.round(totalBars * 0.22)); 

                chartData.volume_profile.forEach(bin => {
                    const barWidthInTimeUnits = Math.round((bin.volume / maxVol) * profileWidthScale);
                    if (barWidthInTimeUnits > 0) {
                        const vpLine = chart.addSeries(LightweightCharts.LineSeries, {
                            color: "rgba(38, 166, 154, 0.18)", 
                            lineWidth: 2, 
                            priceLineVisible: false,
                            lastValueVisible: false
                        }, 0); // Always forces registration onto parent price pane (Index 0)

                        const endTime = candles[candles.length - 1].time;
                        const startIndex = Math.max(0, candles.length - 1 - barWidthInTimeUnits);
                        const startTime = candles[startIndex].time;

                        vpLine.setData([
                            { time: startTime, value: bin.price },
                            { time: endTime, value: bin.price }
                        ]);
                    }
                });
            }

            // -------------------------------------------------
            // MOVING AVERAGES & OVERLAYS (PANE 0)
            // -------------------------------------------------
            if (selectedIndicators.includes("sma50")) {
                const smaSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#42a5f5", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
                smaSeries.setData(indicators.filter(x => x.sma50 != null).map(x => ({ time: x.time, value: x.sma50 })));
            }

            if (selectedIndicators.includes("ema50")) {
                const emaSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#f5a623", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
                emaSeries.setData(indicators.filter(x => x.ema50 != null).map(x => ({ time: x.time, value: x.ema50 })));
            }

            if (selectedIndicators.includes("bb50")) {
                const bbUpperSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#42a5f5", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
                const bbMiddleSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#888888", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
                const bbLowerSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#42a5f5", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
                bbUpperSeries.setData(indicators.filter(x => x.bb_upper != null).map(x => ({ time: x.time, value: x.bb_upper })));
                bbMiddleSeries.setData(indicators.filter(x => x.bb_middle != null).map(x => ({ time: x.time, value: x.bb_middle })));
                bbLowerSeries.setData(indicators.filter(x => x.bb_lower != null).map(x => ({ time: x.time, value: x.bb_lower })));
            }

            if (selectedIndicators.includes("vwap")) {
                const vwapSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#00e676", lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
                vwapSeries.setData(indicators.filter(x => x.vwap != null).map(x => ({ time: x.time, value: x.vwap })));
            }

            // -------------------------------------------------
            // LOWER SUB-PANE: VOLUME DELTA
            // -------------------------------------------------
            if (selectedIndicators.includes("volume_delta")) {
                currentPaneIndex++;
                chart.addPane({ height: 50 });
            
                const buyVolSeries = chart.addSeries(LightweightCharts.HistogramSeries, { 
                    color: "#26a69a", priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false, baseLineVisible: false 
                }, currentPaneIndex);

                const sellVolSeries = chart.addSeries(LightweightCharts.HistogramSeries, { 
                    color: "#ef5350", priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false, baseLineVisible: false 
                }, currentPaneIndex);
            
                buyVolSeries.setData(indicators.filter(x => x.buy_vol != null && x.buy_vol > 0).map(x => ({ time: x.time, value: x.buy_vol })));
                sellVolSeries.setData(indicators.filter(x => x.sell_vol != null && x.sell_vol > 0).map(x => ({ time: x.time, value: -x.sell_vol })));
            }

            // -------------------------------------------------
            // LOWER SUB-PANE: MONEY FLOW INDEX (MFI)
            // -------------------------------------------------
            if (selectedIndicators.includes("mfi")) {
                currentPaneIndex++;
                chart.addPane({ height: 50 });
            
                const mfiSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#ce93d8", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, currentPaneIndex);
                const obSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#ef5350", lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false }, currentPaneIndex);
                const osSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#26a69a", lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false }, currentPaneIndex);
            
                const mfiPoints = indicators.filter(x => x.mfi != null);
                mfiSeries.setData(mfiPoints.map(x => ({ time: x.time, value: x.mfi })));
                obSeries.setData(mfiPoints.map(x => ({ time: x.time, value: 80 })));
                osSeries.setData(mfiPoints.map(x => ({ time: x.time, value: 20 })));
            }

            // -------------------------------------------------
            // LOWER SUB-PANE: VOLUME FLOW INDICATOR (VFI)
            // -------------------------------------------------
            if (selectedIndicators.includes("vfi")) {
                currentPaneIndex++;
                chart.addPane({ height: 50 });

                const vfiSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#e11d48", lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false }, currentPaneIndex);
                const zeroLine = chart.addSeries(LightweightCharts.LineSeries, { color: "#555555", lineWidth: 1, lineStyle: 3, priceLineVisible: false, lastValueVisible: false }, currentPaneIndex);

                const vfiPoints = indicators.filter(x => x.vfi != null);
                vfiSeries.setData(vfiPoints.map(x => ({ time: x.time, value: x.vfi })));
                zeroLine.setData(vfiPoints.map(x => ({ time: x.time, value: 0 })));
            }

            // -------------------------------------------------
            // REGISTER INSTANCE BACK TO CACHE
            // -------------------------------------------------
            window.lwcCharts[safeId] = chart;

        } catch(err) {
            console.error("Critical rendering error detected on asset loop initialization:", symbol, err);
        }
    });

    return "rendered";
};
