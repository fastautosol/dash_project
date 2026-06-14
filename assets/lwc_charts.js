// 2026.06.14 Premium Multi-Pane Chart Rendering Pipeline

window.LWCharts = function(data, selectedIndicators) {
    if (typeof LightweightCharts === "undefined") {
        console.error("LightweightCharts library is not loaded globally.");
        return "LWCharts missing";
    }

    const symbols = Object.keys(data);
    if (!window.lwcCharts) { window.lwcCharts = {}; }
    if (!selectedIndicators) { selectedIndicators = []; }

    symbols.forEach((symbol) => {
        try {
            const chartData = data[symbol];
            const candles    = chartData.candles    || [];
            const indicators = chartData.indicators || [];
            const vpData     = chartData.volume_profile || [];
        
            if (!Array.isArray(candles) || candles.length === 0) { return; }
            const safeId = symbol.replace("/", "-");
            const el = document.getElementById(`chart-${safeId}`);
            if (!el) { return; }

            // Clear old instances cleanly
            if (window.lwcCharts[safeId]) {
                try { window.lwcCharts[safeId].remove(); } catch(e) {}
                window.lwcCharts[safeId] = null;
            }
            el.innerHTML = "";

            // Init Base Chart
            const chart = LightweightCharts.createChart(el, {
                autoSize: true,
                layout: { background: { color: "#111111" }, textColor: "#DDDDDD" },
                grid: { vertLines: { color: "#222222" }, horzLines: { color: "#222222" } },
                crosshair: { mode: 1 },
                rightPriceScale: { borderColor: "#444", scaleMargins: { top: 0.1, bottom: 0.1 } },
                timeScale: { barSpacing: 4, rightOffset: 4, borderColor: "#444", timeVisible: true }
            });

            // Main Price Candles
            const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
                upColor: "#26a69a", downColor: "#ef5350", borderVisible: false, wickUpColor: "#26a69a", wickDownColor: "#ef5350"
            }, 0);
            candleSeries.setData(candles.map(c => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })));

            // -------------------------------------------------
            // RENDER: VOLUME PROFILE (VP)
            // -------------------------------------------------
            if (selectedIndicators.includes("volume_profile") && vpData.length > 0) {
                const maxVol = Math.max(...vpData.map(v => v.volume));
                const totalBars = candles.length;
                // Scale width to fill up to 25% of the chart horizontally
                const maxBarWidth = Math.max(5, Math.round(totalBars * 0.25));

                vpData.forEach(bin => {
                    if (bin.volume > 0) {
                        const widthUnits = Math.round((bin.volume / maxVol) * maxBarWidth);
                        const startIndex = Math.max(0, totalBars - 1 - widthUnits);
                        
                        const startTime = candles[startIndex].time;
                        const endTime = candles[totalBars - 1].time;

                        const vpLine = chart.addSeries(LightweightCharts.LineSeries, {
                            color: "rgba(38, 166, 154, 0.25)",
                            lineWidth: 2,
                            priceLineVisible: false,
                            lastValueVisible: false,
                            crosshairMarkerVisible: false
                        }, 0);

                        vpLine.setData([
                            { time: startTime, value: bin.price },
                            { time: endTime, value: bin.price }
                        ]);
                    }
                });
            }

            // Track sub-panes dynamically
            let paneCounter = 0;

            // Main Overlays (Pane 0)
            if (selectedIndicators.includes("sma50")) {
                const smaSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#42a5f5", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, 0);
                smaSeries.setData(indicators.filter(x => x.sma50 != null).map(x => ({ time: x.time, value: x.sma50 })));
            }
            if (selectedIndicators.includes("ema50")) {
                const emaSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#f5a623", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, 0);
                emaSeries.setData(indicators.filter(x => x.ema50 != null).map(x => ({ time: x.time, value: x.ema50 })));
            }
            if (selectedIndicators.includes("bb50")) {
                const bbu = chart.addSeries(LightweightCharts.LineSeries, { color: "#2196f3", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, 0);
                const bbm = chart.addSeries(LightweightCharts.LineSeries, { color: "#555555", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, 0);
                const bbl = chart.addSeries(LightweightCharts.LineSeries, { color: "#2196f3", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, 0);
                bbu.setData(indicators.filter(x => x.bb_upper != null).map(x => ({ time: x.time, value: x.bb_upper })));
                bbm.setData(indicators.filter(x => x.bb_middle != null).map(x => ({ time: x.time, value: x.bb_middle })));
                bbl.setData(indicators.filter(x => x.bb_lower != null).map(x => ({ time: x.time, value: x.bb_lower })));
            }
            if (selectedIndicators.includes("vwap")) {
                const vwapSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#00e676", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, 0);
                vwapSeries.setData(indicators.filter(x => x.vwap != null).map(x => ({ time: x.time, value: x.vwap })));
            }

            // Lower Sub-pane: Volume Delta
            if (selectedIndicators.includes("volume_delta")) {
                paneCounter++;
                chart.addPane({ height: 45 });
                const bVol = chart.addSeries(LightweightCharts.HistogramSeries, { color: "#26a69a", priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false }, paneCounter);
                const sVol = chart.addSeries(LightweightCharts.HistogramSeries, { color: "#ef5350", priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false }, paneCounter);
                bVol.setData(indicators.filter(x => x.buy_vol != null && x.buy_vol > 0).map(x => ({ time: x.time, value: x.buy_vol })));
                sVol.setData(indicators.filter(x => x.sell_vol != null && x.sell_vol > 0).map(x => ({ time: x.time, value: -x.sell_vol })));
            }

            // Lower Sub-pane: MFI
            if (selectedIndicators.includes("mfi")) {
                paneCounter++;
                chart.addPane({ height: 45 });
                const mfiS = chart.addSeries(LightweightCharts.LineSeries, { color: "#ce93d8", lineWidth: 1, priceLineVisible: false, lastValueVisible: false }, paneCounter);
                const obLine = chart.addSeries(LightweightCharts.LineSeries, { color: "#ef5350", lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false }, paneCounter);
                const osLine = chart.addSeries(LightweightCharts.LineSeries, { color: "#26a69a", lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: false }, paneCounter);
                const mPoints = indicators.filter(x => x.mfi != null);
                mfiS.setData(mPoints.map(x => ({ time: x.time, value: x.mfi })));
                obLine.setData(mPoints.map(x => ({ time: x.time, value: 80 })));
                osLine.setData(mPoints.map(x => ({ time: x.time, value: 20 })));
            }

            // Lower Sub-pane: Volume Flow Indicator (VFI)
            if (selectedIndicators.includes("vfi")) {
                paneCounter++;
                chart.addPane({ height: 45 });
                const vfiSeries = chart.addSeries(LightweightCharts.LineSeries, { color: "#e11d48", lineWidth: 1.5, priceLineVisible: false, lastValueVisible: false }, paneCounter);
                const zeroLine = chart.addSeries(LightweightCharts.LineSeries, { color: "#555555", lineWidth: 1, lineStyle: 3, priceLineVisible: false, lastValueVisible: false }, paneCounter);
                const vPoints = indicators.filter(x => x.vfi != null);
                vfiSeries.setData(vPoints.map(x => ({ time: x.time, value: x.vfi })));
                zeroLine.setData(vPoints.map(x => ({ time: x.time, value: 0 })));
            }

            window.lwcCharts[safeId] = chart;

        } catch(err) {
            console.error("Rendering pipeline error context for:", symbol, err);
        }
    });

    return "rendered";
}; 
