// 2026.05.31  12.00

window.LWCharts = function(data, selectedIndicators) {

    console.log("renderMultiCharts() called");

    if (typeof LightweightCharts === "undefined") {
        console.error("LightweightCharts not loaded");
        return "LWCharts missing";
    }

    if (!data) {console.error("No chart data"); return "No data";}
    const symbols = Object.keys(data);
    if (!window.lwcCharts) { window.lwcCharts = {};}

    // -------------------------------------------------
    // LOOP SYMBOLS
    // -------------------------------------------------

    symbols.forEach((symbol) => {

        try {
            console.log("Rendering:", symbol);
  
            const chartData = data[symbol];
            const candles    = chartData.candles    || [];
            const indicators = chartData.indicators || [];
        
            if (!Array.isArray(candles) || candles.length === 0) {console.warn("No candles:", symbol); return;}
            const safeId = symbol.replace("/", "-");
            const el = document.getElementById(`chart-${safeId}`);
            if (!el) {console.warn("Missing div:", safeId); return;}

            // -------------------------------------------------
            // REMOVE OLD CHART
            // -------------------------------------------------

            if (window.lwcCharts[safeId]) {
                try {
                    window.lwcCharts[safeId].remove();
                }
                catch(e) {
                    console.warn("Remove failed:", e);
                }
                window.lwcCharts[safeId] = null;
            }
            el.innerHTML = "";

            // -------------------------------------------------
            // CREATE CHART
            // -------------------------------------------------

            const chart = LightweightCharts.createChart(el, {
                autoSize: true,
                layout: { background: { color: "#111111" }, textColor: "#DDDDDD"},
                grid: { vertLines: { color: "#222222" }, horzLines: { color: "#222222" }},
                crosshair: { mode: 1},
                rightPriceScale: {borderColor: "#444"},
                timeScale: {barSpacing: 3, rightOffset: 5, borderColor: "#444", timeVisible: true, secondsVisible: false}
            });

            // -------------------------------------------------
            // CANDLES
            // -------------------------------------------------

            const candleSeries = chart.addSeries(LightweightCharts.CandlestickSeries, 
                {upColor: "#26a69a",downColor: "#ef5350", borderVisible: false, wickUpColor: "#26a69a", wickDownColor: "#ef5350"});
            candleSeries.setData(candles.map(c => ({time: c.time, open: c.open, high: c.high, low: c.low, close: c.close})));

            // -------------------------------------------------
            // SMA50
            // -------------------------------------------------

            if (selectedIndicators.includes("sma50")) {
                const smaSeries = chart.addSeries(LightweightCharts.LineSeries, {color:"#42a5f5",lineWidth:1, priceLineVisible:false, lastValueVisible:false});
                smaSeries.setData(indicators.filter(x => x.sma50 != null).map(x => ({time: x.time, value: x.sma50})));
            }

            // -------------------------------------------------
            // EMA50
            // -------------------------------------------------

            if (selectedIndicators.includes("ema50")) {
                const emaSeries = chart.addSeries(LightweightCharts.LineSeries, {color:"#f5a623",lineWidth:1, priceLineVisible:false, lastValueVisible:false});
                emaSeries.setData(indicators.filter(x => x.ema50 != null).map(x => ({time: x.time, value: x.ema50})));
            }

            // -------------------------------------------------
            // BOLLINGER BANDS
            // -------------------------------------------------

            if (selectedIndicators.includes("bb50")) {
                const bbUpperSeries = chart.addSeries(LightweightCharts.LineSeries, {color:"#42a5f5", lineWidth:1, priceLineVisible:false, lastValueVisible:false});
                const bbMiddleSeries = chart.addSeries(LightweightCharts.LineSeries, {color:"#888888", lineWidth:1, priceLineVisible:false, lastValueVisible:false});
                const bbLowerSeries = chart.addSeries(LightweightCharts.LineSeries, {color: "#42a5f5", lineWidth:1, priceLineVisible:false, lastValueVisible:false});
                bbUpperSeries.setData(indicators.filter(x => x.bb_upper != null).map(x => ({time: x.time, value: x.bb_upper})));
                bbMiddleSeries.setData(indicators.filter(x => x.bb_middle != null).map(x => ({time: x.time, value: x.bb_middle})));
                bbLowerSeries.setData(indicators.filter(x => x.bb_lower != null).map(x => ({time: x.time, value: x.bb_lower})));
            }

            // -------------------------------------------------
            // VWAP
            // -------------------------------------------------

            if (selectedIndicators.includes("vwap")) {
                const vwapSeries = chart.addSeries(LightweightCharts.LineSeries, {color:"#00e676", lineWidth:1, priceLineVisible:false, lastValueVisible:false});
                vwapSeries.setData(indicators.filter(x => x.vwap != null).map(x => ({time: x.time, value: x.vwap })));
            }

            // -------------------------------------------------
            // FINALIZE
            // -------------------------------------------------

            //chart.timeScale().fitContent();
            //chart.timeScale().setVisibleLogicalRange({from: data.length - 200, to: data.length})
            window.lwcCharts[safeId] = chart;
            console.log("Chart OK:", symbol);

        }
        catch(err) {
            console.error("Chart render failed:", symbol);
            console.error(err);
        }

    });

    return "rendered";
};
