// 2026.06.14  12.00

// -------------------------------------------------
// ORDER FLOW / VOLUME PROFILE PRIMITIVE
// Draws a single footprint-style column to the right
// of the last candle: sell volume (purple) on the left
// half, buy volume (green) on the right half, one row
// per price bucket.
// -------------------------------------------------

class VolumeProfilePrimitive {
    constructor(profile, lastTime, options = {}) {
        this._profile = profile || [];
        this._lastTime = lastTime;
        this._colWidth = options.colWidth || 24;   // px width of each side (buy/sell)
        this._gap = options.gap || 4;              // px gap from last candle
        this._chart = null;
        this._series = null;
    }

    attached({ chart, series }) {
        this._chart = chart;
        this._series = series;
    }

    detached() {
        this._chart = null;
        this._series = null;
    }

    updateAllViews() {}

    paneViews() {
        return [{
            renderer: {
                draw: () => {},
                drawBackground: (target) => {
                    if (!this._chart || !this._series || !this._profile.length) return;

                    target.useBitmapCoordinateSpace((scope) => {
                        const ctx = scope.context;
                        const timeScale = this._chart.timeScale();

                        const xAnchor = timeScale.timeToCoordinate(this._lastTime);
                        if (xAnchor === null) return;

                        const x0 = (xAnchor + this._gap) * scope.horizontalPixelRatio;
                        const gapPx = 2 * scope.horizontalPixelRatio;

                        const maxVol = Math.max(
                            ...this._profile.map(p => Math.max(p.buy || 0, p.sell || 0)),
                            1e-9
                        );

                        for (const bucket of this._profile) {
                            const yTop = this._series.priceToCoordinate(bucket.price_high);
                            const yBot = this._series.priceToCoordinate(bucket.price_low);
                            if (yTop === null || yBot === null) continue;

                            const y = Math.min(yTop, yBot) * scope.verticalPixelRatio;
                            const h = Math.max(1, Math.abs(yBot - yTop) * scope.verticalPixelRatio - 1);

                            const sellW = ((bucket.sell || 0) / maxVol) * this._colWidth * scope.horizontalPixelRatio;
                            const buyW  = ((bucket.buy  || 0) / maxVol) * this._colWidth * scope.horizontalPixelRatio;

                            // Sell volume — purple, grows left of x0
                            if (sellW > 0) {
                                ctx.fillStyle = "#7e57c2";
                                ctx.fillRect(x0 - sellW, y, sellW, h);
                            }

                            // Buy volume — green, grows right of x0
                            if (buyW > 0) {
                                ctx.fillStyle = "#26a69a";
                                ctx.fillRect(x0 + gapPx, y, buyW, h);
                            }
                        }
                    });
                },
            },
            zOrder: "top",
        }];
    }
}

// -------------------------------------------------
// MAIN RENDER FUNCTION
// -------------------------------------------------

window.LWCharts = function(data, selectedIndicators) {

    console.log("renderMultiCharts() called");

    if (typeof LightweightCharts === "undefined") {
        console.error("LightweightCharts not loaded");
        return "LWCharts missing";
    }

    if (!data) {console.error("No chart data"); return "No data";}
    const symbols = Object.keys(data);
    if (!window.lwcCharts) { window.lwcCharts = {};}

    const showProfile = selectedIndicators.includes("profile");

    // -------------------------------------------------
    // LOOP SYMBOLS
    // -------------------------------------------------

    symbols.forEach((symbol) => {

        try {
            console.log("Rendering:", symbol);
  
            const chartData = data[symbol];
            const candles    = chartData.candles    || [];
            const indicators = chartData.indicators || [];
            const profile    = chartData.profile    || [];
        
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

            // Leave extra room on the right when the order-flow
            // column is shown, so it doesn't get clipped.
            const rightOffset = showProfile && profile.length ? 12 : 5;

            const chart = LightweightCharts.createChart(el, {
                autoSize: true,
                layout: { background: { color: "#111111" }, textColor: "#DDDDDD"},
                grid: { vertLines: { color: "#222222" }, horzLines: { color: "#222222" }},
                crosshair: { mode: 1},
                rightPriceScale: {borderColor: "#444"},
                timeScale: {barSpacing: 3, rightOffset: rightOffset, borderColor: "#444", timeVisible: true, secondsVisible: false}
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
            // VOLUME DELTA  (buy_vol ↑ green / sell_vol ↓ red)
            // -------------------------------------------------

            if (selectedIndicators.includes("volume_delta")) {
            
                chart.addPane({ height: 60 });   // creates pane index 1
            
                const buyVolSeries = chart.addSeries(
                    LightweightCharts.HistogramSeries,
                    { color: "#26a69a", priceFormat: { type: "volume" },
                      priceLineVisible: false, lastValueVisible: false,
                      baseLineVisible: false },
                    1   // ← pane index
                );

                const sellVolSeries = chart.addSeries(
                    LightweightCharts.HistogramSeries,
                    { color: "#ef5350", priceFormat: { type: "volume" },
                      priceLineVisible: false, lastValueVisible: false,
                      baseLineVisible: false },
                    1   // ← same pane index
                );
            
                buyVolSeries.setData(
                    indicators
                        .filter(x => x.buy_vol != null && x.buy_vol > 0)
                        .map(x => ({ time: x.time, value: x.buy_vol }))
                );
            
                sellVolSeries.setData(
                    indicators
                        .filter(x => x.sell_vol != null && x.sell_vol > 0)
                        .map(x => ({ time: x.time, value: -x.sell_vol }))  // ← negative → below zero
                );
            }

            // -------------------------------------------------
            // MFI  (Money Flow Index, overbought/oversold lines)
            // -------------------------------------------------
            
            if (selectedIndicators.includes("mfi")) {
            
                chart.addPane({ height: 55 });   // creates pane index 2
                //   (or index 1 if volume_delta is NOT selected — addPane() is only called
                //    when that block runs, so the pane index depends on render order)
            
                const mfiPane = selectedIndicators.includes("volume_delta") ? 2 : 1;
            
                const mfiSeries = chart.addSeries(
                    LightweightCharts.LineSeries,
                    { color: "#ce93d8", lineWidth: 1,
                      priceLineVisible: false, lastValueVisible: false },
                    mfiPane
                            );
            
                // Overbought (80) and oversold (20) reference lines
                const obSeries = chart.addSeries(
                    LightweightCharts.LineSeries,
                    { color: "#ef5350", lineWidth: 1, lineStyle: 2,
                      priceLineVisible: false, lastValueVisible: false },
                    mfiPane
                );
                const osSeries = chart.addSeries(
                    LightweightCharts.LineSeries,
                    { color: "#26a69a", lineWidth: 1, lineStyle: 2,
                      priceLineVisible: false, lastValueVisible: false },
                    mfiPane
                );
            
                const mfiPoints = indicators.filter(x => x.mfi != null);
                mfiSeries.setData(mfiPoints.map(x => ({ time: x.time, value: x.mfi   })));
                obSeries.setData( mfiPoints.map(x => ({ time: x.time, value: 80      })));
                osSeries.setData( mfiPoints.map(x => ({ time: x.time, value: 20      })));
            }

            // -------------------------------------------------
            // ORDER FLOW / VOLUME PROFILE (last candle, right side)
            // -------------------------------------------------

            if (showProfile && Array.isArray(profile) && profile.length) {
                const lastTime = candles[candles.length - 1].time;
                const profilePrimitive = new VolumeProfilePrimitive(profile, lastTime, { colWidth: 24, gap: 4 });
                candleSeries.attachPrimitive(profilePrimitive);
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
