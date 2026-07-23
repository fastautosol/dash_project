// 2026.07.23 pixel_tracker.js

// Egy központi szótár, ami összeköti a modellek slug-jait a Meta Pixel ID-kkal
const MODEL_PIXELS = {
    "chloe-throne": "1501393861736276", // Itt a te valós ID-d van a képről
    "model2_slug": "MÁSODIK_MODELL_PIXEL_ID",
    "model3_slug": "HARMADIK_MODELL_PIXEL_ID",
    "model4_slug": "NEGYEDIK_MODELL_PIXEL_ID",
    "model5_slug": "ÖTÖDIK_MODELL_PIXEL_ID"
};

// Segédfunkció a Meta alap kód inicializálásához
function initMetaPixel(pixelId) {
    if (!window.fbq) {
        !function(f,b,e,v,n,t,s){if(f.fbq)return;n=f.fbq=function(){n.callMethod?
        n.callMethod.apply(n,arguments):n.queue.push(arguments)};if(!f._fbq)f._fbq=n;
        n.push=n;n.loaded=!0;n.version='2.0';n.queue=[];t=b.createElement(e);t.async=!0;
        t.src=v;s=b.getElementsByTagName(e)[0];s.parentNode.insertBefore(t,s)}
        (window,document,'script','https://facebook.net');
    }
    fbq('init', pixelId);
}

// Ezt a funkciót fogjuk meghívni Pythonból, amikor egy modell oldala betöltődik
window.trackModelPage = function(modelSlug) {
    const pixelId = MODEL_PIXELS[modelSlug];
    if (!pixelId) return;

    initMetaPixel(pixelId);
    fbq('track', 'PageView');
    console.log(`Meta PageView elküldve a következőhöz: ${modelSlug} (ID: ${pixelId})`);
};

// Ezt a funkciót hívjuk meg a Fanvue gombra való kattintáskor
window.trackFanvueClick = function(modelSlug) {
    const pixelId = MODEL_PIXELS[modelSlug];
    if (!pixelId) return;

    fbq('trackCustom', 'Click_Fanvue_Model', { model: modelSlug });
    console.log(`Meta Fanvue kattintás mérve a következőhöz: ${modelSlug}`);
};
