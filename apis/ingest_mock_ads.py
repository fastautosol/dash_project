# 2026.11.23
import random
from datetime import datetime, timedelta, timezone
from typing import Any


def generate_mock_meta_insights(days: int = 14) -> list[dict[str, Any]]:
    """Meta Graph API /{act_id}/insights valós struktúrája (Facebook + Instagram bontással)."""
    campaigns = [
        {"campaign_id": "120210001", "campaign_name": "META_CONV_PROSPECTING_HU", "adset_id": "120210011", "adset_name": "Broad_25_54_HU"},
        {"campaign_id": "120210001", "campaign_name": "META_CONV_PROSPECTING_HU", "adset_id": "120210012", "adset_name": "Lookalike_Purchasers_3pct"},
        {"campaign_id": "120210002", "campaign_name": "META_RETARGETING_DPA", "adset_id": "120210021", "adset_name": "ViewContent_ATC_14d"},
    ]
    platforms = ["facebook", "instagram"]
    today = datetime.now(timezone.utc).date()
    rows: list[dict[str, Any]] = []

    for day_offset in range(days):
        dt_str = (today - timedelta(days=day_offset)).isoformat()
        for camp in campaigns:
            for platform in platforms:
                spend = round(random.uniform(3500.0, 28000.0), 2)
                impressions = random.randint(1800, 24000)
                clicks = max(1, int(impressions * random.uniform(0.008, 0.025)))
                purchases = random.choices([0, 1, 2, 3, 5], weights=[25, 35, 20, 15, 5])[0]
                purchase_val = round(purchases * random.uniform(14900.0, 42000.0), 2)

                actions = [{"action_type": "link_click", "value": str(int(clicks * 0.82))}]
                action_values = []
                if purchases > 0:
                    actions.append({"action_type": "purchase", "value": str(purchases)})
                    action_values.append({"action_type": "purchase", "value": f"{purchase_val:.2f}"})

                # A Meta API számokat is stringként ad vissza a JSON-ban
                rows.append({
                    "account_id": "987654321012345",
                    "account_currency": "HUF",
                    "campaign_id": camp["campaign_id"],
                    "campaign_name": camp["campaign_name"],
                    "adset_id": camp["adset_id"],
                    "adset_name": camp["adset_name"],
                    "publisher_platform": platform,
                    "date_start": dt_str,
                    "date_stop": dt_str,
                    "impressions": str(impressions),
                    "clicks": str(clicks),
                    "reach": str(int(impressions * 0.84)),
                    "spend": f"{spend:.2f}",
                    "actions": actions,
                    "action_values": action_values,
                })
    return rows


def generate_mock_google_ads_insights(days: int = 14) -> list[dict[str, Any]]:
    """Google Ads REST searchStream valós struktúrája (Search, Shopping, PMax + YouTube VIDEO)."""
    campaigns = [
        {"id": "20981101", "name": "GADS_SEARCH_BRAND_HU", "channel": "SEARCH", "adgroup_id": "15400101", "adgroup_name": "Brand_Exact"},
        {"id": "20981102", "name": "GADS_PMAX_TOP_MARGIN", "channel": "PERFORMANCE_MAX", "adgroup_id": "0", "adgroup_name": "PMax_All_Assets"},
        {"id": "20981103", "name": "YT_VIDEO_ACTION_REMARKETING", "channel": "VIDEO", "adgroup_id": "15400301", "adgroup_name": "Cart_Abandoners_YT"},
        {"id": "20981104", "name": "YT_DEMAND_GEN_SHORTS", "channel": "DEMAND_GEN", "adgroup_id": "15400401", "adgroup_name": "Shorts_Prospecting"},
    ]
    today = datetime.now(timezone.utc).date()
    rows: list[dict[str, Any]] = []

    for day_offset in range(days):
        dt_str = (today - timedelta(days=day_offset)).isoformat()
        for camp in campaigns:
            spend_huf = round(random.uniform(4000.0, 35000.0), 2)
            cost_micros = int(spend_huf * 1_000_000)
            impressions = random.randint(1200, 32000)
            clicks = max(1, int(impressions * random.uniform(0.01, 0.06)))
            conversions = round(random.uniform(0.0, 7.5), 2)
            conv_value = round(conversions * random.uniform(16000.0, 48000.0), 2)
            is_video = camp["channel"] in ("VIDEO", "DEMAND_GEN")

            rows.append({
                "customer": {
                    "id": "4567891230",
                    "currencyCode": "HUF",
                },
                "campaign": {
                    "id": camp["id"],
                    "name": camp["name"],
                    "advertisingChannelType": camp["channel"],
                },
                "adGroup": {
                    "id": camp["adgroup_id"],
                    "name": camp["adgroup_name"],
                },
                "segments": {
                    "date": dt_str,
                },
                "metrics": {
                    "impressions": str(impressions),
                    "clicks": str(clicks),
                    "costMicros": str(cost_micros),
                    "conversions": conversions,
                    "conversionsValue": conv_value,
                    "videoViews": str(int(impressions * 0.32)) if is_video else "0",
                },
            })
    return rows


def generate_mock_tiktok_insights(days: int = 14) -> list[dict[str, Any]]:
    """TikTok Business API v1.3 /report/integrated/get/ valós struktúrája (EUR devizában)."""
    adgroups = [
        {"campaign_id": "18011223301", "campaign_name": "TT_CONV_UGC_SCALE_HU", "adgroup_id": "18011223391", "adgroup_name": "Broad_18_44_CreativeA"},
        {"campaign_id": "18011223301", "campaign_name": "TT_CONV_UGC_SCALE_HU", "adgroup_id": "18011223392", "adgroup_name": "Interest_Tech_Home"},
        {"campaign_id": "18011223302", "campaign_name": "TT_RETARGETING_SPARK", "adgroup_id": "18011223395", "adgroup_name": "VideoViewers_75pct_7d"},
    ]
    today = datetime.now(timezone.utc).date()
    rows: list[dict[str, Any]] = []

    for day_offset in range(days):
        dt_str = f"{(today - timedelta(days=day_offset)).isoformat()} 00:00:00"
        for ag in adgroups:
            spend_eur = round(random.uniform(12.0, 85.0), 2)
            impressions = random.randint(4500, 48000)
            clicks = max(1, int(impressions * random.uniform(0.007, 0.019)))
            purchases = random.choices([0, 1, 2, 4], weights=[30, 35, 25, 10])[0]
            purchase_val_eur = round(purchases * random.uniform(38.0, 110.0), 2)

            rows.append({
                "advertiser_id": "7198234567890123456",
                "currency": "EUR",
                "dimensions": {
                    "stat_time_day": dt_str,
                    "campaign_id": ag["campaign_id"],
                    "adgroup_id": ag["adgroup_id"],
                },
                "metrics": {
                    "campaign_name": ag["campaign_name"],
                    "adgroup_name": ag["adgroup_name"],
                    "spend": f"{spend_eur:.2f}",
                    "impressions": str(impressions),
                    "clicks": str(clicks),
                    "conversion": str(purchases),
                    "complete_payment": str(purchases),
                    "total_complete_payment_rate": f"{purchase_val_eur:.2f}",
                },
            })
    return rows
