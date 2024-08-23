CREATE VIEW performance_summary AS
SELECT 
    CAST("3rd Party Data ID" AS INTEGER) || '|' || "3rd Party Data Brand ID" AS ThirdPartyDataId,
    "Vertical",
    SUM("Hypothetical Advertiser Cost (USD)") AS total_hypothetical_cost,
    SUM("Clicks") AS total_clicks,
    SUM("Impressions") AS total_impressions,
    SUM("01 - Total Click + View Conversions") AS total_click_view_conversions
FROM 
    report_stack
GROUP BY 
    ThirdPartyDataId,
    Vertical;