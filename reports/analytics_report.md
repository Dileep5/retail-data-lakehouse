# Retail Analytics Report

Generated 2026-11-02 18:18 from the warehouse.

## Kpi Summary

| completed_orders | revenue | avg_order_value | return_rate_pct | online_share_pct |
|---|---|---|---|---|
| 1071 | 1190327.50 | 1111.42 | 5.60 | 32.21 |

## Monthly Revenue By Region

| month | region | revenue |
|---|---|---|
| 2025-08 | Central | 159819.03 |
| 2025-08 | East | 54849.45 |
| 2025-08 | West | 171458.17 |
| 2025-09 | Central | 167241.43 |
| 2025-09 | East | 57506.84 |
| 2025-09 | West | 152898.97 |
| 2025-10 | Central | 129812.47 |
| 2025-10 | East | 89070.25 |
| 2025-10 | West | 170967.09 |
| 2025-11 | Central | 17691.36 |
| 2025-11 | East | 4853.95 |
| 2025-11 | West | 14158.49 |

## Category Margin

| category | units | revenue | gross_margin | margin_pct |
|---|---|---|---|---|
| Tents | 717 | 327682.25 | 134043.17 | 40.9 |
| Electronics | 645 | 184298.23 | 78084.58 | 42.4 |
| Apparel | 616 | 160983.69 | 62972.50 | 39.1 |
| Sleeping Bags | 670 | 144455.39 | 54115.22 | 37.5 |
| Climbing | 670 | 112070.55 | 50020.39 | 44.6 |
| Backpacks | 619 | 102204.55 | 43654.07 | 42.7 |
| Footwear | 604 | 101036.43 | 42625.15 | 42.2 |
| Cooking | 698 | 57596.41 | 23884.60 | 41.5 |

## Top Customers

| customer_id | full_name | current_tier | completed_orders | lifetime_revenue | avg_order_value |
|---|---|---|---|---|---|
| 160 | Jacob Kumar | Bronze | 10 | 16041.92 | 1604.19 |
| 251 | Hannah Ahmed | Gold | 7 | 12813.55 | 1830.51 |
| 271 | Wei Clark | Bronze | 8 | 11997.97 | 1499.75 |
| 163 | Arjun Singh | Bronze | 8 | 11622.31 | 1452.79 |
| 7 | Hannah Martin | Bronze | 8 | 10416.01 | 1302.00 |
| 116 | Jacob Leblanc | Bronze | 5 | 10275.93 | 2055.19 |
| 20 | Chloe Lee | Bronze | 9 | 10125.05 | 1125.01 |
| 39 | Lucas Gagnon | Bronze | 5 | 9859.28 | 1971.86 |
| 221 | Mohammed Roy | Silver | 10 | 9589.46 | 958.95 |
| 1 | Wei Nguyen | Silver | 6 | 8919.19 | 1486.53 |

## Revenue By Tier At Order Time

| loyalty_tier_at_order | current_tier | completed_orders | revenue |
|---|---|---|---|
| Bronze | Bronze | 662 | 730482.11 |
| Bronze | Gold | 8 | 6807.47 |
| Bronze | Silver | 31 | 36470.09 |
| Gold | Gold | 95 | 107061.87 |
| Gold | Silver | 6 | 8919.19 |
| Silver | Silver | 269 | 300586.77 |

## Inventory Reorder Alerts

| store_name | sku | product_name | quantity_on_hand | reorder_point | units_short |
|---|---|---|---|---|---|
| Maple Ridge Kelowna | MR-ELE-0026 | Ridge Electronic 26 | 0 | 15 | 15 |
| Maple Ridge Vancouver Kitsilano | MR-APP-0022 | Summit Apparel 22 | 1 | 15 | 14 |
| Maple Ridge Halifax Waterfront | MR-ELE-0030 | Ridge Electronic 30 | 1 | 13 | 12 |
| Maple Ridge Calgary Kensington | MR-BAC-0011 | Tundra Backpack 11 | 0 | 11 | 11 |
| Maple Ridge Kelowna | MR-ELE-0030 | Ridge Electronic 30 | 1 | 12 | 11 |
| Maple Ridge Waterloo Uptown | MR-APP-0021 | Canyon Apparel 21 | 4 | 15 | 11 |
| Maple Ridge Waterloo Uptown | MR-COO-0035 | Glacier Cooking 35 | 0 | 11 | 11 |
| Maple Ridge Waterloo Uptown | MR-BAC-0011 | Tundra Backpack 11 | 6 | 15 | 9 |
| Maple Ridge Calgary Kensington | MR-TEN-0005 | Alpine Tent 5 | 6 | 14 | 8 |
| Maple Ridge Halifax Waterfront | MR-SLE-0006 | Alpine Sleeping Bag 6 | 3 | 11 | 8 |
| Maple Ridge Montreal Plateau | MR-APP-0025 | Canyon Apparel 25 | 3 | 11 | 8 |
| Maple Ridge Ottawa Rideau | MR-BAC-0012 | Boreal Backpack 12 | 5 | 13 | 8 |
| Maple Ridge Calgary Kensington | MR-BAC-0015 | Tundra Backpack 15 | 6 | 13 | 7 |
| Maple Ridge Halifax Waterfront | MR-TEN-0001 | Canyon Tent 1 | 1 | 8 | 7 |
| Maple Ridge Montreal Plateau | MR-ELE-0026 | Ridge Electronic 26 | 4 | 11 | 7 |

## Pipeline Batches

| batch_id | window_start | window_end | status | finished_at |
|---|---|---|---|---|
| 2025-11-01 | 1970-01-01 00:00:00 | 2025-11-01 00:00:00 | succeeded | 2026-11-02 18:17:40.019645 |
| 2025-11-02 | 2025-11-01 00:00:00 | 2025-11-02 00:00:00 | succeeded | 2026-11-02 18:17:49.185325 |
| 2025-11-03 | 2025-11-02 00:00:00 | 2025-11-03 00:00:00 | succeeded | 2026-11-02 18:18:06.839556 |
