# Retail Analytics Report

Generated 2026-11-02 18:56 from the warehouse.

## Kpi Summary

| completed_orders | revenue | avg_order_value | return_rate_pct | online_share_pct |
|---|---|---|---|---|
| 1091 | 1209632.34 | 1108.74 | 5.80 | 32.26 |

## Monthly Revenue By Region

| month | region | revenue |
|---|---|---|
| 2025-08 | Central | 159819.03 |
| 2025-08 | East | 54849.45 |
| 2025-08 | West | 171458.17 |
| 2025-09 | Central | 167241.43 |
| 2025-09 | East | 57506.84 |
| 2025-09 | West | 152898.97 |
| 2025-10 | Central | 130326.12 |
| 2025-10 | East | 89070.25 |
| 2025-10 | West | 169225.62 |
| 2025-11 | Central | 29130.15 |
| 2025-11 | East | 9495.86 |
| 2025-11 | West | 18610.45 |

## Category Margin

| category | units | revenue | gross_margin | margin_pct |
|---|---|---|---|---|
| Tents | 723 | 330598.10 | 135370.13 | 40.9 |
| Electronics | 655 | 187408.67 | 79364.84 | 42.3 |
| Apparel | 619 | 162138.01 | 63489.28 | 39.2 |
| Sleeping Bags | 688 | 148010.73 | 55391.94 | 37.4 |
| Climbing | 690 | 114938.20 | 51284.36 | 44.6 |
| Backpacks | 640 | 105490.99 | 44992.94 | 42.7 |
| Footwear | 616 | 102891.05 | 43419.36 | 42.2 |
| Cooking | 708 | 58156.59 | 24094.50 | 41.4 |

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
| 176 | Jacob Clark | Silver | 10 | 9889.99 | 989.00 |
| 39 | Lucas Gagnon | Bronze | 5 | 9859.28 | 1971.86 |
| 1 | Wei Nguyen | Silver | 6 | 8919.19 | 1486.53 |

## Revenue By Tier At Order Time

| loyalty_tier_at_order | current_tier | completed_orders | revenue |
|---|---|---|---|
| Bronze | Bronze | 667 | 736250.16 |
| Bronze | Gold | 8 | 6807.47 |
| Bronze | Silver | 37 | 43514.48 |
| Gold | Gold | 91 | 100222.04 |
| Gold | Silver | 10 | 15759.02 |
| Silver | Gold | 6 | 6628.60 |
| Silver | Silver | 272 | 300450.57 |

## Inventory Reorder Alerts

| store_name | sku | product_name | quantity_on_hand | reorder_point | units_short |
|---|---|---|---|---|---|
| Maple Ridge Ottawa Rideau | MR-SLE-0006 | Alpine Sleeping Bag 6 | 0 | 15 | 15 |
| Maple Ridge Calgary Kensington | MR-APP-0022 | Summit Apparel 22 | 1 | 15 | 14 |
| Maple Ridge Toronto Queen St | MR-COO-0035 | Glacier Cooking 35 | 0 | 14 | 14 |
| Maple Ridge Toronto Queen St | MR-CLI-0036 | Trail Climbing 36 | 1 | 15 | 14 |
| Maple Ridge Waterloo Uptown | MR-BAC-0015 | Tundra Backpack 15 | 2 | 15 | 13 |
| Maple Ridge Ottawa Rideau | MR-TEN-0003 | Boreal Tent 3 | 2 | 14 | 12 |
| Maple Ridge Waterloo Uptown | MR-TEN-0001 | Canyon Tent 1 | 2 | 14 | 12 |
| Maple Ridge Waterloo Uptown | MR-SLE-0007 | Summit Sleeping Bag 7 | 3 | 15 | 12 |
| Maple Ridge Calgary Kensington | MR-COO-0031 | Boreal Cooking 31 | 3 | 13 | 10 |
| Maple Ridge Halifax Waterfront | MR-BAC-0015 | Tundra Backpack 15 | 2 | 12 | 10 |
| Maple Ridge Calgary Kensington | MR-SLE-0010 | Boreal Sleeping Bag 10 | 2 | 11 | 9 |
| Maple Ridge Waterloo Uptown | MR-FOO-0020 | Glacier Footwear 20 | 5 | 14 | 9 |
| Maple Ridge Waterloo Uptown | MR-TEN-0004 | Ridge Tent 4 | 0 | 9 | 9 |
| Maple Ridge Halifax Waterfront | MR-FOO-0020 | Glacier Footwear 20 | 7 | 15 | 8 |
| Maple Ridge Halifax Waterfront | MR-BAC-0011 | Tundra Backpack 11 | 3 | 11 | 8 |

## Pipeline Batches

| batch_id | window_start | window_end | status | finished_at |
|---|---|---|---|---|
| 2025-11-01 | 1970-01-01 00:00:00 | 2025-11-01 00:00:00 | failed | 2026-11-02 18:55:26.183492 |
| 2025-11-02 | 2025-11-01 00:00:00 | 2025-11-02 00:00:00 | succeeded | 2026-11-02 18:54:50.081348 |
| 2025-11-03 | 2025-11-02 00:00:00 | 2025-11-03 00:00:00 | succeeded | 2026-11-02 18:55:24.850849 |
| 2025-11-04 | 2025-11-03 00:00:00 | 2025-11-04 00:00:00 | succeeded | 2026-11-02 18:56:05.561040 |
