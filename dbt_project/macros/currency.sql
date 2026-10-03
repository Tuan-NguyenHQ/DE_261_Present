-- ============================================================
--  Currency helpers
--  Tỷ giá lấy từ seed fx_rates (effective-dated) qua ASOF JOIN:
--    ASOF LEFT JOIN {{ ref('fx_rates') }} fx
--      ON x.currency = fx.currency AND x.<date> >= fx.valid_from
--  Tiền tệ không có trong fx_rates → NULL (KHÔNG mặc định coi là USD)
-- ============================================================

{% macro to_usd(amount_col, units_per_usd_col) -%}
    toDecimal64(round(toFloat64({{ amount_col }}) / nullIf(toFloat64({{ units_per_usd_col }}), 0), 2), 2)
{%- endmacro %}
