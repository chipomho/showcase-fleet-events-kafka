%dw 2.0
output application/java
import enrich from dw::modules::Telemetry

var limits = {
    coolantTempMaxC: p('fleet.thresholds.coolantTempMaxC') as Number,
    oilPressureMinKpa: p('fleet.thresholds.oilPressureMinKpa') as Number,
    tyrePressureMinKpa: p('fleet.thresholds.tyrePressureMinKpa') as Number,
    tyreTempMaxC: p('fleet.thresholds.tyreTempMaxC') as Number,
    overloadPct: p('fleet.thresholds.overloadPct') as Number,
    speedMaxKph: p('fleet.thresholds.speedMaxKph') as Number,
    fuelMinPct: p('fleet.thresholds.fuelMinPct') as Number
}
---
enrich(payload, limits, now() as String { format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" })
