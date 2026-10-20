%dw 2.0
output application/java
import recordContext, correlationIdFor from dw::modules::Telemetry
---
recordContext(attributes) ++ {
    correlationId: correlationIdFor(attributes, payload, correlationId)
}
