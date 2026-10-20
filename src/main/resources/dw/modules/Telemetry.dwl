%dw 2.0
import isBlank from dw::core::Strings
import try from dw::Runtime
import errorTypeName, shortDescription from dw::modules::Errors

/**
 * Rated payload by truck model, from the manufacturers' specification sheets.
 */
var RATED_PAYLOAD_TONNES = {
    CAT_793F: 226.8,
    CAT_797F: 363.0,
    KOMATSU_930E: 290.0,
    LIEBHERR_T284: 363.0
}

var REQUIRED_TEXT = ["eventId", "eventType", "truckId", "model", "siteId", "occurredAt"]

var NUMERIC_RANGES = [
    { path: ["position", "lat"], min: -90, max: 90 },
    { path: ["position", "lon"], min: -180, max: 180 },
    { path: ["position", "speedKph"], min: 0, max: 120 },
    { path: ["engine", "coolantTempC"], min: -40, max: 200 },
    { path: ["engine", "oilPressureKpa"], min: 0, max: 2000 },
    { path: ["engine", "rpm"], min: 0, max: 4000 },
    { path: ["payloadTonnes"], min: 0, max: 500 },
    { path: ["fuelLevelPct"], min: 0, max: 100 }
]

/* ---------------------------------------------------------------------------------------
   Parsing and record metadata
   --------------------------------------------------------------------------------------- */

/**
 * The listener hands over raw bytes; a test or an upstream flow may already have typed the
 * payload as JSON. Either way the result is the parsed event.
 */
fun asEvent(value: Any): Any =
    if ((value is Binary) or (value is String)) read(value, "application/json") else value

/**
 * Kafka header values arrive as bytes.
 */
fun headerText(headers: Any, name: String): String | Null = do {
    var value = if (headers is Object) headers[name] else null
    ---
    if (value == null) null
    else if (value is String) value
    else read(value, "text/plain") as String
}

/**
 * The producer's x-correlation-id if it sent one, else the eventId, else the id Mule generated.
 */
fun correlationIdFor(attributes: Any, payload: Any, generated: String): String = do {
    var fromHeader = headerText(attributes.headers, "x-correlation-id")
    var fromEvent = try(() -> asEvent(payload).eventId)
    ---
    if (fromHeader != null) fromHeader
    else if (fromEvent.success and (fromEvent.result is String)) fromEvent.result
    else generated
}

fun recordContext(attributes: Any): Object = {
    topic: attributes.topic,
    partition: attributes.partition,
    offset: attributes.offset,
    key: attributes.key default null,
    commitKey: attributes.consumerCommitKey
}

/* ---------------------------------------------------------------------------------------
   Validation
   --------------------------------------------------------------------------------------- */

fun valueAt(event: Any, path: Array<String>): Any =
    path reduce ((key, node = event) -> if (node is Object) node[key] else null)

fun inRange(value: Any, min: Number, max: Number): Boolean =
    (value is Number) and (value >= min) and (value <= max)

fun isText(value: Any): Boolean =
    (value is String) and (not isBlank(value))

fun isTimestamp(value: Any): Boolean =
    (value is String) and try(() -> value as DateTime).success

fun tyreProblems(tyres: Any): Array<String> =
    if ((not (tyres is Array)) or isEmpty(tyres)) ["tyres must be a non-empty array"]
    else tyres
        filter ((tyre) -> (not inRange(tyre.pressureKpa, 0, 1500)) or (not inRange(tyre.tempC, -40, 200)))
        map ((tyre) -> "tyre " ++ (tyre.position default "?") ++ " has an invalid pressure or temperature")

/**
 * Every problem with the event, or an empty array if it is usable.
 */
fun validationErrors(event: Any): Array<String> =
    if (not (event is Object)) ["the event must be a JSON object"]
    else
        (REQUIRED_TEXT filter ((field) -> not isText(event[field])) map ((field) -> field ++ " is required"))
        ++ (if (isText(event.eventType) and (event.eventType != "TRUCK_TELEMETRY")) ["eventType must be TRUCK_TELEMETRY"] else [])
        ++ (if (isText(event.truckId) and (not (event.truckId matches /^HT-[0-9]{4}$/))) ["truckId must look like HT-0417"] else [])
        ++ (if (isText(event.occurredAt) and (not isTimestamp(event.occurredAt))) ["occurredAt must be an ISO-8601 timestamp with an offset"] else [])
        ++ (NUMERIC_RANGES
                filter ((range) -> not inRange(valueAt(event, range.path), range.min, range.max))
                map ((range) -> (range.path joinBy ".") ++ " must be a number from " ++ (range.min as String) ++ " to " ++ (range.max as String)))
        ++ tyreProblems(event.tyres)

/* ---------------------------------------------------------------------------------------
   Enrichment
   --------------------------------------------------------------------------------------- */

fun breach(code: String, severity: String, observed: Number, limit: Number, unit: String, message: String): Object = {
    code: code,
    severity: severity,
    observed: observed,
    limit: limit,
    unit: unit,
    message: message
}

fun payloadUtilisationPct(event: Object): Number | Null = do {
    var rated = RATED_PAYLOAD_TONNES[event.model]
    ---
    if (rated == null) null else round((event.payloadTonnes / rated) * 1000) / 10
}

/**
 * Threshold checks. limits holds the fleet.thresholds.* properties as numbers.
 */
fun breaches(event: Object, limits: Object, utilisation: Number | Null): Array<Object> =
    (if (event.engine.coolantTempC > limits.coolantTempMaxC)
        [breach("ENGINE_OVERHEAT", "CRITICAL", event.engine.coolantTempC, limits.coolantTempMaxC, "C", "Coolant temperature above limit")]
     else [])
    ++ (if ((event.engine.rpm > 500) and (event.engine.oilPressureKpa < limits.oilPressureMinKpa))
        [breach("LOW_OIL_PRESSURE", "CRITICAL", event.engine.oilPressureKpa, limits.oilPressureMinKpa, "kPa", "Oil pressure below limit with the engine running")]
     else [])
    ++ (event.tyres
        filter ((tyre) -> tyre.pressureKpa < limits.tyrePressureMinKpa)
        map ((tyre) -> breach("TYRE_PRESSURE_LOW", "WARNING", tyre.pressureKpa, limits.tyrePressureMinKpa, "kPa", "Tyre " ++ tyre.position ++ " pressure below limit")))
    ++ (event.tyres
        filter ((tyre) -> tyre.tempC > limits.tyreTempMaxC)
        map ((tyre) -> breach("TYRE_OVERHEAT", "WARNING", tyre.tempC, limits.tyreTempMaxC, "C", "Tyre " ++ tyre.position ++ " temperature above limit")))
    ++ (if ((utilisation != null) and (utilisation > limits.overloadPct))
        [breach("OVERLOAD", "WARNING", utilisation, limits.overloadPct, "%", "Payload above rated capacity")]
     else [])
    ++ (if (event.position.speedKph > limits.speedMaxKph)
        [breach("OVERSPEED", "WARNING", event.position.speedKph, limits.speedMaxKph, "km/h", "Speed above haul road limit")]
     else [])
    ++ (if (event.fuelLevelPct < limits.fuelMinPct)
        [breach("LOW_FUEL", "WARNING", event.fuelLevelPct, limits.fuelMinPct, "%", "Fuel below refuelling threshold")]
     else [])

fun overallSeverity(found: Array<Object>): String =
    if (not isEmpty(found filter ($.severity == "CRITICAL"))) "CRITICAL"
    else if (isEmpty(found)) "NORMAL"
    else "WARNING"

fun enrich(event: Object, limits: Object, receivedAt: String): Object = do {
    var utilisation = payloadUtilisationPct(event)
    var found = breaches(event, limits, utilisation)
    ---
    event ++ {
        receivedAt: receivedAt,
        ratedPayloadTonnes: RATED_PAYLOAD_TONNES[event.model] default null,
        payloadUtilisationPct: utilisation,
        breaches: found,
        severity: overallSeverity(found)
    }
}

/* ---------------------------------------------------------------------------------------
   Outbound messages
   --------------------------------------------------------------------------------------- */

fun toAlert(event: Object): Object = {
    alertId: event.eventId ++ ":alert",
    eventId: event.eventId,
    truckId: event.truckId,
    model: event.model,
    siteId: event.siteId,
    occurredAt: event.occurredAt,
    detectedAt: event.receivedAt,
    severity: event.severity,
    breaches: event.breaches,
    position: event.position
}

fun toMetric(event: Object): Object = {
    eventId: event.eventId,
    truckId: event.truckId,
    model: event.model,
    siteId: event.siteId,
    occurredAt: event.occurredAt,
    receivedAt: event.receivedAt,
    position: event.position,
    engine: event.engine,
    tyres: event.tyres,
    payloadTonnes: event.payloadTonnes,
    ratedPayloadTonnes: event.ratedPayloadTonnes,
    payloadUtilisationPct: event.payloadUtilisationPct,
    fuelLevelPct: event.fuelLevelPct
}

fun outboundHeaders(messageType: String, correlationId: String, record: Object): Object = {
    "x-correlation-id": correlationId,
    "x-message-type": messageType,
    "x-source-topic": record.topic as String,
    "x-source-partition": record.partition as String,
    "x-source-offset": record.offset as String
}

/**
 * Error metadata for a dead-lettered record: enough to find, understand and replay it.
 */
fun deadLetterHeaders(err: Any, attributes: Any, correlationId: String, consumerGroup: String): Object = {
    "x-error-type": errorTypeName(err),
    "x-error-description": shortDescription(err, 1000),
    "x-original-topic": attributes.topic as String,
    "x-original-partition": attributes.partition as String,
    "x-original-offset": attributes.offset as String,
    "x-consumer-group": consumerGroup,
    "x-failed-at": now() as String { format: "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" },
    "x-correlation-id": correlationId
}
