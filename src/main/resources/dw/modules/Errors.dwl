%dw 2.0

/**
 * "NAMESPACE:IDENTIFIER" for logs and headers.
 */
fun errorTypeName(err: Any): String =
    if (err == null) "UNKNOWN"
    else (err.errorType.namespace default "UNKNOWN") ++ ":" ++ (err.errorType.identifier default "UNKNOWN")

/**
 * Error description cut to a size that is safe to put in a Kafka header.
 */
fun shortDescription(err: Any, maxLength: Number): String = do {
    var text = (err.description default "") as String
    ---
    if (sizeOf(text) <= maxLength) text else text[0 to maxLength - 1]
}
