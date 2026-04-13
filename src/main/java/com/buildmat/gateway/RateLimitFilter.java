package com.buildmat.gateway;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple token-bucket rate limiter that runs entirely in memory — no Redis required.
 * Each unique client IP gets its own bucket: 10 requests/second sustained,
 * burst up to 30 before the bucket empties.
 *
 * This is per-instance only (not distributed across multiple gateway replicas).
 * Add Redis-backed RequestRateLimiter if you scale the gateway horizontally.
 */
@Component
public class RateLimitFilter implements GlobalFilter, Ordered {

    private static final int  BURST_CAPACITY   = 30;
    private static final long REFILL_RATE_MS   = 100; // +1 token every 100 ms = 10/sec

    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String ip = extractIp(exchange);
        TokenBucket bucket = buckets.computeIfAbsent(ip, k -> new TokenBucket());

        if (!bucket.tryConsume()) {
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            exchange.getResponse().getHeaders().add("Retry-After", "1");
            return exchange.getResponse().setComplete();
        }
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() { return -1; } // run before routing filters

    private String extractIp(ServerWebExchange exchange) {
        String xff = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) return xff.split(",")[0].trim();
        var addr = exchange.getRequest().getRemoteAddress();
        return addr != null ? addr.getAddress().getHostAddress() : "unknown";
    }

    // ── Token bucket ──────────────────────────────────────────────────────────

    static class TokenBucket {
        private final AtomicLong tokens    = new AtomicLong(BURST_CAPACITY);
        private volatile long    lastRefill = System.currentTimeMillis();

        synchronized boolean tryConsume() {
            refill();
            long current = tokens.get();
            if (current <= 0) return false;
            tokens.decrementAndGet();
            return true;
        }

        private void refill() {
            long now     = System.currentTimeMillis();
            long elapsed = now - lastRefill;
            long toAdd   = elapsed / REFILL_RATE_MS;
            if (toAdd > 0) {
                tokens.updateAndGet(t -> Math.min(BURST_CAPACITY, t + toAdd));
                lastRefill = now;
            }
        }
    }
}
