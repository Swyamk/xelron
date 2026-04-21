# Part 3: Prompt Preparation - Socket Grouping Architecture

## Overview

This document provides comprehensive preparation for implementing socket grouping functionality in aiokafka, based on PR #196. The prompt preparation includes repository context, PR description, acceptance criteria, edge cases, and a complete initial prompt for implementation.

---

## 3.1 Repository Context

**Repository Name:** aiokafka  
**Repository URL:** https://github.com/aio-libs/aiokafka  
**Language:** Python 93.1%  
**Purpose:** High-performance asynchronous Apache Kafka client library  

### What This Repository Does

Aiokafka is the authoritative Python library for asynchronous communication with Apache Kafka brokers. It provides two primary interfaces: 
- **AIOKafkaProducer** - For reliably sending messages to Kafka topics
- **AIOKafkaConsumer** - For consuming messages from topics with automatic group management

Unlike synchronous Python Kafka clients, aiokafka leverages Python's asyncio framework, enabling single-threaded concurrency models that are essential for modern microservices architectures. The library handles all Kafka protocol complexity internally, including broker discovery, metadata management, rebalancing, and connection pooling.

### Intended Users

1. **Microservice Developers** - Building event-driven architectures without thread management overhead
2. **Data Pipeline Engineers** - Creating ETL/streaming jobs with high throughput requirements
3. **DevOps/SRE Teams** - Operating Kafka infrastructure that Python services consume
4. **Financial Systems** - High-frequency trading or market data systems requiring low latency
5. **IoT Platforms** - Edge devices and gateways collecting telemetry streams

### Problem Domain

The library operates in the distributed systems space, specifically addressing:
- **Reliability** - Guaranteed message delivery with offset management
- **Performance** - Non-blocking I/O avoiding thread overhead
- **Scalability** - Handling thousands of concurrent consumers efficiently
- **Operability** - Transparent connection management, automatic rebalancing

The Kafka protocol itself is inherently request-response based: clients initiate connections to brokers and wait for responses. This PR addresses a fundamental constraint in how aiokafka manages these connections.

---

## 3.2 Pull Request Description

### Current Implementation Problem

The existing aiokafka architecture maintains a simple one-socket-per-broker model:

```
Client -> Broker 1 <- Single Socket Connection
       -> Broker 2 <- Single Socket Connection
       -> Broker N <- Single Socket Connection
```

This creates a critical bottleneck: When a consumer fetches messages, the fetch request can block for up to 30 seconds (by default) waiting for messages to accumulate or the timeout to expire. During this blocking period, the single socket to that broker cannot accept or send other requests.

**Specific Problem Scenario:**
1. Consumer calls `consumer.getmany()` on Broker 1
2. Fetch request waits 500ms for messages to arrive
3. Consumer Group Coordinator heartbeat becomes due (normally every 3 seconds)
4. Heartbeat request queues behind the fetch request, waiting 500ms
5. Broker-side coordinator never receives heartbeat in time
6. Broker considers consumer dead and rebalances the consumer group
7. Rebalancing causes processing interruption and message loss/reprocessing

**Root Cause Analysis:**
The socket is a limited resource. Kafka protocol is synchronous per-connection, meaning only one request can be in-flight at a time. By forcing all request types through a single socket, high-latency operations (fetch) block critical operations (coordination).

### Proposed Solution

Introduce socket groups, creating separate socket pools for different request categories:

```
Client -> NORMAL_GROUP
        -> Broker 1 <- Socket for fetch, metadata, produce
        -> Broker 2 <- Socket for fetch, metadata, produce
        
       -> COORDINATION_GROUP  
        -> Broker 1 <- Socket exclusively for group coordination
        -> Broker 2 <- Socket exclusively for group coordination
```

Now coordinator operations have their own dedicated path, eliminating contention with fetch operations.

### Previous Behavior

**Before Implementation:**
- Single socket per broker, shared by all request types
- Fetch latency directly impacts group coordinator responsiveness
- Consumer groups could rebalance unexpectedly under fetch load
- No configuration or control over socket resource allocation

### New Behavior

**After Implementation:**
- Multiple socket pools per broker, one per socket group
- Coordinator operations use dedicated socket group (COORDINATION)
- Fetch operations use standard operations socket group
- Producer operations use standard operations socket group
- Each socket group has independent connection state and reuse logic

### Behavioral Changes from User Perspective

For library users, the change is **entirely transparent**:
- No API changes (all socket group handling is internal)
- Consumer and Producer classes work identically
- No configuration changes required
- Performance improves silently (no code changes needed to benefit)

### Internal Architectural Changes

For library maintainers:
- Client class gains socket group concept
- Connection pooling keyed by (broker_id, group_id) instead of just broker_id
- Group coordinator explicitly specifies COORDINATION group when acquiring sockets
- Connection lifecycle management must handle multiple pools per broker

---

## 3.3 Acceptance Criteria

These criteria define what successful implementation must achieve:

### Functional Requirements

**When a new socket group concept is introduced, the client must support acquiring sockets from different groups**
- The client connection pool should maintain separate socket pools indexed by (broker_id, group_id)
- Requests must be able to specify which socket group they need
- Multiple socket groups to the same broker must coexist independently

**When group coordination operations occur, they must use a dedicated COORDINATION socket group**
- Group coordinator initialization must explicitly request COORDINATION group
- Heartbeat requests must travel through COORDINATION sockets
- Offset commit requests must travel through COORDINATION sockets
- Consumer group rebalancing must not be blocked by fetch operations

**When one socket group experiences connection failure, other groups to the same broker must remain operational**
- Socket pool for GROUP A should be independent of socket pool for GROUP B
- Closing/reconnecting a coordination socket should not affect fetch socket state
- Broker failures should only affect the appropriate socket group

**When client connections are cleaned up, all socket groups must be properly closed**
- Client.close() must close sockets from all groups
- Resource cleanup must iterate all socket groups
- No socket leaks across different groups

### Performance Requirements

**The implementation should maintain backward compatibility and not degrade existing performance**
- Throughput for fetch operations should not decrease (parallel coordination socket eliminates contention)
- Memory overhead should be minimal (one additional socket per coordination group)
- CPU overhead should be negligible (indexing by tuple instead of just broker_id)

**Coordinator operations should complete faster when heavy fetching is in progress**
- Heartbeat latency should not increase with fetch request length
- Offset commit operations should not queue behind fetch requests

### Code Quality Requirements

**The implementation must maintain existing test coverage (>96%)**
- All new code paths must be tested
- Existing consumer/producer tests must continue passing
- Socket group test coverage should be comprehensive

**Backward compatibility must be preserved**
- Existing code not specifying socket groups should work as-is
- Default socket group behavior must match old single-socket behavior
- Internal implementation change, zero external API impact

---

## 3.4 Edge Cases

These scenarios require special handling:

### Edge Case 1: Broker Connectivity Loss

**Scenario:** Connection to Broker 1's COORDINATION socket fails while NORMAL socket is still healthy.

**Required Behavior:**
- Coordinator must detect the connection failure
- Coordinator should reconnect through the COORDINATION socket pool
- Consumer can continue fetching through the NORMAL socket pool
- No cascading failures across socket groups

**Implementation Consideration:** Each socket pool must have independent reconnection logic.

### Edge Case 2: Rapid Rebalancing

**Scenario:** Consumer group experiences back-to-back rebalancing events while fetch requests are in flight.

**Required Behavior:**
- Rebalance messages must reach coordinator despite ongoing fetch requests
- Socket group isolation ensures rebalance messages are not queued behind fetch
- Rebalancing should complete quickly
- Consumer should resume fetching after rebalance

**Implementation Consideration:** Coordination socket pool priority should be independent of fetch socket load.

### Edge Case 3: Multiple Socket Groups to Same Broker

**Scenario:** Code attempts to create additional socket groups beyond NORMAL and COORDINATION.

**Required Behavior:**
- System should support arbitrary socket group definitions  
- New socket pools should be created and managed identically to existing groups
- No hardcoded assumptions about specific group names
- Clean API for defining socket groups

**Implementation Consideration:** Socket group should be a flexible enum/class, not hardcoded strings.

### Edge Case 4: Socket Exhaustion

**Scenario:** Server runs out of file descriptors and cannot open new sockets.

**Required Behavior:**
- Both socket groups should fail gracefully
- Errors should be raised to the caller, not silently suppressed
- Partial socket pool state should not cause permanent resource leaks
- Client should be able to retry or shutdown cleanly

**Implementation Consideration:** Exception handling must account for multiple socket pools when reporting connection errors.

### Edge Case 5: Mixed Kafka Broker Versions

**Scenario:** Consumer connects to Kafka cluster with older brokers that don't support certain features requiring COORDINATION socket.

**Required Behavior:**
- COORDINATION socket should still work (uses standard Kafka protocol)
- Older brokers should handle requests from either socket group identically
- Protocol version negotiation per broker should work with multiple socket groups
- No special version-specific socket group handling

**Implementation Consideration:** Feature compatibility should be per-broker, not per-socket-group.

### Edge Case 6: Connection Pooling and Reuse

**Scenario:** Socket for a particular (broker_id, COORDINATION) pair is reused for multiple client instances.

**Required Behavior:**
- Socket reuse should be safe within same socket group
- Concurrent requests from different clients should not corrupt protocol state
- Socket lifecycle should be managed at the pool level, not individual request level

**Implementation Consideration:** Connection pooling strategy must account for multiple pools with concurrent access.

---

## 3.5 Initial Prompt

Below is the comprehensive prompt that would be provided to a ML model or developer implementation assistant:

---

### Implementation Prompt

**Task:** Implement socket grouping functionality in aiokafka to separate consumer fetch operations from group coordination operations, improving consumer group stability under high fetch loads.

**Context:**

You are implementing a feature for aiokafka, an asyncio-based Apache Kafka client library. The feature solves a real production issue: when consumers fetch messages with long timeouts (100-500ms), group coordinator heartbeats cannot be sent because they queue behind fetch requests on the same socket. This can cause unexpected consumer group rebalancing.

**Current Architecture:**

```python
# Current single-socket model
class AIOKafkaClient:
    def __init__(self, bootstrap_servers):
        self._conns = {}  # Maps broker_id -> socket
```

**Proposed Architecture:**

```python
# New multi-socket-group model
class SocketGroup(enum.Enum):
    NORMAL = 'normal'
    COORDINATION = 'coordination'

class AIOKafkaClient:
    def __init__(self, bootstrap_servers):
        self._conns = {}  # Maps (broker_id, group_name) -> socket
```

**Requirements:**

1. **Socket Group Concept**
   - Define a SocketGroup enum with at least NORMAL and COORDINATION values
   - Modify the client connection pool to use (broker_id, group_id) as key
   - Implement get_socket(broker_id, group=SocketGroup.NORMAL) method

2. **Coordinator Integration**
   - Update GroupCoordinator class to use SocketGroup.COORDINATION explicitly
   - Ensure heartbeat() and offset_commit() requests use coordination sockets
   - Keep default behavior for other components unchanged

3. **Connection Management**
   - Closing a client must close sockets from all groups
   - Reconnection logic must work independently per socket group
   - Broker failure in one group should not affect other groups

4. **Backward Compatibility**
   - Existing code not specifying socket groups should default to NORMAL
   - No changes to public API
   - Consumer and Producer classes should require zero code changes

5. **Testing**
   - Add unit tests for multiple socket groups to same broker
   - Verify group coordinator uses coordination sockets
   - Ensure total test coverage remains above 96%
   - Test scenario: fetch blocks while heartbeat succeeds

**Acceptance Criteria (must all pass):**

- [ ] Can acquire sockets from different groups for the same broker
- [ ] Coordinator heartbeats use COORDINATION group sockets
- [ ] Coordinator operations don't block on fetch request timeouts
- [ ] Connection failure in one group doesn't affect other groups
- [ ] All existing tests pass
- [ ] New test coverage >= 96%
- [ ] No public API changes
- [ ] No breaking changes to behavior

**Edge Cases to Handle:**

1. Concurrency: Multiple concurrent requests to different socket groups
2. Reconnection: Failed COORDINATION socket should reconnect independently
3. Cleanup: Client.close() must clean up all socket groups
4. Errors: FileDescriptorError when no sockets available (both groups fail safely)

**Performance Expectations:**

- No regression in fetch throughput
- Coordinator latency should decrease when fetching heavily
- Memory overhead should be <10MB per additional socket group

**Implementation Hints:**

- Consider how socket reuse/cleanup changes with multiple pools
- Group coordinator may need access to socket group enum
- Connection state machine must work per-group
- Error handling should report failures per socket group

**Testing Strategy:**

1. Unit tests for socket group pool management
2. Integration test: verify heartbeat timeout doesn't increase with fetch timeout
3. Stress test: many concurrent requests to different groups
4. Explicit test case: fetcher blocks while coordinator succeeds

**Success Definition:**

A successful implementation allows consumer groups to remain healthy even when fetch operations are blocked due to long timeouts, by ensuring group coordination uses a separate socket path with no contention.

---

---

## Summary

This prompt preparation document provides the essential context for understanding and implementing PR #196's socket grouping feature. It establishes:

1. **Problem Context** - Why single sockets per broker are insufficient in production
2. **Solution Design** - How socket groups solve the contention issue
3. **Acceptance Criteria** - What constitutes successful implementation
4. **Edge Cases** - Scenarios requiring special handling  
5. **Complete Prompt** - Ready for implementation by any experienced developer

The documentation is written in accessible technical language suitable for developer handoff while maintaining precision about requirements and expected behavior.
