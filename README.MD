# ⬡ NEXUS CORE
### Advanced Asynchronous Data Orchestration Engine `v1.0`

> *"The central brain between your distributed Minecraft infrastructure and your persistent data layer."*

[![Java](https://img.shields.io/badge/Java-17+-blue?style=flat-square&logo=java)](https://www.java.com)
[![Redis](https://img.shields.io/badge/Redis-Message%20Broker-red?style=flat-square&logo=redis)](https://redis.io)
[![MongoDB](https://img.shields.io/badge/MongoDB-Persistence-green?style=flat-square&logo=mongodb)](https://www.mongodb.com)

---

## 📖 Table of Contents

- [What is Nexus Core?](#-what-is-nexus-core)
- [System Architecture](#-system-architecture)
- [Data Flow Diagram](#-data-flow-diagram)
- [Core Components](#-core-components)
- [Getting Started](#-getting-started)
- [Writing an Addon](#️-writing-an-addon)
- [DataAddon API Reference](#-dataaddon-api-reference)
- [Annotations Reference](#-annotations-reference)
- [Request Lifecycle](#-request-lifecycle)
- [Protocol & Packet Structure](#-protocol--packet-structure)
- [Redis Key Strategy](#-redis-key-strategy)
- [Dashboard & Monitoring](#-dashboard--monitoring)
- [Configuration](#️-configuration)
- [Best Practices](#-best-practices)

---

## 🧠 What is Nexus Core?

**Nexus Core** is a standalone Java application that acts as the **central orchestration layer** for a distributed Minecraft network. Instead of every Spigot server connecting directly to MongoDB and managing its own cache, all data operations are delegated to Nexus Core through a **Redis pub/sub message bus**.

This design solves three critical problems at scale:

| Problem | Without Nexus Core | With Nexus Core |
|---|---|---|
| **DB Connections** | Every server holds its own connection pool | Single optimized connection pool |
| **Cache Coherency** | Each server has its own stale cache | Centralized Redis cache, always consistent |
| **Data Logic** | Duplicated across every server's codebase | Defined once in a `DataAddon`, shared everywhere |

Think of it as a **microservice** purpose-built for Minecraft infrastructure — Nexus Core owns all data, and your Spigot servers are just thin clients.

---

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│                                                                 │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│   │  Spigot #1   │    │  Spigot #2   │    │  Spigot #3   │     │
│   │  (Lobby)     │    │  (PvP)       │    │  (Survival)  │     │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘     │
│          │                   │                   │             │
│          └───────────────────┼───────────────────┘             │
│                              │  Redis Pub/Sub                   │
│                    ┌─────────▼──────────┐                      │
│                    │    Redis Broker     │                      │
│                    │  (Message Bus)      │                      │
│                    └─────────┬──────────┘                      │
│                              │                                  │
│                    ┌─────────▼──────────┐                      │
│                    │   ⬡ NEXUS CORE ⬡   │  ← YOU ARE HERE     │
│                    │                    │                      │
│                    │  ┌──────────────┐  │                      │
│                    │  │ AddonRegistry│  │                      │
│                    │  │  Addon #100  │  │                      │
│                    │  │  Addon #200  │  │                      │
│                    │  │  Addon #500  │  │                      │
│                    │  └──────┬───────┘  │                      │
│                    └─────────┼──────────┘                      │
│                              │  CRUD Operations                 │
│                    ┌─────────▼──────────┐                      │
│                    │      MongoDB        │                      │
│                    │  ┌───────────────┐ │                      │
│                    │  │ nexus_core_db │ │                      │
│                    │  │  → players    │ │                      │
│                    │  │  → guilds     │ │                      │
│                    │  │  → economy    │ │                      │
│                    │  └───────────────┘ │                      │
│                    └────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow Diagram

Every interaction follows a strict **request → orchestrate → respond** lifecycle:

```
Spigot Server                Redis                  Nexus Core               MongoDB
     │                         │                        │                       │
     │  1. PUBLISH packet       │                        │                       │
     │  {                       │                        │                       │
     │    protocol: 500,        │                        │                       │
     │    source: "pvp-1",      │                        │                       │
     │    type: GET_DATA,       │                        │                       │
     │    data: {name:"Steve"}  │                        │                       │
     │  }                       │                        │                       │
     │─────────────────────────►│                        │                       │
     │                          │  2. Route to listener  │                       │
     │                          │───────────────────────►│                       │
     │                          │                        │  3. Lookup AddonRegistry
     │                          │                        │     protocol 500 → ExampleAddon
     │                          │                        │                       │
     │                          │                        │  4. handleRequest()   │
     │                          │                        │     pre-check / reject│
     │                          │                        │                       │
     │                          │                        │  5. Check Redis cache │
     │                          │                        │     key: "example_addon_Steve"
     │                          │                        │──── CACHE HIT? ──────►│
     │                          │                        │◄─── return cached ────│
     │                          │                        │                       │
     │                          │                        │  (CACHE MISS)         │
     │                          │                        │  6. MongoDB query     │
     │                          │                        │  db.players.findOne(  │
     │                          │                        │    {name: "Steve"}    │
     │                          │                        │  )                    │
     │                          │                        │──────────────────────►│
     │                          │                        │◄── Document ──────────│
     │                          │                        │                       │
     │                          │                        │  7. Write to cache    │
     │                          │  8. PUBLISH response   │                       │
     │                          │◄───────────────────────│                       │
     │  9. Receive response     │                        │                       │
     │◄─────────────────────────│                        │                       │
     │                          │                        │                       │
```

### Request Types

| Type | Description | MongoDB Operation | Cache |
|------|-------------|-------------------|-------|
| `GET` | Fetch a document by ID field | `findOne()` | Read-through |
| `SET` | Insert or update a document | `replaceOne(upsert)` | Write-through |
| `DELETE` | Remove a document by ID | `deleteOne()` | Invalidate |
| `GET_ALL` | Fetch all documents in collection | `find()` | Bypass |

---

## 🧩 Core Components

### `NexusApplication`
The main entry point and bootstrapper. Initializes the Redis connection, MongoDB client, and loads all registered `DataAddon` instances into the registry. Exposes `getDataSize()` and `getAddonSize()` for the live dashboard.

### `DataAddon`
The abstract base class that defines a **data module**. Each addon maps to exactly one MongoDB collection and one Redis key namespace. You extend this class to add new data types to the network.

### `ProtocolHandler`
A thread-safe `Map<Integer, DataAddon>` keyed on `addonId()`. When a packet arrives with `protocol: 500`, the registry looks up the corresponding addon in O(1) and delegates the operation.

### `NexusJsonDataContainer`
A thin wrapper around a JSON object that is passed into `handleRequest()`. Provides helper methods for reading typed values from the packet payload without null-pointer noise.

### Dashboard (`Main.java`)
A Swing-based live monitoring UI built entirely with Java2D — no external UI frameworks. Displays real-time JVM CPU, process RAM, cache object count, and addon count with animated donut charts and scrolling line graphs.

---

## 🚀 Getting Started

### Prerequisites

- Java 17+
- A running Redis instance (`redis-server`)
- A running MongoDB instance (`mongod`)

### Build & Run

```bash
# Clone the repository
git clone https://github.com/mustafabinguldev/nexus-core.git
cd nexus-core

# Build with Maven
mvn clean package -DskipTests

# Run the application
java -jar target/nexus-core-4.0.jar
```

The **Nexus Core Dashboard** will launch. Fill in your Redis host, MongoDB URI, and server port, then click **INITIALIZE NEXUS ENGINE**.

### Default Configuration

| Field | Default |
|---|---|
| Redis Host | `127.0.0.1` |
| MongoDB URI | `mongodb://localhost:27017` |
| Server Port | `8080` |

---

## 🛠️ Writing an Addon

An addon is the fundamental unit of data in Nexus Core. Each addon represents a single collection of documents, defines the schema via annotations, and optionally intercepts requests before they are processed.

### Step 1 — Extend `DataAddon`

```java
package network.darkland.addons;

import network.darkland.protocol.DataAddon;
import network.darkland.protocol.NexusJsonDataContainer;
import network.darkland.protocol.backup.annotations.DbDataModels;

public class PlayerStatsAddon extends DataAddon {

    // -------------------------------------------------------
    //  IDENTITY
    // -------------------------------------------------------

    @Override
    public int addonId() {
        return 100; // Must be globally unique across all addons
    }

    @Override
    public String addonName() {
        return "Player Stats";
    }

    // -------------------------------------------------------
    //  DATABASE TARGET
    // -------------------------------------------------------

    @Override
    public String getDatabase() {
        return "nexus_core_db";
    }

    @Override
    public String getCollection() {
        return "player_stats"; // db.player_stats
    }

    // -------------------------------------------------------
    //  CACHE NAMESPACE
    // -------------------------------------------------------

    @Override
    public String cacheKeyHeaderTag() {
        return "stats"; // Redis key: "stats:<value of @isId field>"
    }

    // -------------------------------------------------------
    //  DATA MODEL
    // -------------------------------------------------------

    @DbDataModels(isId = true)
    private String uuid; // Primary key — used for both DB lookup and cache key

    @DbDataModels(defaultValue = "0", isId = false)
    private int kills;

    @DbDataModels(defaultValue = "0", isId = false)
    private int deaths;

    @DbDataModels(defaultValue = "0", isId = false)
    private double balance;

    @DbDataModels(defaultValue = "false", isId = false)
    private boolean isPremium;

    // -------------------------------------------------------
    //  REQUEST INTERCEPTOR
    // -------------------------------------------------------

    @Override
    public boolean handleRequest(String source, RequestType requestType, NexusJsonDataContainer data) {
        // 'source' is the server ID that sent the packet, e.g. "pvp-1"
        // Return false to reject this request without processing it.

        if (requestType == RequestType.REMOVE_DATA) {
            // Example: only allow deletes from the "admin" server
            return source.equals("admin");
        }

        return true;
    }
}
```

### Step 2 — Register the Addon

In your `NexusApplication` constructor or initialization block, register the addon:

```java
// NexusApplication.java
NexusApplication.getInstance().getProtocolHandler().registerAddon(new PlayerStatsAddon());
```

That's it. Nexus Core will now automatically handle all `GET_DATA`, `SET_DATA`, `REMOVE_DATA` requests for protocol ID `100`, routing them to the `player_stats` MongoDB collection.

---

## 📚 DataAddon API Reference

```java
public abstract class DataAddon {

    /**
     * The unique numeric ID for this addon.
     * Remote clients send this as the `protocol` field in their packets.
     * Must be unique — duplicate IDs will throw at startup.
     */
    public abstract int addonId();

    /**
     * Human-readable name shown in the dashboard and log output.
     */
    public abstract String addonName();

    /**
     * The MongoDB database name this addon operates on.
     * Multiple addons can share the same database.
     */
    public abstract String getDatabase();

    /**
     * The MongoDB collection this addon reads and writes.
     * Each addon should have its own dedicated collection.
     */
    public abstract String getCollection();

    /**
     * The prefix for Redis cache keys managed by this addon.
     * Full key format: "{cacheKeyHeaderTag}:{idFieldValue}"
     * Example: "stats:550e8400-e29b-41d4-a716-446655440000"
     *
     * Keep this short and unique to avoid key collisions
     * between addons that share the same database.
     */
    public abstract String cacheKeyHeaderTag();

    /**
     * Called before every data operation.
     *
     * @param source      The server ID that originated the request (e.g. "pvp-1")
     * @param requestType GET / SET / DELETE / GET_ALL
     * @param data        The payload container from the incoming packet
     * @return true  → process the request normally
     *         false → reject and send an empty response back to source
     */
    public abstract boolean handleRequest(
        String source,
        RequestType requestType,
        NexusJsonDataContainer data
    );
}
```

---

## 🏷️ Annotations Reference

The `@DbDataModels` annotation is how Nexus Core discovers your schema at runtime via reflection. Every field you want persisted must be annotated.

```java
@DbDataModels(isId = true)
private String uuid;
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `isId` | `boolean` | ✅ Yes | Marks this field as the primary key. Used for MongoDB `findOne` queries and as the Redis cache key suffix. **Exactly one field per addon must have `isId = true`.** |
| `defaultValue` | `String` | ✅ Yes (if `isId = false`) | The value written to MongoDB when this field is not present in the incoming packet. Must be a parseable string representation of the field's type. |

### Supported Field Types

| Java Type | `defaultValue` example |
|-----------|------------------------|
| `String` | `"unknown"` |
| `int` / `Integer` | `"0"` |
| `long` / `Long` | `"0"` |
| `double` / `Double` | `"0.0"` |
| `boolean` / `Boolean` | `"false"` |

---

## 🔄 Request Lifecycle

Understanding the exact order of operations is important for writing correct addon logic.

```
Incoming Redis Packet
        │
        ▼
┌───────────────────┐
│  Deserialize JSON  │
│  Extract protocol  │
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│  AddonRegistry    │      ┌─────────────┐
│  lookup(protocol) │─────►│  Not found? │──► Log WARN, drop packet
└────────┬──────────┘      └─────────────┘
         │
         ▼
┌───────────────────┐
│  handleRequest()  │      ┌─────────────┐
│  (your code)      │─────►│  Returns    │──► Send empty response to source
└────────┬──────────┘      │  false?     │
         │                 └─────────────┘
         │ true
         ▼
┌───────────────────┐
│  Route by         │
│  RequestType      │
└────────┬──────────┘
         │
    ┌────┴────┬────────────┬──────────
    ▼         ▼            ▼          
  GET_DATA        SET_DATA        REMOVE_DATA    
    │         │            │          
    ▼         ▼            ▼          
 Check     Write to    Delete from  
 Redis     Mongo +     Mongo +      
 cache     Update      Invalidate   
    │       cache       cache
    ▼
 Cache hit?
    │
  No │
    ▼
 Query Mongo
 Write cache
    │
    ▼
Serialize response
Publish to Redis
(source channel)
```

---

## 📦 Protocol & Packet Structure

### Inbound Packet (Spigot → Redis → Nexus)

```json
{
  "protocol": 100,
  "source":   "pvp-1",
  "type":     "GET",
  "data": {
    "uuid": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

### Outbound Packet (Nexus → Redis → Spigot)

```json
{
  "protocol": 100,
  "source":   "nexus",
  "type":     "BROADCAST",
  "target": "pvp-1",
  "data": {
    "uuid":      "550e8400-e29b-41d4-a716-446655440000",
    "kills":     142,
    "deaths":    38,
    "balance":   2500.75,
    "isPremium": true
  }
}
```

---

## 🗝️ Redis Key Strategy

Nexus Core uses a simple, predictable key schema to prevent collisions between addons, databases, and environments:

```
{cacheKeyHeaderTag}_{idFieldValue}
```

### Examples

| Addon | `cacheKeyHeaderTag` | ID Field Value | Full Redis Key |
|-------|---------------------|----------------|----------------|
| `PlayerStatsAddon` | `stats` | `Steve` | `stats:Steve` |
| `GuildAddon` | `guild` | `Darkland` | `guild:Darkland` |
| `EconomyAddon` | `eco` | `550e8400...` | `eco:550e8400...` |

> ⚠️ **Warning:** Two addons with the same `cacheKeyHeaderTag` will overwrite each other's cache entries. Always use unique prefixes.

Cache entries are stored as serialized JSON strings and are automatically invalidated on `SET` and `DELETE` operations.

---

## 📊 Dashboard & Monitoring

The **Nexus Core Dashboard** (`Main.java`) provides real-time visibility into the running engine. All rendering is done with Java2D — no external libraries.
![Project Screenshot](src/main/resources/nexus-core.png)

### Dashboard Metrics

| Card | Source | Description |
|------|--------|-------------|
| **CPU LOAD** | `OperatingSystemMXBean.getProcessCpuLoad()` | JVM process CPU only — not system-wide |
| **PROCESS RAM** | `Runtime.totalMemory() - freeMemory()` | Current JVM heap usage in MB |
| **CACHE OBJECTS** | `NexusApplication.getDataSize()` | Number of documents currently in Redis cache |
| **ACTIVE ADDONS** | `NexusApplication.getAddonSize()` | Number of registered `DataAddon` instances |

---

## ⚙️ Configuration

Configuration is entered via the login panel at startup. There is intentionally no config file — all connection parameters are live and validated at boot time.

| Field | Example | Notes |
|-------|---------|-------|
| `REDIS HOST` | `127.0.0.1` | Hostname or IP of your Redis server. Port defaults to `6379`. |
| `MONGO URI` | `mongodb://user:pass@host:27017` | Full MongoDB connection string. Supports auth and replica sets. |
| `SERVER PORT` | `8080` | Port Nexus Core listens on for internal HTTP health checks. |

---

## ✅ Best Practices

**1. Keep addon IDs in a constants file**
```java
public final class AddonIds {
    public static final int PLAYER_STATS = 100;
    public static final int GUILDS       = 200;
    public static final int ECONOMY      = 300;
    public static final int EXAMPLE      = 500;
}
```

**2. Use UUIDs as ID fields whenever possible**
String names are not unique across time. A player rename will create a duplicate document. Prefer `uuid` as `isId = true`.

**3. Keep `handleRequest()` fast**
This method runs synchronously before the operation. Any blocking call here stalls the entire request. Do validation only — no I/O.

**4. Use distinct `cacheKeyHeaderTag` values**
Even across different databases, share a global convention like `{moduleName}_{collection}` to guarantee uniqueness:
```java
// ✅ Good
public String cacheKeyHeaderTag() { return "pvp_stats"; }

// ❌ Risky — collides with any other addon that uses "stats"
public String cacheKeyHeaderTag() { return "stats"; }
```

**5. Never call MongoDB directly from a Spigot plugin**
The entire point of Nexus Core is centralized data access. Bypassing it defeats cache coherency and defeats the architecture.

---

---

## 🛡️ License

**Mustafa Bingül — All Rights Reserved**

---

<div align="center">

**⬡ NEXUS CORE v1.0**

*Built for performance. Designed for scale.*

</div>
```