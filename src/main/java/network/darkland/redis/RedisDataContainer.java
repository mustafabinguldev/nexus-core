package network.darkland.redis;

import net.bytebuddy.dynamic.Nexus;
import network.darkland.NexusApplication;
import network.darkland.model.DataModel;
import network.darkland.protocol.NexusJsonDataContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ════════════════════════════════════════════════════════════
 *  Veri Senkronizasyon Merkezi — RedisDataContainer
 * ════════════════════════════════════════════════════════════
 *
 * Katman hiyerarşisi (üstten alta öncelik):
 *
 *   ┌─────────────────────────────────┐
 *   │        Redis Cache (MASTER)     │  ← Tek doğru kaynak
 *   └────────────┬──────────┬─────────┘
 *                │          │
 *         pull 10s       flush 15s
 *                │          │
 *   ┌────────────▼──┐  ┌────▼──────────────┐
 *   │  L1 Cache     │  │     MongoDB        │
 *   │  (in-memory)  │  │  (persistent DB)   │
 *   └───────────────┘  └────────────────────┘
 *
 * Görev zamanlamaları:
 *   L1 Sync        →  10s   Redis değiştiyse L1'i günceller
 *   Auto Flush     →  15s   dirty key'leri Redis'ten Mongo'ya yazar
 *   Reconciliation →   3dk  Mongo ≠ Redis ise Redis doğruyu yazar
 *
 * Temel garantiler:
 *   • Redis her zaman master'dır; hiçbir görev Redis'i dışarıdan ezamaz.
 *   • dirtyKeys.remove(key) Mongo yazımı başarıyla tamamlandıktan SONRA yapılır.
 *     Hata durumunda key dirty listede kalır; bir sonraki flush'ta tekrar denenir.
 *   • removeModel() atomik — TOCTOU race yoktur.
 *   • Reconciliation Mongo'ya batch atarak 1000+ veride istek patlaması yaratmaz.
 *   • getDataModelFromId() O(1) — reverse index ile arama yapılır.
 * ════════════════════════════════════════════════════════════
 */
public class RedisDataContainer {

    private static final Logger LOGGER = Logger.getLogger(RedisDataContainer.class.getName());

    private static final int RECONCILE_BATCH_SIZE = 50;

    private final ConcurrentHashMap<String, DataModel> keyToModel;

    private final ConcurrentHashMap<String, String> idToKey;

    private final Set<String> dirtyKeys;

    public RedisDataContainer() {
        this.keyToModel = new ConcurrentHashMap<>();
        this.idToKey    = new ConcurrentHashMap<>();
        this.dirtyKeys  = ConcurrentHashMap.newKeySet();

        RedisManager rm = NexusApplication.getApplication().getRedisManager();
        rm.scheduleTask(this::startL1SyncTask,         10, 10, TimeUnit.SECONDS);
        rm.scheduleTask(this::startAutoFlushTask,      15, 15, TimeUnit.SECONDS);
        rm.scheduleTask(this::startReconciliationTask,  3,  3, TimeUnit.MINUTES);

        rm.scheduleTask(this::sendNetworkLiveBroadcast, 1, 1, TimeUnit.SECONDS);

    }

    private void sendNetworkLiveBroadcast() {

        NexusApplication.getApplication().getRedisManager().processTask(() -> {
            NexusJsonDataContainer jsonDataContainer = new NexusJsonDataContainer();
            jsonDataContainer.set("type","LIVE");
            jsonDataContainer.set("source", "nexus");
            jsonDataContainer.set("time", System.currentTimeMillis() / 1000L);


            NexusApplication.getApplication().getRedisManager().publish("darkland_nexus_live", jsonDataContainer.toFullJson());
        });

    }


    private void startL1SyncTask() {
        if (keyToModel.isEmpty()) return;

        RedisManager rm = NexusApplication.getApplication().getRedisManager();

        rm.processTask(() -> keyToModel.forEach((key, model) -> {
            if (dirtyKeys.contains(key)) return;

            Optional<String> redisOpt = rm.getData(key);

            if (redisOpt.isPresent()) {
                String redisJson = redisOpt.get();
                if (!redisJson.equals(model.getValueJson())) {
                    model.setValueJson(redisJson);
                }
            } else {
                LOGGER.warning("[L1Sync] Redis key kayıp, restore ediliyor: " + key);
                rm.setData(key, model.getValueJson());
                dirtyKeys.add(key);
            }
        }));
    }

    private void startAutoFlushTask() {
        if (dirtyKeys.isEmpty()) return;

        List<String> keysToFlush = new ArrayList<>(dirtyKeys);

        RedisManager rm = NexusApplication.getApplication().getRedisManager();

        rm.processTask(() -> {
            for (String key : keysToFlush) {
                DataModel model = keyToModel.get(key);
                if (model == null) {
                    dirtyKeys.remove(key);
                    continue;
                }

                String jsonToWrite = rm.getData(key).orElseGet(model::getValueJson);

                try {
                    NexusApplication.getApplication().getMongoManager()
                            .setValue(model.getAddon(), model.getSpecificDbKey(), jsonToWrite)
                            .get();

                    dirtyKeys.remove(key);

                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE,
                            "[AutoFlush] Mongo yazımı başarısız, tekrar denenecek: " + key, e);
                }
            }
        });
    }

    private void startReconciliationTask() {
        if (keyToModel.isEmpty()) return;

        RedisManager rm = NexusApplication.getApplication().getRedisManager();

        rm.processTask(() -> {
            List<String> keys  = new ArrayList<>(keyToModel.keySet());
            List<CompletableFuture<?>> batch = new ArrayList<>(RECONCILE_BATCH_SIZE);

            for (String key : keys) {
                // dirty key → flush henüz tamamlanmadı; dokunma
                if (dirtyKeys.contains(key)) continue;

                DataModel model = keyToModel.get(key);
                if (model == null) continue;

                String redisJson = rm.getData(key).orElseGet(model::getValueJson);

                CompletableFuture<?> future = NexusApplication.getApplication()
                        .getMongoManager()
                        .getValue(model.getAddon(), model.getSpecificDbKey())
                        .thenAccept(dbJson -> {
                            if (dbJson == null) {
                                // Mongo'da kayıt yok → Redis'tekini yaz
                                NexusApplication.getApplication().getMongoManager()
                                        .setValue(model.getAddon(), model.getSpecificDbKey(), redisJson);
                                return;
                            }
                            try {
                                String cleanDbJson = model.getAddon().modelInitComp(dbJson);
                                if (!cleanDbJson.equals(redisJson)) {
                                    NexusApplication.getApplication().getMongoManager()
                                            .setValue(model.getAddon(), model.getSpecificDbKey(), redisJson);
                                    model.setValueJson(redisJson);
                                }
                            } catch (Exception e) {
                                LOGGER.log(Level.WARNING,
                                        "[Reconciliation] modelInitComp hatası: " + key, e);
                            }
                        })
                        .exceptionally(ex -> {
                            LOGGER.log(Level.WARNING,
                                    "[Reconciliation] Mongo okuma hatası: " + key, ex);
                            return null;
                        });

                batch.add(future);

                if (batch.size() >= RECONCILE_BATCH_SIZE) {
                    waitForBatch(batch);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) waitForBatch(batch);
        });
    }

    private void waitForBatch(List<CompletableFuture<?>> batch) {
        try {
            CompletableFuture.allOf(batch.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[Reconciliation] Batch bekleme hatası", e);
        }
    }

    public void addModel(String key, DataModel model) {
        writeToL1AndRedis(key, model);
        dirtyKeys.add(key);
    }

    public void addModelFix(String key, DataModel model) {
        writeToL1AndRedis(key, model);
        dirtyKeys.add(key);
    }

    public void addModelDirect(String key, DataModel model) {
        writeToL1AndRedis(key, model);
        NexusApplication.getApplication().getRedisManager().processTask(() -> {
            try {
                NexusApplication.getApplication().getMongoManager()
                        .setValue(model.getAddon(), model.getSpecificDbKey(), model.getValueJson())
                        .get();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE,
                        "[addModelDirect] Mongo yazımı başarısız: " + key, e);
            }
        });
    }

    public void removeModel(String key) {
        DataModel removed = keyToModel.remove(key);
        if (removed == null) return;  // zaten yoktu

        idToKey.remove(removed.getId());
        dirtyKeys.remove(key);
        NexusApplication.getApplication().getRedisManager().deleteData(key);
    }

    public Optional<DataModel> getDataModelFromId(String id) {
        String key = idToKey.get(id);
        if (key == null) return Optional.empty();
        return Optional.ofNullable(keyToModel.get(key));
    }

    public Optional<DataModel> getDataModelFromKey(String key) {
        return Optional.ofNullable(keyToModel.get(key));
    }

    public Set<String> getDirtyKeys() { return dirtyKeys; }
    public int getDataSize()          { return keyToModel.size(); }

    private void writeToL1AndRedis(String key, DataModel model) {
        keyToModel.put(key, model);
        idToKey.put(model.getId(), key);
        NexusApplication.getApplication().getRedisManager().setData(key, model.getValueJson());
    }
}