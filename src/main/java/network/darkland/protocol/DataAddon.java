package network.darkland.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import network.darkland.NexusApplication;
import network.darkland.model.DataModel;
import network.darkland.protocol.backup.annotations.DbDataModels;
import network.darkland.redis.RedisDataContainer;
import network.darkland.redis.RedisManager;
import network.darkland.util.JsonUtils;
import network.darkland.util.NexusJsonBuilder;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class DataAddon {

    protected static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private static final Logger LOGGER = Logger.getLogger(DataAddon.class.getName());

    private volatile String cachedIdFieldName = null;
    private volatile Class<?> cachedIdClass = null;
    private final Object idCacheLock = new Object();


    private volatile Field[] cachedAnnotatedFields = null;
    private final Object fieldCacheLock = new Object();

    private final ConcurrentHashMap<String, Object> keyLocks = new ConcurrentHashMap<>();

    public abstract boolean handleRequest(String source, RequestType type, NexusJsonDataContainer json);

    public abstract int addonId();

    public abstract String addonName();

    public abstract String cacheKeyHeaderTag();

    public abstract String getDatabase();

    public abstract String getCollection();

    public void loadIntoCache(Object key) {
        Class<?> expectedType = getIdClassName();
        if (expectedType == null || !expectedType.isInstance(key)) return;

        String keyTag = cacheKeyHeaderTag() + "_" + key;
        NexusApplication app = NexusApplication.getApplication();

        if (app.getDataContainer().getDataModelFromKey(keyTag).isPresent()) return;

        NexusJsonDataContainer triggerContainer = new NexusJsonDataContainer();
        triggerContainer.set(getIdFieldName(), key);
        getData(triggerContainer);

    }

    public void handleIncrementData(String source, NexusJsonDataContainer json) {
        NexusApplication.getApplication().getRedisManager().processTask(() -> {
            try {
                if (!json.containsKey("key") || !json.containsKey("field") || !json.containsKey("amount")) {
                    LOGGER.warning("[DataAddon/" + addonName() + "] INCREMENT_DATA: missing required fields (key/field/amount)");
                    return;
                }

                String field  = json.get("field", String.class);
                Number amount = json.get("amount", Number.class);
                Object keyValue = json.get("key", getIdClassName());

                if (field == null || amount == null || keyValue == null) {
                    LOGGER.warning("[DataAddon/" + addonName() + "] INCREMENT_DATA: null value detected");
                    return;
                }

                json.set(getIdFieldName(), keyValue);
                Object lock = keyLocks.computeIfAbsent(keyValue.toString(), k -> new Object());
                synchronized (lock) {
                    Optional<DataModel> dataModelOpt = getData(json);
                    if (dataModelOpt.isEmpty()) {
                        LOGGER.warning("[DataAddon/" + addonName() + "] INCREMENT_DATA: model not found, key=" + keyValue);
                        return;
                    }

                    DataModel dataModel = dataModelOpt.get();
                    JsonNode rootNode = MAPPER.readTree(dataModel.getValueJson());

                    if (!rootNode.has(field) || !rootNode.get(field).isNumber()) {
                        LOGGER.warning("[DataAddon/" + addonName() + "] INCREMENT_DATA: field missing or not a number -> " + field);
                        return;
                    }

                    ObjectNode updatedNode = (ObjectNode) rootNode;
                    JsonNode targetNode = rootNode.get(field);

                    if (targetNode.isIntegralNumber()) {
                        updatedNode.put(field, targetNode.asLong() + amount.longValue());
                    } else {
                        updatedNode.put(field, targetNode.asDouble() + amount.doubleValue());
                    }

                    String updatedJson = MAPPER.writeValueAsString(updatedNode);
                    dataModel.setValueJson(updatedJson);
                    NexusApplication.getApplication().getRedisManager().setData(dataModel.getKey(), updatedJson);
                }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] handleIncrementData error", e);
            }
        });
    }

    public void handleRemove(String source, NexusJsonDataContainer json) {
        NexusApplication.getApplication().getRedisManager().processTask(() -> {
            try {
                String idFieldName = getIdFieldName();
                if (idFieldName.isEmpty() || !json.containsKey(idFieldName)) return;
                if (!json.containsKey("all")) return;

                String specificId = json.get(idFieldName, getIdClassName()).toString();
                boolean allRemove = Boolean.TRUE.equals(json.get("all", Boolean.class));

                RedisDataContainer dataContainer = NexusApplication.getApplication().getDataContainer();
                Optional<DataModel> dataModel = getData(json);

                if (dataModel.isPresent()) {
                    dataContainer.removeModel(dataModel.get().getKey());
                    if (allRemove) {
                        NexusApplication.getApplication().getMongoManager().removeValue(this, specificId);
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] handleRemove error", e);
            }
        });
    }

    public void handleGet(String source, NexusJsonDataContainer json) {
        NexusApplication.getApplication().getRedisManager().processTask(() -> {
            try {
                Optional<DataModel> dataModel = getData(json);
                DataModel targetModel;

                if (dataModel.isEmpty()) {
                    String idFieldName = getIdFieldName();
                    if (idFieldName.isEmpty()) return;

                    NexusJsonDataContainer extract = json.containsKey("data")
                            ? new NexusJsonDataContainer(MAPPER.writeValueAsString(json.get("data", Object.class)))
                            : json;

                    Object idValue = extract.get(idFieldName, Object.class);
                    if (idValue == null) return;

                    targetModel = createModel(generateRawJson(idValue.toString()));
                    NexusApplication app = NexusApplication.getApplication();
                    app.getDataContainer().addModelDirect(targetModel.getKey(), targetModel);
                    app.getRedisManager().setData(targetModel.getKey(), targetModel.getValueJson());
                } else {
                    targetModel = dataModel.get();
                }

                ObjectNode rootNode = JsonUtils.getMapper().createObjectNode();
                rootNode.put("protocol", addonId());
                rootNode.put("source", "nexus");
                rootNode.put("type", "BROADCAST");
                rootNode.put("target", source);
                rootNode.set("data", MAPPER.readTree(targetModel.getValueJson()));

                NexusApplication.getApplication().getRedisManager()
                        .publish(RedisManager.CHANNEL + "_" + source, MAPPER.writeValueAsString(rootNode));

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] handleGet error", e);
            }
        });
    }

    public void handleSet(String source, NexusJsonDataContainer json) {
        NexusApplication.getApplication().getRedisManager().processTask(() -> {
            try {
                String rawInput = json.containsKey("data")
                        ? JsonUtils.toJson(json.get("data", Object.class))
                        : json.toFullJson();

                Optional<DataModel> dataModel = getData(json);

                if (dataModel.isEmpty()) {
                    DataModel newModel = createModel(modelInit(rawInput));
                    NexusApplication app = NexusApplication.getApplication();
                    app.getDataContainer().addModelDirect(newModel.getKey(), newModel);
                    app.getRedisManager().setData(newModel.getKey(), newModel.getValueJson());
                } else {
                    DataModel existing = dataModel.get();
                    String updated = modelInitComp(rawInput);
                    existing.setValueJson(updated);
                    NexusApplication.getApplication().getRedisManager().setData(existing.getKey(), updated);
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] handleSet error", e);
            }
        });
    }

    public String modelInit(String json) {
        ObjectNode outputNode = MAPPER.createObjectNode();
        try {
            JsonNode inputNode = MAPPER.readTree(json);
            for (Field field : getAnnotatedFields()) {
                String fieldName = field.getName();
                DbDataModels anno = field.getAnnotation(DbDataModels.class);

                Object targetValue = convertToType(anno.defaultValue(), field.getType());

                if (inputNode.has(fieldName) && !inputNode.get(fieldName).isNull()) {
                    targetValue = MAPPER.readerForUpdating(targetValue).readValue(inputNode.get(fieldName));
                }

                outputNode.set(fieldName, targetValue != null ? MAPPER.valueToTree(targetValue) : MAPPER.createObjectNode());
            }
            return MAPPER.writeValueAsString(outputNode);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] modelInit error, json=" + json, e);
            return "{}";
        }
    }

    public String modelInitComp(String json) {
        try {
            JsonNode rootNode = MAPPER.readTree(json);
            ObjectNode updatedNode = MAPPER.createObjectNode();

            for (Field field : getAnnotatedFields()) {
                String fieldName = field.getName();
                DbDataModels anno = field.getAnnotation(DbDataModels.class);
                Object baseObj = convertToType(anno.defaultValue(), field.getType());

                if (rootNode.has(fieldName) && !rootNode.get(fieldName).isNull()) {
                    baseObj = MAPPER.readerForUpdating(baseObj).readValue(rootNode.get(fieldName));
                }
                updatedNode.set(fieldName, MAPPER.valueToTree(baseObj));
            }
            return MAPPER.writeValueAsString(updatedNode);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "[DataAddon/" + addonName() + "] modelInitComp error, json=" + json, e);
            return json;
        }
    }

    private Field[] getAnnotatedFields() {
        if (cachedAnnotatedFields != null) return cachedAnnotatedFields;
        synchronized (fieldCacheLock) {
            if (cachedAnnotatedFields != null) return cachedAnnotatedFields;
            cachedAnnotatedFields = java.util.Arrays.stream(getClass().getDeclaredFields())
                    .filter(f -> f.isAnnotationPresent(DbDataModels.class))
                    .peek(f -> f.setAccessible(true))
                    .toArray(Field[]::new);
        }
        return cachedAnnotatedFields;
    }

    private Object convertToType(String value, Class<?> type) {
        boolean blank = value == null || value.isEmpty() || value.equals("{}");

        if (blank) {
            if (type == String.class)                           return "";
            if (type == int.class || type == Integer.class)     return 0;
            if (type == long.class || type == Long.class)       return 0L;
            if (type == double.class || type == Double.class)   return 0.0;
            if (type == boolean.class || type == Boolean.class) return false;
            try {
                return type.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                try { return MAPPER.readValue("{}", type); } catch (Exception ex) {
                    LOGGER.warning("[DataAddon/" + addonName() + "] convertToType: could not produce default for " + type.getSimpleName());
                    return null;
                }
            }
        }

        if (type == String.class)                           return value;
        if (type == int.class || type == Integer.class)     return Integer.parseInt(value);
        if (type == long.class || type == Long.class)       return Long.parseLong(value);
        if (type == double.class || type == Double.class)   return Double.parseDouble(value);
        if (type == boolean.class || type == Boolean.class) return Boolean.parseBoolean(value);

        try {
            return MAPPER.readValue(value, type);
        } catch (Exception e) {
            LOGGER.warning("[DataAddon/" + addonName() + "] convertToType parse error: " + type.getSimpleName() + " value=" + value);
            return null;
        }
    }

    public String getIdFieldName() {
        if (cachedIdFieldName != null) return cachedIdFieldName;
        synchronized (idCacheLock) {
            if (cachedIdFieldName != null) return cachedIdFieldName;
            for (Field f : getClass().getDeclaredFields()) {
                if (f.isAnnotationPresent(DbDataModels.class) && f.getAnnotation(DbDataModels.class).isId()) {
                    cachedIdFieldName = f.getName();
                    cachedIdClass = f.getType();
                    return cachedIdFieldName;
                }
            }
            cachedIdFieldName = "";
        }
        return cachedIdFieldName;
    }

    public Class<?> getIdClassName() {
        if (cachedIdClass != null) return cachedIdClass;
        getIdFieldName();
        return cachedIdClass;
    }

    public Optional<DataModel> getData(NexusJsonDataContainer dataContainer) {
        try {
            NexusJsonDataContainer work = dataContainer.containsKey("data")
                    ? new NexusJsonDataContainer(JsonUtils.toJson(dataContainer.get("data", Object.class)))
                    : dataContainer;

            String specificValue = getSpecificDbKeyFromJsonKeyToValue(work);
            if (specificValue.isEmpty() || "null".equals(specificValue)) return Optional.empty();

            String keyTag = cacheKeyHeaderTag() + "_" + specificValue;
            NexusApplication app = NexusApplication.getApplication();

            Optional<DataModel> l1 = app.getDataContainer().getDataModelFromKey(keyTag);
            if (l1.isPresent()) {
                return l1;
            }

            if (app.getRedisManager().exists(keyTag)) {
                String redisJson = app.getRedisManager().getData(keyTag).get();
                DataModel m = new DataModel(keyTag, UUID.randomUUID().toString(),
                        modelInitComp(redisJson), this, specificValue);
                app.getDataContainer().addModelFix(keyTag, m);
                return Optional.of(m);
            }

            String dbJson = app.getMongoManager().getValue(this, specificValue).join();
            if (dbJson != null) {
                DataModel m = new DataModel(keyTag, UUID.randomUUID().toString(),
                        modelInitComp(dbJson), this, specificValue);
                app.getDataContainer().addModel(keyTag, m);
                return Optional.of(m);
            }

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "[DataAddon/" + addonName() + "] getData error", e);
        }
        return Optional.empty();
    }

    public String getSpecificDbKeyFromJsonKeyToValue(NexusJsonDataContainer dataContainer) {
        String idName = getIdFieldName();
        if (idName.isEmpty()) return "";
        Object val = dataContainer.get(idName, Object.class);
        return val != null ? val.toString() : "";
    }

    public String generateRawJson(String idValue) {
        String idName = getIdFieldName();
        return idName.isEmpty() ? "{}" : modelInit(NexusJsonBuilder.create().add(idName, idValue).build());
    }

    public DataModel createModel(String json) throws JsonProcessingException {
        NexusJsonDataContainer container = new NexusJsonDataContainer(json);
        String specificKey = getSpecificDbKeyFromJsonKeyToValue(container);
        String keyTag = cacheKeyHeaderTag() + "_" + specificKey;
        return new DataModel(keyTag, UUID.randomUUID().toString(), json, this, specificKey);
    }

    public enum RequestType {
        SET_DATA, GET_DATA, UPDATE_DATA, REMOVE_DATA, BROADCAST, LOAD_CACHE, INCREMENT_DATA, LIVE
    }
}