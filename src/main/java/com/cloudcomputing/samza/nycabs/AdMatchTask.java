package com.cloudcomputing.samza.nycabs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.io.Resources;

/**
 * Consumes the stream of events. Outputs a stream which handles static file and
 * one stream and gives a stream of advertisement matches.
 */
public class AdMatchTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
     */
    private KeyValueStore<Integer, Map<String, Object>> userInfo;

    private KeyValueStore<String, Map<String, Object>> yelpInfo;

    private Set<String> lowCalories;

    private Set<String> energyProviders;

    private Set<String> willingTour;

    private Set<String> stressRelease;

    private Set<String> happyChoice;

    private void initSets() {
        lowCalories = new HashSet<>(Arrays.asList("seafood", "vegetarian", "vegan", "sushi"));
        energyProviders = new HashSet<>(Arrays.asList("bakeries", "ramen", "donuts", "burgers",
                "bagels", "pizza", "sandwiches", "icecream",
                "desserts", "bbq", "dimsum", "steak"));
        willingTour = new HashSet<>(Arrays.asList("parks", "museums", "newamerican", "landmarks"));
        stressRelease = new HashSet<>(Arrays.asList("coffee", "bars", "wine_bars", "cocktailbars", "lounges"));
        happyChoice = new HashSet<>(Arrays.asList("italian", "thai", "cuban", "japanese", "mideastern",
                "cajun", "tapas", "breakfast_brunch", "korean", "mediterranean",
                "vietnamese", "indpak", "southern", "latin", "greek", "mexican",
                "asianfusion", "spanish", "chinese"));
    }

    // Get store tag
    private String getTag(String cate) {
        String tag = "";
        if (happyChoice.contains(cate)) {
            tag = "happyChoice";
        } else if (stressRelease.contains(cate)) {
            tag = "stressRelease";
        } else if (willingTour.contains(cate)) {
            tag = "willingTour";
        } else if (energyProviders.contains(cate)) {
            tag = "energyProviders";
        } else if (lowCalories.contains(cate)) {
            tag = "lowCalories";
        } else {
            tag = "others";
        }
        return tag;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize kv store

        userInfo = (KeyValueStore<Integer, Map<String, Object>>) context.getTaskContext().getStore("user-info");
        yelpInfo = (KeyValueStore<String, Map<String, Object>>) context.getTaskContext().getStore("yelp-info");

        //Initialize store tags set
        initSets();

        //Initialize static data and save them in kv store
        initialize("UserInfoData.json", "NYCstore.json");
    }

    /**
     * This function will read the static data from resources folder and save
     * data in KV store.
     * <p>
     * This is just an example, feel free to change them.
     */
    public void initialize(String userInfoFile, String businessFile) {
        List<String> userInfoRawString = AdMatchConfig.readFile(userInfoFile);
        System.out.println("Reading user info file from " + Resources.getResource(userInfoFile).toString());
        System.out.println("UserInfo raw string size: " + userInfoRawString.size());
        for (String rawString : userInfoRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                int userId = (Integer) mapResult.get("userId");
                userInfo.put(userId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse user info :" + rawString);
            }
        }

        List<String> businessRawString = AdMatchConfig.readFile(businessFile);

        System.out.println("Reading store info file from " + Resources.getResource(businessFile).toString());
        System.out.println("Store raw string size: " + businessRawString.size());

        for (String rawString : businessRawString) {
            Map<String, Object> mapResult;
            ObjectMapper mapper = new ObjectMapper();
            try {
                mapResult = mapper.readValue(rawString, HashMap.class);
                String storeId = (String) mapResult.get("storeId");
                String cate = (String) mapResult.get("categories");
                String tag = getTag(cate);
                mapResult.put("tag", tag);
                yelpInfo.put(storeId, mapResult);
            } catch (Exception e) {
                System.out.println("Failed at parse store info :" + rawString);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
         */
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        System.out.println("Message class type: " + message.getClass().getName());
        Object userIdObj = message.get("userId");
        System.out.println("User ID class type: " + userIdObj.getClass().getName());
        
        int userId = (Integer) userIdObj;
        if (incomingStream.equals(AdMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            String eventType = (String) message.get("type");

            switch (eventType) {
                case "RIDER_STATUS":
                    processRiderStatus(userId, message);
                    break;
                case "RIDER_INTEREST":
                    processRiderInterest(userId, message);
                    break;
                case "RIDE_REQUEST":
                    processRideRequest(userId, collector);
                    break;
                default:
                    throw new IllegalStateException("Unexpected event type: " + eventType);
            }

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private void processRiderStatus(int userId, Map<String, Object> message) {
        // Update user's mood, blood_sugar, stress, active in userInfo store
        Map<String, Object> user = userInfo.get(userId);
        if (user != null) {
            user.put("mood", message.get("mood"));
            user.put("blood_sugar", message.get("blood_sugar"));
            user.put("stress", message.get("stress"));
            user.put("active", message.get("active"));
            userInfo.put(userId, user);
        }
    }

    private void processRiderInterest(int userId, Map<String, Object> message) {
        // Update user's interest if duration > 5 minutes
        int duration = (Integer) message.get("duration");
        if (duration > 300000) { // 5 minutes in milliseconds
            Map<String, Object> user = userInfo.get(userId);
            if (user != null) {
                user.put("interest", message.get("interest"));
                userInfo.put(userId, user);
            }
        }
    }

    private void processRideRequest(int userId, MessageCollector collector) {
        // Calculate ad-match score and send to ad-stream
        Map<String, Object> user = userInfo.get(userId);
        if (user != null) {
            String bestStoreId = null;
            String bestStoreName = null;
            double highestScore = 0;

            // Use a KeyValueIterator to iterate over the yelpInfo store
            KeyValueIterator<String, Map<String, Object>> iterator = yelpInfo.all();
            try {
                while (iterator.hasNext()) {
                    org.apache.samza.storage.kv.Entry<String, Map<String, Object>> entry = iterator.next();
                    String storeId = entry.getKey();
                    Map<String, Object> store = entry.getValue();
                    double score = calculateMatchScore(user, store);

                    if (score > highestScore) {
                        highestScore = score;
                        bestStoreId = storeId;
                        bestStoreName = (String) store.get("name");
                    }
                }
            } finally {
                iterator.close(); // Ensure the iterator is closed to release resources
            }

            if (bestStoreId != null) {
                Map<String, Object> adMessage = new HashMap<>();
                adMessage.put("userId", userId);
                adMessage.put("storeId", bestStoreId);
                adMessage.put("name", bestStoreName);
                collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM, adMessage));
            }
        }
    }

    private double calculateMatchScore(Map<String, Object> user, Map<String, Object> store) {
        // Implement the logic to calculate the match score based on the provided criteria
        double score = (Integer) store.get("review_count") * (Double) store.get("rating");
        // Add additional logic for interest, device, and distance adjustments
        return score;
    }
}
