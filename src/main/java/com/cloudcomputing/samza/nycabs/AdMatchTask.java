package com.cloudcomputing.samza.nycabs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.Entry;
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
                // determine the user tag
                Set<String> userTags = determineUserTags(
                        (Integer) mapResult.getOrDefault("blood_sugar", 0),
                        (Integer) mapResult.getOrDefault("mood", 0),
                        (Integer) mapResult.getOrDefault("stress", 0),
                        (Integer) mapResult.getOrDefault("active", 0)
                );
                mapResult.put("tags", userTags);
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
        Object message = envelope.getMessage();
        System.out.println("Processing message: " + message + " of type: " + message.getClass().getName());
        if (message instanceof Map) {
            Map<String, Object> event = (Map<String, Object>) message;
            String type = (String) event.get("type");
            System.out.println("Processing event of type: " + type);
            switch (type) {
                case "RIDER_STATUS":
                    handleRiderStatus(event);
                    break;
                case "RIDER_INTEREST":
                    handleRiderInterest(event);
                    break;
                case "RIDE_REQUEST":
                    handleRideRequest(event, collector);
                    break;
                default:
                    // No action needed for other event types
                    break;
            }
        }

    }

    /**
     * Handles the RIDER_STATUS event to update userInfo and assign tags.
     */
    private void handleRiderStatus(Map<String, Object> event) {
        try {
            int userId = (Integer) event.get("userId");
            int mood = (Integer) event.get("mood");
            int bloodSugar = (Integer) event.get("blood_sugar");
            int stress = (Integer) event.get("stress");
            int active = (Integer) event.get("active");

            // Retrieve existing user profile
            Map<String, Object> userProfile = userInfo.get(userId);

            // Update user profile with RIDER_STATUS data
            userProfile.put("mood", mood);
            userProfile.put("blood_sugar", bloodSugar);
            userProfile.put("stress", stress);
            userProfile.put("active", active);

            // Assign tags based on updated profile
            Set<String> userTags = determineUserTags(bloodSugar, mood, stress, active);
            userProfile.put("tags", userTags);

            // Update the userInfo KV store
            userInfo.put(userId, userProfile);
        } catch (Exception e) {
            System.err.println("Error processing RIDER_STATUS event: " + e.getMessage());
        }
    }

    /**
     * Handles the RIDER_INTEREST event to update userInfo.
     */
    private void handleRiderInterest(Map<String, Object> event) {
        try {
            int userId = (Integer) event.get("userId");
            String interest = (String) event.get("interest");
            int duration = (Integer) event.get("duration");

            // Only update interest if duration > 5 minutes
            if (duration > 300000) {
                // Retrieve existing user profile
                Map<String, Object> userProfile = userInfo.get(userId);

                // Update user interest
                userProfile.put("interest", interest);

                // Update the userInfo KV store
                userInfo.put(userId, userProfile);
            }
        } catch (Exception e) {
            System.err.println("Error processing RIDER_INTEREST event: " + e.getMessage());
        }
    }

    /**
     * Handles the RIDE_REQUEST event to perform ad-matching and send to
     * ad-stream.
     */
    private void handleRideRequest(Map<String, Object> event, MessageCollector collector) {
        try {
            int userId = (Integer) event.get("userId");

            // Retrieve user profile
            Map<String, Object> userProfile = userInfo.get(userId);

            @SuppressWarnings("unchecked")
            Set<String> userTags = (Set<String>) userProfile.get("tags");
            String userInterest = (String) userProfile.getOrDefault("interest", "");
            String device = (String) userProfile.getOrDefault("device", "");
            int travelCount = (Integer) userProfile.getOrDefault("travel_count", 0);
            int age = (Integer) userProfile.getOrDefault("age", 0);
            double userLat = Double.parseDouble(userProfile.getOrDefault("latitude", "0").toString());
            double userLon = Double.parseDouble(userProfile.getOrDefault("longitude", "0").toString());

            // Collect all possible stores matching user's tags
            List<Map<String, Object>> candidateStores = getCandidateStores(userTags);

            if (candidateStores.isEmpty()) {
                System.out.println("No candidate stores found for userId: " + userId);
                return;
            }

            // Calculate match scores for candidate stores
            Map<Map<String, Object>, Double> storeScores = new HashMap<>();
            for (Map<String, Object> store : candidateStores) {
                double score = calculateMatchScore(store, userInterest, device, travelCount, age, userLat, userLon);
                storeScores.put(store, score);
            }

            // Find the store with the highest score
            Map<String, Object> bestStore = null;
            double maxScore = -1;
            for (Map.Entry<Map<String, Object>, Double> entry : storeScores.entrySet()) {
                if (entry.getValue() > maxScore) {
                    maxScore = entry.getValue();
                    bestStore = entry.getKey();
                }
            }

            if (bestStore != null) {
                // Prepare the advertisement message
                Map<String, Object> adMessage = new HashMap<>();
                adMessage.put("userId", userId);
                adMessage.put("storeId", bestStore.get("storeId"));
                adMessage.put("name", bestStore.get("name"));
                // Send to ad-stream
                collector.send(new OutgoingMessageEnvelope(AdMatchConfig.AD_STREAM, adMessage));
                System.out.println("Sent ad for userId " + userId + " to ad-stream: " + adMessage);
            }

        } catch (Exception e) {
            System.err.println("Error processing RIDE_REQUEST event: " + e.getMessage());
        }
    }

    /**
     * Retrieves candidate stores that match any of the user's tags.
     */
    private List<Map<String, Object>> getCandidateStores(Set<String> userTags) {
        List<Map<String, Object>> candidateStores = new ArrayList<>();

        KeyValueIterator<String, Map<String, Object>> iterator = yelpInfo.all();
        try {
            while (iterator.hasNext()) {
                Entry<String, Map<String, Object>> entry = iterator.next();
                Map<String, Object> store = entry.getValue();
                String storeTag = (String) store.get("tag");

                // Check if store tag matches any of user's tags
                if (userTags.contains(storeTag)) {
                    candidateStores.add(store);
                }
            }
        } finally {
            iterator.close(); // Ensure the iterator is closed to free resources
        }

        return candidateStores;
    }

    /**
     * Calculates the match score for a store based on various criteria.
     */
    private double calculateMatchScore(Map<String, Object> store,
            String userInterest, String device, int travelCount, int age,
            double userLat, double userLon) {
        // Initial match score: review_count * rating
        int reviewCount = (Integer) store.getOrDefault("review_count", 0);
        double rating = Double.parseDouble(store.getOrDefault("rating", "0").toString());
        double score = reviewCount * rating;

        // Add 10 if store category matches user interest
        String storeCategory = (String) store.getOrDefault("categories", "");
        if (storeCategory.equalsIgnoreCase(userInterest)) {
            score += 10;
        }

        // Device and price adjustment
        int deviceValue = getDeviceValue(device);
        String price = (String) store.getOrDefault("price", "$");
        int priceValue = getPriceValue(price);
        double priceAdjustment = 1 - (Math.abs(priceValue - deviceValue) * 0.1);
        score *= priceAdjustment;

        // Distance adjustment
        double storeLat = Double.parseDouble(store.getOrDefault("latitude", "0").toString());
        double storeLon = Double.parseDouble(store.getOrDefault("longitude", "0").toString());
        double distance = calculateDistance(userLat, userLon, storeLat, storeLon, "M");

        boolean isYoungOrFrequentTraveler = (age == 20) || (travelCount > 50);
        double distanceThreshold = isYoungOrFrequentTraveler ? 10.0 : 5.0;

        if (distance > distanceThreshold) {
            score *= 0.1;
        }

        return score;
    }

    /**
     * Determines user tags based on metrics.
     */
    private Set<String> determineUserTags(int bloodSugar, int mood, int stress, int active) {
        Set<String> tags = new HashSet<>();

        // lowCalories
        if (bloodSugar > 4 && mood > 6 && active == 3) {
            tags.add("lowCalories");
        }

        // energyProviders
        if (bloodSugar < 2 || mood < 4) {
            tags.add("energyProviders");
        }

        // willingTour
        if (active == 3) {
            tags.add("willingTour");
        }

        // stressRelease
        if (stress > 5 || active == 1 || mood < 4) {
            tags.add("stressRelease");
        }

        // happyChoice
        if (mood > 6) {
            tags.add("happyChoice");
        }

        // others
        if (tags.isEmpty()) {
            tags.add("others");
        }

        return tags;
    }

    /**
     * Maps device types to their corresponding values.
     */
    private int getDeviceValue(String device) {
        switch (device) {
            case "iPhone XS":
                return 3;
            case "iPhone 7":
                return 2;
            case "iPhone 5":
                return 1;
            default:
                return 0;
        }
    }

    /**
     * Maps price categories to their corresponding values.
     */
    private int getPriceValue(String price) {
        switch (price) {
            case "$$$":
            case "$$$$":
                return 3;
            case "$$":
                return 2;
            case "$":
                return 1;
            default:
                return 0;
        }
    }

    private static double calculateDistance(double lat1, double lon1, double lat2, double lon2, String unit) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            double theta = lon1 - lon2;
            double dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
            dist = Math.acos(dist);
            dist = Math.toDegrees(dist);
            dist = dist * 60 * 1.1515;
            if (unit.equals("K")) {
                dist = dist * 1.609344;
            } else if (unit.equals("N")) {
                dist = dist * 0.8684;
            }
            return (dist);
        }
    }

}
